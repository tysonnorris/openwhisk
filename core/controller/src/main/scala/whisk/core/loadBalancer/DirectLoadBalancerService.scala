/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package whisk.core.loadBalancer

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorRefFactory
import akka.actor.ActorSystem
import akka.actor.Props
import akka.pattern.ask
import akka.util.Timeout
import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.Failure
import scala.util.Success
import spray.json._
import whisk.common.Logging
import whisk.common.TransactionId
import whisk.core.WhiskConfig
import whisk.core.connector.ActivationMessage
import whisk.core.connector.MessageFeed
import whisk.core.containerpool.ConcurrentPoolScheduler
import whisk.core.containerpool.ContainerFactoryProvider
import whisk.core.containerpool.ContainerPool
import whisk.core.containerpool.ContainerProxy
import whisk.core.containerpool.PrewarmingConfig
import whisk.core.containerpool.Run
import whisk.core.entity.ActivationId
import whisk.core.entity.CodeExecAsString
import whisk.core.entity.ExecManifest
import whisk.core.entity.ExecutableWhiskAction
import whisk.core.entity.InstanceId
import whisk.core.entity.UUID
import whisk.core.entity.WhiskActivation
import whisk.core.entity.WhiskActivationStore
import whisk.core.entity.size._
import whisk.spi.Dependencies
import whisk.spi.SpiFactory
import whisk.spi.SpiLoader

class DirectLoadBalancerServiceProvider extends LoadBalancerProvider {
    override def getLoadBalancer(config: WhiskConfig, instance: InstanceId)
            (implicit logging: Logging, actorSystem: ActorSystem): LoadBalancer = new DirectLoadBalancerService(config, instance)
}

object DirectLoadBalancerServiceProvider extends SpiFactory[LoadBalancerProvider]{
    override def apply(dependencies: Dependencies): LoadBalancerProvider = new DirectLoadBalancerServiceProvider
}

class DirectLoadBalancerService(
    config: WhiskConfig,
    instance: InstanceId)(
        implicit val actorSystem: ActorSystem,
        logging: Logging)
    extends LoadBalancer {

    /** The execution context for futures */
    implicit val executionContext: ExecutionContext = actorSystem.dispatcher

    implicit val cfg = config

    private val loadBalancerData = new LoadBalancerData()
    val containerFactory = SpiLoader
            .get[ContainerFactoryProvider]()
            .getContainerFactory(actorSystem, logging, config)
            .createContainer _
    val activationStore = WhiskActivationStore.datastore(config)

    /** Stores an activation in the database. */
    val store = (tid: TransactionId, activation: WhiskActivation) => {
        implicit val transid = tid
        logging.info(this, "recording the activation result to the data store")
        WhiskActivation.put(activationStore, activation).andThen {
            case Success(id) => logging.info(this, s"recorded activation")
            case Failure(t)  => logging.error(this, s"failed to record activation")
        }
    }
    val prewarmKind = "nodejs:6"
    val prewarmExec = ExecManifest.runtimesManifest
            .resolveDefaultRuntime(prewarmKind)
            .map { manifest =>
                logging.info(
                    this,
                    s"configuring prewarm container with image ${manifest.image
                            .localImageName(config.dockerRegistry, config.dockerImagePrefix, Some(config.dockerImageTag))}"
                )
                new CodeExecAsString(manifest, "", None)
            }
            .get
    val ack =
        (tid: TransactionId, activationResult: WhiskActivation, controllerInstance: InstanceId) => {
            implicit val transid = tid
            feed ! activationResult
            Future.successful(Unit)
        }

    /** Creates a ContainerProxy Actor when being called. */
    val childFactory = (f: ActorRefFactory) =>
        f.actorOf(ContainerProxy.props(containerFactory, ack, store, instance, 30.seconds))

    //local pool feed
    //TODO: make maxContainers dynamic, based on containerfactory
    val maxContainers = 100//i.e. val maximumContainers = config.invokerNumCore.toInt * config.invokerCoreShare.toInt
    val maxConcurrent = 400//if maxConcurrent > maxContainers, indicates concurrent requests will be sent to pool
    val feed:ActorRef = actorSystem.actorOf(DirectFeed.props(maxConcurrent, (r:Run) => {pool ! r}))


    val pool = actorSystem.actorOf(
        ContainerPool
                .props(childFactory,
                    maxContainers,
                    maxContainers,
                    feed,
                    Some(PrewarmingConfig(2, prewarmExec, 256.MB)),
                    new ConcurrentPoolScheduler(maxConcurrent)), "poolactor")



    override def activeActivationsFor(namespace: UUID) = loadBalancerData.activationCountOn(namespace)

    override def totalActiveActivations = loadBalancerData.totalActivationCount

    override def publish(action: ExecutableWhiskAction, msg: ActivationMessage)(
        implicit transid: TransactionId): Future[Future[Either[ActivationId, WhiskActivation]]] = {

        implicit val timeout = Timeout(30.seconds)
        //val result = pool.ask(Run(action, msg))
        val result = feed.ask(Run(action, msg)).mapTo[WhiskActivation]

        //TODO: implement timeout handling
        Future.successful(result.map(a => {
            logging.info(this, "returning response from balancer")
            Right(a) }))


    }

    /**
     * Return a message indicating the health of the containers and/or container pool in general
     * @return a Future[String] representing the heal response that will be sent to the client
     */
    override def healthStatus = Future.successful(new JsObject(Map()))
}

object DirectLoadBalancerService {
    def requiredProperties = Map(
        WhiskConfig.dockerRegistry -> "",
        WhiskConfig.dockerImagePrefix -> "",
        WhiskConfig.dockerImageTag -> "latest")



}

//TODO: concurrentLimit does not account for multiple containers running the same action
private class DirectFeed(concurrentLimit: Int, submit:Run => Unit)(implicit logging: Logging) extends Actor {
    val activations = mutable.Map[ActivationId,(ActorRef, ExecutableWhiskAction)]()
    val actionCounts = mutable.Map[ExecutableWhiskAction, Int]()
    val queue = mutable.Queue[Run]()
    override def receive: Receive = {
        case r:Run => {
            activations.update(r.msg.activationId, (sender(), r.action))
            val newCount = actionCounts.getOrElse(r.action, 0)+1
            actionCounts.put(r.action, newCount)

            //if pool cannot keep up with concurrentLimit, start queueing requests
            if (newCount < concurrentLimit+1){//triggers a new container when reaching concurrentLimit+1
                submit(r)
            } else {
                queue.enqueue(r)
            }
        }
        case a:WhiskActivation => {
            activations.remove(a.activationId) match {
            case Some(s) =>
                logging.info(this, s"forwarding activation to ${s}")
                val newCount = actionCounts.getOrElse(s._2, 0)-1
                actionCounts.put(s._2, newCount)
                s._1 ! a
            case None =>
                logging.warn(this, s"received completion for unknown activation ${a.activationId}")
            }
        }
        case MessageFeed.Processed =>
            //send the next msg
            if (!queue.isEmpty){
                submit(queue.dequeue())
            }
    }
}
private object DirectFeed {
    def props(concurrentLimit:Int = 1, submit:Run => Unit)(implicit logging:Logging) = Props(new DirectFeed(concurrentLimit, submit))
}
private case class DirectLoadBalancerException(msg: String) extends Throwable(msg)
