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

package whisk.core.containerpool

import java.time.Instant

import akka.actor.ActorRef
import spray.json.DefaultJsonProtocol._
import spray.json.JsObject
import spray.json._
import whisk.common.Logging
import whisk.common.TransactionId
import whisk.core.container.Interval
import whisk.core.entity.ActivationResponse
import whisk.core.entity.ByteSize
import whisk.core.entity.CodeExec
import whisk.core.entity.EntityName
import whisk.core.entity.ExecManifest.ImageName
import whisk.core.entity.ExecutableWhiskAction
import whisk.core.entity.WhiskActivation
import whisk.core.entity.size._
import whisk.http.Messages

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

//sealed trait WorkerState
//case object Busy extends WorkerState
//case object Free extends WorkerState

//case class WorkerData(data: ContainerData, state: WorkerState)

/**
 * A pool managing containers to run actions on.
 *
 * This pool fulfills the other half of the ContainerProxy contract. Only
 * one job (either Start or Run) is sent to a child-actor at any given
 * time. The pool then waits for a response of that container, indicating
 * the container is done with the job. Only then will the pool send another
 * request to that container.
 *
 * Upon actor creation, the pool will start to prewarm containers according
 * to the provided prewarmConfig, iff set. Those containers will **not** be
 * part of the poolsize calculation, which is capped by the poolSize parameter.
 * Prewarm containers are only used, if they have matching arguments
 * (kind, memory) and there is space in the pool.
 *
 * @param childFactory method to create new container proxy actor
 * @param maxActiveContainers maximum amount of containers doing work
 * @param maxPoolSize maximum size of containers allowed in the pool
 * @param feed actor to request more work from
 * @param prewarmConfig optional settings for container prewarming
 */
class SynchronousContainerPool(
    childFactory: () => SynchronousContainer,
    maxActiveContainers: Int,
    maxPoolSize: Int,
    feed: ActorRef,
    prewarmConfig: Option[PrewarmingConfig] = None)(implicit val logging:Logging) {

    val freePool = mutable.Map[SynchronousContainer, ContainerData]()
    val busyPool = mutable.Map[SynchronousContainer, ContainerData]()
    val prewarmedPool = mutable.Map[SynchronousContainer, ContainerData]()




    prewarmConfig.foreach { config =>
        logging.info(this, s"pre-warming ${config.count} ${config.exec.kind} containers")
        (1 to config.count).foreach { _ =>
            prewarmContainer(config.exec, config.memoryLimit)
        }
    }


    def runNow(r:Run): Future[WhiskActivation] ={
            val container = if (busyPool.size < maxActiveContainers) {
                logging.info(this, s"room in pool ${freePool.size}, will schedule")
                // Schedule a job to a warm container


                SynchronousContainerPool.schedule(r.action, r.msg.user.namespace, freePool.toMap).orElse {
                    logging.info(this, "did not schedule, will try for prewarm or create...")
                    if (busyPool.size + freePool.size < maxPoolSize) {
                        logging.info(this, "will try to use a prewarm...")
                        takePrewarmContainer(r.action).orElse {
                            logging.info(this, "will create new container...")
                            Some(createContainer())
                        }
                    } else None
                }.orElse {
                    // Remove a container and create a new one for the given job
                    SynchronousContainerPool.remove(r.action, r.msg.user.namespace, freePool.toMap).map { toDelete =>
                        removeContainer(toDelete)
                        takePrewarmContainer(r.action).getOrElse {
                            createContainer()
                        }
                    }
                }
            } else None

            container match {
                case Some((actor, data)) =>
                    busyPool.update(actor, data)
                    //freePool.remove(actor)
                    logging.info(this, s"submitting msg ${r.msg.activationId} to actor ${actor}")
                    actor.run(r)// ! r // forwards the run request to the container
                case None =>
                    logging.error(this, "TODO: handling for cluster resource exhaustion")
                    Future.failed[WhiskActivation](new RuntimeException("no container available in cluster..."))
            }

    }

    /** Creates a new container and updates state accordingly. */
    def createContainer(): (SynchronousContainer, ContainerData) = {
        val ref = childFactory()
        val data = NoData()
        freePool.update(ref, data)

        (ref, data)
    }

    /** Creates a new prewarmed container */
    def prewarmContainer(exec: CodeExec[_], memoryLimit: ByteSize) =
        childFactory().startContainer(exec, memoryLimit)// ! Start(exec, memoryLimit)

    /**
     * Takes a prewarm container out of the prewarmed pool
     * iff a container with a matching kind is found.
     *
     * @param kind the kind you want to invoke
     * @return the container iff found
     */
    def takePrewarmContainer(action: ExecutableWhiskAction): Option[(SynchronousContainer, ContainerData)] =
        prewarmConfig.flatMap { config =>
            val kind = action.exec.kind
            val memory = action.limits.memory.megabytes.MB
            prewarmedPool.find {
                case (_, PreWarmedData(_, `kind`, `memory`)) => true
                case _                                       => false
            }.map {
                case (ref, data) =>
                    // Move the container to the usual pool
                    freePool.update(ref, data)
                    prewarmedPool.remove(ref)
                    // Create a new prewarm container
                    prewarmContainer(config.exec, config.memoryLimit)

                    (ref, data)
            }
        }

    /** Removes a container and updates state accordingly. */
    def removeContainer(toDelete: SynchronousContainer) = {
        toDelete.removeContainer()// ! Remove
        freePool.remove(toDelete)
        busyPool.remove(toDelete)
    }
}
class SynchronousContainer(factory: (TransactionId, String, ImageName, Boolean, ByteSize) => Future[Container],
                           store: (TransactionId, WhiskActivation) => Future[Any])(implicit val logging:Logging, val ec:ExecutionContext) {
    // create a new container
//    val container:Future[Container] = factory(
//        job.msg.transid,
//        ContainerProxy.containerName(job.msg.user.namespace.name, job.action.name.name),
//        job.action.exec.image,
//        job.action.exec.pull,
//        job.action.limits.memory.megabytes.MB)

    var container:Future[Container] = null
    var stateData:ContainerData = NoData()

    def removeContainer():Unit = {

    }
    //for prewarm
    def startContainer(exec: CodeExec[_], memoryLimit: ByteSize):Unit = {
        val cont = factory(
            TransactionId.invokerWarmup,
            ContainerProxy.containerName("prewarm", exec.kind),
            exec.image,
            exec.pull,
            memoryLimit)
        cont.map(container => PreWarmedData(container, exec.kind, memoryLimit))
        container = cont
    }

    def run(job:Run):Future[WhiskActivation] = {

        if (container == null){
            synchronized{
                if (container == null){
                    container = factory(
                        job.msg.transid,
                        ContainerProxy.containerName(job.msg.user.namespace.name, job.action.name.name),
                        job.action.exec.image,
                        job.action.exec.pull,
                        job.action.limits.memory.megabytes.MB)

                }
            }
        }

        val actionTimeout = job.action.limits.timeout.duration
        container.flatMap(startedContainer => {

            // Only initialize iff we haven't yet warmed the container
            val initialize = stateData match {
                case data: WarmedData => Future.successful(Interval.zero)
                case _                =>
                    logging.info(this, "about to initialize a running container...")
                    startedContainer.initialize(job.action.containerInitializer, actionTimeout)(job.msg.transid)
            }

            val res = initialize.map (inter => {

            })

            val activation: Future[WhiskActivation] = initialize.flatMap { initInterval =>
                val parameters = job.msg.content getOrElse JsObject()

                val environment = JsObject(
                    "api_key" -> job.msg.user.authkey.compact.toJson,
                    "namespace" -> job.msg.user.namespace.toJson,
                    "action_name" -> job.msg.action.qualifiedNameWithLeadingSlash.toJson,
                    "activation_id" -> job.msg.activationId.toString.toJson,
                    // compute deadline on invoker side avoids discrepancies inside container
                    // but potentially under-estimates actual deadline
                    "deadline" -> (Instant.now.toEpochMilli + actionTimeout.toMillis).toString.toJson)

                logging.info(this, s"about to run msg ${job.msg.activationId}")




                val result = startedContainer.run(parameters, environment, actionTimeout)(job.msg.transid).map {
                    case (runInterval, response) =>
                        val initRunInterval = Interval(runInterval.start.minusMillis(initInterval.duration.toMillis), runInterval.end)
                        ContainerProxy.constructWhiskActivation(job, initRunInterval, response)
                    case _ => throw new RuntimeException("failed...")

                }.recover {
                    case InitializationError(interval, response) =>
                        ContainerProxy.constructWhiskActivation(job, interval, response)
                    case t =>
                        // Actually, this should never happen - but we want to make sure to not miss a problem
                        logging.error(this, s"caught unexpected error while running activation: ${t}")
                        ContainerProxy.constructWhiskActivation(job, Interval.zero, ActivationResponse.whiskError(Messages.abnormalRun))
                }
              result
            }

            // Sending active ack and storing the activation are concurrent side-effects
            // and do not block further execution of the future. They are completely
            // asynchronous.
            activation



//            activation.andThen {
//                // the activation future will always complete with Success
//                case Success(ack) => sendActiveAck(tid, ack, job.msg.rootControllerIndex)
//            }.flatMap { activation =>
//                startedContainer.logs(job.action.limits.logs.asMegaBytes, job.action.exec.sentinelledLogs).map { logs =>
//                    activation.withLogs(ActivationLogs(logs.toVector))
//                }
//            }.andThen {
//                case Success(activation) => storeActivation(tid, activation)
//            }.flatMap { activation =>
//                self ! ActivationCompleted
//                // Fail the future iff the activation was unsuccessful to facilitate
//                // better cleanup logic.
//                if (activation.response.isSuccess) Future.successful(activation)
//                else Future.failed(ActivationUnsuccessfulError(activation))
//            }
//        }

    })

}}
object SynchronousContainerPool {
    /**
     * Finds the best container for a given job to run on.
     *
     * Selects an arbitrary warm container from the passed pool of idle containers
     * that matches the action and the invocation namespace. The implementation uses
     * matching such that structural equality of action and the invocation namespace
     * is required.
     * Returns None iff no matching container is in the idle pool.
     * Does not consider pre-warmed containers.
     *
     * @param action the action to run
     * @param invocationNamespace the namespace, that wants to run the action
     * @param idles a map of idle containers, awaiting work
     * @return a container if one found
     */
    def schedule[A](action: ExecutableWhiskAction, invocationNamespace: EntityName, idles: Map[A, ContainerData]): Option[(A, ContainerData)] = {
        idles.find {
            case (_, WarmedData(_, `invocationNamespace`, `action`, _)) => true
            case _ => false
        }
    }

    /**
     * Finds the best container to remove to make space for the job passed to run.
     *
     * Determines the least recently used Free container in the pool.
     *
     * @param action the action that wants to get a container
     * @param invocationNamespace the namespace, that wants to run the action
     * @param pool a map of all free containers in the pool
     * @return a container to be removed iff found
     */
    def remove[A](action: ExecutableWhiskAction, invocationNamespace: EntityName, pool: Map[A, ContainerData]): Option[A] = {
        // Try to find a Free container that is initialized with any OTHER action
        val freeContainers = pool.collect {
            case (ref, w: WarmedData) if (w.action != action || w.invocationNamespace != invocationNamespace) => ref -> w
        }

        if (freeContainers.nonEmpty) {
            val (ref, _) = freeContainers.minBy(_._2.lastUsed)
            Some(ref)
        } else None
    }

//    def props(factory: ActorRefFactory => ActorRef,
//              maxActive: Int,
//              size: Int,
//              feed: ActorRef,
//              prewarmConfig: Option[PrewarmingConfig] = None) = Props(new ContainerPool(factory, maxActive, size, feed, prewarmConfig))

}

/** Contains settings needed to perform container prewarming */
//case class PrewarmingConfig(count: Int, exec: CodeExec[_], memoryLimit: ByteSize)
