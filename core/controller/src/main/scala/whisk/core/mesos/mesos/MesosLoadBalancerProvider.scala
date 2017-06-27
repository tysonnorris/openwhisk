package whisk.core.mesos.mesos

import java.util.UUID

import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props
import scaldi.Injector
import whisk.common.Logging
import whisk.common.TransactionId
import whisk.core.WhiskConfig
import whisk.core.connector.ActivationMessage
import whisk.core.container.{ContainerPool => OldContainerPool}
import whisk.core.containerpool.PrewarmingConfig
import whisk.core.containerpool.Run
import whisk.core.containerpool.SynchronousContainer
import whisk.core.containerpool.SynchronousContainerPool
import whisk.core.entity.ByteSize
import whisk.core.entity.CodeExecAsString
import whisk.core.entity.ExecManifest
import whisk.core.entity.ExecManifest.ImageName
import whisk.core.entity.InstanceId
import whisk.core.entity.WhiskAction
import whisk.core.entity.WhiskActivation
import whisk.core.entity.size._
import whisk.core.entity.types.ActivationStore
import whisk.core.entity.types.EntityStore
import whisk.core.loadBalancer.LoadBalancer
import whisk.core.loadBalancer.LoadBalancerProvider
import whisk.core.mesos.MesosClientActor
import whisk.core.mesos.MesosTask
import whisk.core.mesos.Subscribe
import whisk.spi.SpiFactoryModule

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.Failure
import scala.util.Success

/**
  * Created by tnorris on 6/23/17.
  */

class MesosLoadBalancerModule extends SpiFactoryModule[LoadBalancerProvider]{
  override def getInstance(implicit injector: Injector): LoadBalancerProvider = {
    new MesosLoadBalancerProvider(inject[WhiskConfig], inject[InstanceId], inject[EntityStore], inject[ActivationStore])( inject[Logging], inject[ActorSystem])
  }
}

class MesosLoadBalancerProvider(config:WhiskConfig, instance: InstanceId, entityStore: EntityStore, activationStore: ActivationStore)(implicit val logging:Logging, val actorSystem:ActorSystem) extends LoadBalancerProvider {
  def getLoadBalancer(config: WhiskConfig, instance: InstanceId, entityStore: EntityStore): LoadBalancer = {
    new MesosLoadBalancer(config, activationStore)(logging, actorSystem)
  }
}

class MesosLoadBalancer(config:WhiskConfig, activationStore:ActivationStore)(implicit val logging:Logging,val actorSystem:ActorSystem) extends LoadBalancer {
  //init mesos framework:
  implicit val mesosClientActor = actorSystem.actorOf(MesosClientActor.props(
    "whisk-loadbalancer-"+UUID.randomUUID(),
    "whisk-loadbalancer-framework",
    "http://192.168.99.100:5050",
    "*",
    taskBuilder = MesosTask.buildTask
  ))

  mesosClientActor ! Subscribe

  implicit val ec:ExecutionContext = actorSystem.dispatcher

  //TODO: verify subscribed status
  /** Factory used by the ContainerProxy to physically create a new container. */
  val containerFactory = (tid: TransactionId, name: String, actionImage: ImageName, userProvidedImage: Boolean, memory: ByteSize) => {
    val image = if (userProvidedImage) {
      actionImage.publicImageName
    } else {
      actionImage.localImageName(config.dockerRegistry, config.dockerImagePrefix, Some(config.dockerImageTag))
    }


    logging.info(this, "using Mesos to create a container...")
    val startingTask = MesosTask.create(
      tid,
      image = image,
      userProvidedImage = userProvidedImage,
      memory = memory,
      cpuShares = OldContainerPool.cpuShare(config),
      environment = Map("__OW_API_HOST" -> config.wskApiHost),
      network = config.invokerContainerNetwork,
      dnsServers = config.invokerContainerDns,
      name = Some(name))
    logging.info(this, s"created task is completed??? ${startingTask.isCompleted}" )
    startingTask.map(runningTask => {
      logging.info(this, "returning running task")
      runningTask
    })

  }
  /** Sends an active-ack. */

  def ack (tid: TransactionId, activation: WhiskActivation, controllerInstance: InstanceId):Future[Unit] = {
//    Future{
//      processCompletion(tid, activation.activationId, activation)
//    }
    Future {
      Unit
    }
  }
  /** Stores an activation in the database. */
  val store = (tid: TransactionId, activation: WhiskActivation) => {
    implicit val transid = tid
    logging.info(this, "recording the activation result to the data store")
    WhiskActivation.put(activationStore, activation).andThen {
      case Success(id) => logging.info(this, s"recorded activation")
      case Failure(t)  => logging.error(this, s"failed to record activation")
    }
  }

  /** Creates a ContainerProxy Actor when being called. */
  val childFactory = () => new SynchronousContainer(containerFactory, store)// f.actorOf(ContainerProxy.props(containerFactory, ack, store))

  val prewarmKind = "nodejs:6"
  val prewarmExec = ExecManifest.runtimesManifest.resolveDefaultRuntime(prewarmKind).map { manifest =>
    new CodeExecAsString(manifest, "", None)
  }.get

//  val pool = actorSystem.actorOf(ContainerPool.props(
//    childFactory,
//    OldContainerPool.getDefaultMaxActive(config),
//    OldContainerPool.getDefaultMaxActive(config),
//    //activationFeed,
//    actorSystem.actorOf(Props(new Actor {
//      override def receive: Receive = Actor.emptyBehavior
//    })),
//    Some(PrewarmingConfig(2, prewarmExec, 256.MB))))

  val pool = new SynchronousContainerPool(childFactory,
    OldContainerPool.getDefaultMaxActive(config),
    OldContainerPool.getDefaultMaxActive(config),
    //activationFeed,
    actorSystem.actorOf(Props(new Actor {
      override def receive: Receive = Actor.emptyBehavior
    })),
    Some(PrewarmingConfig(2, prewarmExec, 256.MB)))

//  val pool = new ContainerPool(childFactory, OldContainerPool.getDefaultMaxActive(config), OldContainerPool.getDefaultMaxActive(config), activationFeed, Some(PrewarmingConfig(2, prewarmExec, 256.MB)))

  /**
    * Retrieves a per subject map of counts representing in-flight activations as seen by the load balancer
    *
    * @return a map where the key is the subject and the long is total issued activations by that user
    */
  override def getActiveUserActivationCounts: Map[String, Int] = {
    Map()
  }

  /**
    * Publishes activation message on internal bus for the invoker to pick up.
    *
    * @param msg     the activation message to publish on an invoker topic
    * @param timeout the desired active ack timeout
    * @param transid the transaction id for the request
    * @return result a nested Future the outer indicating completion of publishing and
    *         the inner the completion of the action (i.e., the result)
    *         if it is ready before timeout otherwise the future fails with ActiveAckTimeout
    */
  override def publish(action: WhiskAction, msg: ActivationMessage, timeout: FiniteDuration)(implicit transid: TransactionId): Future[Future[WhiskActivation]] = {
//    val container = if (pool.busyPool.size < pool.maxActiveContainers) {
//      // Schedule a job to a warm container
//      ContainerPool.schedule(action, msg.user.namespace, freePool.toMap).orElse {
//        if (busyPool.size + freePool.size < maxPoolSize) {
//          takePrewarmContainer(r.action).orElse {
//            Some(createContainer())
//          }
//        } else None
//      }.orElse {
//        // Remove a container and create a new one for the given job
//        ContainerPool.remove(r.action, r.msg.user.namespace, freePool.toMap).map { toDelete =>
//          removeContainer(toDelete)
//          takePrewarmContainer(r.action).getOrElse {
//            createContainer()
//          }
//        }
//      }
//    } else None
//
//    container match {
//      case Some((actor, data)) =>
//        busyPool.update(actor, data)
//        freePool.remove(actor)
//        actor ! r // forwards the run request to the container
//      case None =>
//        self ! r
//    }

    action.toExecutableWhiskAction match {
      case Some(executable) =>
        val subject = msg.user.subject.asString
        //this entry contains the promise that needs to be completed once the run is finished
        //val entry = setupActivation(msg.activationId, subject, "mesos", timeout, transid)
        //val result = sendActivationToInvoker(messageProducer, msg, invokerName)
        //pool ! Run(executable, msg)
        Future {
          pool.runNow(Run(executable, msg))
        }
//        val result1 = result  .map { _ =>
//          entry.promise.future
//        }
//        result1

//        Future {
//          entry.promise.future
//        }

        //Future.successful(())
      case None =>
        logging.error(this, s"non-executable action reached the invoker ${action.fullyQualifiedName(false)}")
        Future.failed(new IllegalStateException())
    }
  }
}