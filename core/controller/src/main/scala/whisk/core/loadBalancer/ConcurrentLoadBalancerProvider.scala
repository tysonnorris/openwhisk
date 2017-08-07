package whisk.core.loadBalancer

import akka.actor.ActorRefFactory
import akka.actor.ActorSystem
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success
import whisk.common.Logging
import whisk.common.TransactionId
import whisk.core.WhiskConfig
import whisk.core.connector.ActivationMessage
import whisk.core.containerpool.ActivationTracker
import whisk.core.containerpool.ContainerFactoryProvider
import whisk.core.containerpool.ContainerLifecycleProxy
import whisk.core.containerpool.ContainerManager
import whisk.core.containerpool.ObjectContainerPool
import whisk.core.containerpool.PrewarmingConfig
import whisk.core.containerpool.Run
import whisk.core.entity.ActivationId
import whisk.core.entity.CodeExecAsString
import whisk.core.entity.ExecManifest
import whisk.core.entity.ExecutableWhiskAction
import whisk.core.entity.InstanceId
import whisk.core.entity.UUID
import whisk.core.entity.WhiskActivation
import whisk.core.entity.size._
import whisk.core.entity.types.ActivationStore
import whisk.core.entity.types.EntityStore
import whisk.spi.Dependencies
import whisk.spi.SpiFactory
import whisk.spi.SpiLoader

/**
 * Created by tnorris on 6/23/17.
 */

object ConcurrentLoadBalancerProvider extends SpiFactory[LoadBalancerProvider] {
    override def apply(dependencies: Dependencies): LoadBalancerProvider = new ConcurrentLoadBalancerProvider
}

class ConcurrentLoadBalancerProvider() extends LoadBalancerProvider {
    def getLoadBalancer(config: WhiskConfig, instance: InstanceId, entityStore: EntityStore, activationStore: ActivationStore)(implicit logging: Logging, actorSystem: ActorSystem): LoadBalancer = {
        new ConcurrentLoadBalancer(config, activationStore)(logging, actorSystem)
    }
}

class ConcurrentLoadBalancer(config: WhiskConfig, activationStore: ActivationStore)(implicit val logging: Logging, val actorSystem: ActorSystem) extends LoadBalancer with ActivationTracker {

    val maxConcurrency = actorSystem.settings.config.getInt("whisk.mesos.max-concurrent")

    implicit val c = config

    val containerFactory = SpiLoader.get[ContainerFactoryProvider]().getContainerFactory(actorSystem, logging, config).createContainer _
    /** Sends an active-ack. */

    def ack(tid: TransactionId, activation: WhiskActivation, controllerInstance: InstanceId): Future[Unit] = {
        Future.successful(processCompletion(tid, activation.activationId, activation))
    }

    /** Stores an activation in the database. */
    val store = (tid: TransactionId, activation: WhiskActivation) => {
        implicit val transid = tid
        //TODO: SPI for store behavior, so that delayed/decoupled storage is an option
//        logging.info(this, "skipping activation storage...")
//        Future.successful(Unit)
            logging.info(this, "recording the activation result to the data store")
            WhiskActivation.put(activationStore, activation).andThen {
              case Success(id) => logging.info(this, s"recorded activation")
              case Failure(t)  => logging.error(this, s"failed to record activation")
            }
    }

    val prewarmKind = "nodejs:6"
    val prewarmExec = ExecManifest.runtimesManifest.resolveDefaultRuntime(prewarmKind).map { manifest =>
            logging.info(this, s"configuring prewarm container with image ${manifest.image.localImageName(config.dockerRegistry, config.dockerImagePrefix, Some(config.dockerImageTag))}")
        new CodeExecAsString(manifest, "", None)
    }.get

    ExecManifest.runtimesManifest.runtimes.foreach(r => {
        r.versions.foreach(rm => {
            logging.info(this, s"enabled runtime:+${rm.image.publicImageName}")
        })
    })

    /** Creates a ContainerProxy Actor when being called. */
    val childFactory = (f: ActorRefFactory) => f.actorOf(ContainerLifecycleProxy.props(containerFactory, ack, store, 30.seconds))

    val cManagerClient = actorSystem.actorOf(ContainerManager.props(
        childFactory,
        Some(PrewarmingConfig(2, prewarmExec, 256.MB))))
    val pool = new ObjectContainerPool(
        actorSystem,
        cManagerClient,
        store,
        maxConcurrency)

    /**
     * Retrieves a per subject map of counts representing in-flight activations as seen by the load balancer
     *
     * @return a map where the key is the subject and the long is total issued activations by that user
     */
    def getActiveNamespaceActivationCounts: Map[UUID, Int] = {
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
    override def publish(action: ExecutableWhiskAction, msg: ActivationMessage)(implicit transid: TransactionId): Future[Future[Either[ActivationId, WhiskActivation]]] = {

        //  override def publish(action: WhiskAction, msg: ActivationMessage, timeout: FiniteDuration)(implicit transid: TransactionId): Future[Future[WhiskActivation]] = {

        val subject = msg.user.subject.asString
        pool.run(Run(action, msg))
    }

}