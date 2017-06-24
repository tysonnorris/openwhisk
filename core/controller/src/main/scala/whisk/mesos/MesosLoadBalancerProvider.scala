package whisk.mesos

import whisk.common.TransactionId
import whisk.core.WhiskConfig
import whisk.core.connector.ActivationMessage
import whisk.core.entity.InstanceId
import whisk.core.entity.WhiskAction
import whisk.core.entity.WhiskActivation
import whisk.core.entity.types.EntityStore
import whisk.core.loadBalancer.LoadBalancer
import whisk.core.loadBalancer.LoadBalancerProvider

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

/**
  * Created by tnorris on 6/23/17.
  */
class MesosLoadBalancerProvider extends LoadBalancerProvider {
  override def getLoadBalancer(config: WhiskConfig, instance: InstanceId, entityStore: EntityStore): LoadBalancer = ???
}

class MesosLoadBalancer extends LoadBalancer {
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
    null
  }
}