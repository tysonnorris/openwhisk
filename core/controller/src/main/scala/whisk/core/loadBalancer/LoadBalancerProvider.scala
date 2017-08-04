package whisk.core.loadBalancer

import akka.actor.ActorSystem
import whisk.common.Logging
import whisk.core.WhiskConfig
import whisk.core.entity.InstanceId
import whisk.core.entity.types.ActivationStore
import whisk.core.entity.types.EntityStore
import whisk.spi.Spi

/**
  * Created by tnorris on 6/22/17.
  */
trait LoadBalancerProvider extends Spi {
  def getLoadBalancer(config: WhiskConfig, instance: InstanceId, entityStore: EntityStore, activationStore: ActivationStore)(implicit logging:Logging, actorSystem:ActorSystem):LoadBalancer
}
