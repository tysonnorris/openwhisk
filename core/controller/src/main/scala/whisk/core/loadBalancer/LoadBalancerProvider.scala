package whisk.core.loadBalancer

import whisk.core.WhiskConfig
import whisk.core.entity.InstanceId
import whisk.core.entity.types.EntityStore
import whisk.spi.Spi
import whisk.spi.SpiProvider

/**
  * Created by tnorris on 6/22/17.
  */
trait LoadBalancerProvider extends Spi {
  def getLoadBalancer(config: WhiskConfig, instance: InstanceId, entityStore: EntityStore):LoadBalancer
}

object LoadBalancerProvider extends SpiProvider[LoadBalancerProvider](configKey = "whisk.spi.loadbalancer.impl")