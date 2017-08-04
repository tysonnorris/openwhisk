package whisk.core.mesos

import akka.actor.ActorRef

/**
  * Created by tnorris on 6/28/17.
  */
class MesosContainerProxy (val data:WarmedData, val actor:ActorRef) {
//    val currentActivations = new AtomicInteger(0)
    def taskId = data.container.taskId
}
