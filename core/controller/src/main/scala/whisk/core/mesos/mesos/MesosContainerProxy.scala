package whisk.core.mesos.mesos

import scala.concurrent.{ExecutionContext, Future}
import java.util.concurrent.atomic.AtomicInteger

import whisk.core.mesos.MesosTask
/**
  * Created by tnorris on 6/28/17.
  */
class MesosContainerProxy (val container:Future[MesosTask]) {
    val currentActivations = new AtomicInteger(0)
    def taskId(implicit ec:ExecutionContext) = container.map(task => task.taskId)
}
