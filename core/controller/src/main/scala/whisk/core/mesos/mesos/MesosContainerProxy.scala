package whisk.core.mesos.mesos

import whisk.core.containerpool.Container

import scala.concurrent.Future

/**
  * Created by tnorris on 6/28/17.
  */
class MesosContainerProxy (val container:Future[Container]) {

}
