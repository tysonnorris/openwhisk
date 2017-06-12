package whisk.core.containerpool.mesos

import akka.actor.{ActorRefFactory, ActorSystem}
import spray.client.pipelining._
import spray.http.HttpRequest
import whisk.common.{Logging, TransactionId}
import whisk.core.containerpool.docker.{ContainerId, ContainerIp, DockerApiWithFileAccess, DockerContainer, RuncApi}
import whisk.core.containerpool.mesos.MesosTask.invokerFrameworkEndpoint
import whisk.core.entity.ByteSize
import whisk.core.entity.size._
import spray.json._
import DefaultJsonProtocol._

import scala.concurrent.{ExecutionContext, Future, Promise}
import spray.httpx.SprayJsonSupport._
/**
  * Created by tnorris on 5/22/17.
  */
case class Environment()
case class CreateContainer (image: String, memory:String, cpuShare:String)

object MesosTask {

  //TODO: make this configurable (endpoint of framework)
  val  invokerFrameworkEndpoint = "http://10.0.2.2:3000/invoker"

  //implicit var ref:ActorRefFactory
  implicit val system = ActorSystem( "spray-api-service" )
  implicit val ec = system.dispatcher
  val invokerFrameworkPipeline: HttpRequest => Future[JsObject] = (
    sendReceive
      ~> unmarshal[JsObject]
    )

  def create(transid: TransactionId,
             image: String,
             userProvidedImage: Boolean = false,
             memory: ByteSize = 256.MB,
             cpuShares: Int = 0,
             environment: Map[String, String] = Map(),
             network: String = "bridge",
             dnsServers: Seq[String] = Seq(),
             name: Option[String] = None)(
              implicit docker: DockerApiWithFileAccess, runc: RuncApi, ec: ExecutionContext, log: Logging, af:ActorRefFactory): Future[MesosTask] = {
    implicit val tid = transid
    //TODO: freeze payload form in case classes?
    val createPayload = CreateContainer("whisk/nodejs6action:latest", "256m", "256")

    log.info(this, "creating task for image...")

    val payload = containerInitializerPayload
    log.info(this, s"task creation payload is: ${containerInitializerPayload.toString()}")
    val response:Future[JsObject] = invokerFrameworkPipeline(Post(invokerFrameworkEndpoint, payload))

    val promise = Promise[MesosTask]
    //"{\"agentId\":\"c573e0d7-446a-4d06-8fbe-0340808a874a-S0\",\"state\":\"TASK_STAGING\",\"network\":{\"hostname\":\"172.17.0.7\",\"ip\":\"172.17.0.7\",\"ports\":[31000]}}"
    val localLog = log



//    val processResponse = (jsonResponse:JsObject) => {
//      val taskId = jsonResponse.fields.get("taskId").get.toString()
//      val runtimeInfo = jsonResponse.fields.get("runtimeInfo").get.asJsObject
//      val network:JsObject = runtimeInfo.fields.get("network").get.asJsObject
//      val containerIp = new ContainerIp(network.fields.get("ip").get.toString(),network.fields.get("ports").get.convertTo[JsArray].elements.head.toString().toInt)
//
//      new MesosTask(containerId, containerIp, taskId)
//    }


    //promise.completeWith {
      response map (jsonResponse => {
        log.info(this, s"created task with task info ${jsonResponse}")
        val taskId = jsonResponse.fields.get("taskId").get.toString()
        val runtimeInfo = jsonResponse.fields.get("runtimeInfo").get.asJsObject
        val network:JsObject = runtimeInfo.fields.get("network").get.asJsObject
        //val ip = network.fields.get("ip").get.convertTo[String]
        //if env is Docker for Mac, ip is always 10.0.2.2
        val ip = "10.0.2.2"

        val port = network.fields.get("ports").get.convertTo[JsArray].elements.head.convertTo[Int]
        log.info(this, s"ip was ${ip} port was ${port}")
        val containerIp = new ContainerIp(ip,port)
        val containerId = new ContainerId(taskId);
        new MesosTask(containerId, containerIp, taskId)
      })
//    };

//    promise.future

  }
  def containerInitializerPayload: JsObject = {
    val env = JsObject("__OW_API_HOST" -> JsString("my.host.name"))
    val container = JsObject("image" -> JsString("whisk/nodejs6action:latest"), "memory" -> JsString("128m"), "cpuShares" -> JsString("128"), "environment" -> env)
    JsObject("container" -> container)
  }
}

object JsonFormatters extends DefaultJsonProtocol{
  implicit val createContainerJson = jsonFormat3(CreateContainer)
}
class MesosTask(id: ContainerId, ip: ContainerIp, taskId: String)(
    implicit docker: DockerApiWithFileAccess, runc: RuncApi, ec: ExecutionContext, logger: Logging, af:ActorRefFactory) extends DockerContainer(id, ip) {
  /** Stops the container from consuming CPU cycles. */
  override def suspend()(implicit transid: TransactionId): Future[Unit] = {
    //suspend not supported
    Future(Unit)
  }

  /** Dual of halt. */
  override def resume()(implicit transid: TransactionId): Future[Unit] = {
    //resume not supported
    Future(Unit)

  }

  /** Completely destroys this instance of the container. */
  override def destroy()(implicit transid: TransactionId): Future[Unit] = {
//    val invokerFrameworkPipeline: HttpRequest => Future[JsObject] = (
//      sendReceive
//        ~> unmarshal[JsObject]
//      )
    val response:Future[JsObject] = MesosTask.invokerFrameworkPipeline(Delete(s"${invokerFrameworkEndpoint}/${taskId}"))
    val promise = Promise[Unit]
    response.map(jsObject =>{
      promise.success(Unit)
    })
    promise.future
  }

//
//  /** Initializes code in the container. */
//  override def initialize(initializer: JsObject, timeout: FiniteDuration)(implicit transid: TransactionId): Future[container.Interval] = {
//
//  }
//
//  /** Runs code in the container. */
//  override def run(parameters: JsObject, environment: JsObject, timeout: FiniteDuration)(implicit transid: TransactionId): Future[(container.Interval, ActivationResponse)] = {
//
//  }

  /** Obtains logs up to a given threshold from the container. Optionally waits for a sentinel to appear. */
  override def logs(limit: ByteSize, waitForSentinel: Boolean)(implicit transid: TransactionId): Future[Vector[String]] = {
    Future{
      Vector("log retrieval not implemented")
    }
  }
}
