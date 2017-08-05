package whisk.core.mesos

import java.time.Instant
import akka.actor.{ActorRef, ActorRefFactory, ActorSystem}
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding.Post
import akka.http.scaladsl.model.{ContentTypes, HttpRequest, HttpResponse}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.pattern.ask
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{ActorMaterializer, OverflowStrategy, QueueOfferResult}
import akka.util.Timeout
import org.apache.mesos.v1.Protos
import org.apache.mesos.v1.Protos.ContainerInfo.DockerInfo
import org.apache.mesos.v1.Protos.ContainerInfo.DockerInfo.PortMapping
import org.apache.mesos.v1.Protos.HealthCheck.TCPCheckInfo
import org.apache.mesos.v1.Protos.Value.Ranges
import org.apache.mesos.v1.Protos.{CommandInfo, ContainerInfo, HealthCheck, Offer, Resource, TaskID, TaskInfo, TaskStatus, Value}
import spray.json.DefaultJsonProtocol._
import spray.json.{JsObject, _}
import whisk.core.entity.ActivationResponse.ConnectionError
import scala.concurrent.Promise
import whisk.core.containerpool.Container
import whisk.core.containerpool.ContainerId
import whisk.core.containerpool.ContainerIp
import whisk.core.containerpool.InitializationError
import whisk.core.containerpool.Interval
import whisk.core.containerpool.RunResult
//import spray.http.HttpRequest
import whisk.common.{Counter, Logging, LoggingMarkers, TransactionId}
import whisk.core.entity.ActivationResponse.ContainerResponse
import whisk.core.entity.size._
import whisk.core.entity.{ActivationResponse, ByteSize}

import whisk.http.Messages

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps
import scala.util.{Failure, Success}

/**
  * Created by tnorris on 5/22/17.
  */
case class Environment()
case class CreateContainer (image: String, memory:String, cpuShare:String)

object MesosTask {
  val taskLaunchTimeout = Timeout(30 seconds)
  val taskDeleteTimeout = Timeout(10 seconds)
  val counter = new Counter()
  val startTime = Instant.now.getEpochSecond
  //implicit var ref:ActorRefFactory
  implicit val system = ActorSystem( "spray-api-service" )
  implicit val ec = system.dispatcher


  def create(transid: TransactionId,
             image: String,
             userProvidedImage: Boolean = false,
             memory: ByteSize = 256.MB,
             cpuShares: Int = 0,
             environment: Map[String, String] = Map(),
             network: String = "bridge",
             dnsServers: Seq[String] = Seq(),
             name: Option[String] = None)(
              implicit mesosClientActor: ActorRef, ec: ExecutionContext, log: Logging, af:ActorRefFactory): Future[MesosTask] = {
    implicit val tid = transid

    log.info(this, s"creating task for image ${image}...")

    val taskId = s"task-${counter.next()}-${startTime}"

    val mesosCpuShares = cpuShares / 1024.0 //convert openwhisk (docker based) shares to mesos (cpu percentage)
    val mesosRam = memory.toMB.toInt
    val task = TaskReqs(taskId, image, mesosCpuShares, mesosRam, 8080)

    val launched:Future[TaskDetails] = mesosClientActor.ask(SubmitTask(task))(taskLaunchTimeout).mapTo[TaskDetails]


    launched.map (taskDetails => {
      log.info(this, s"launched task with state ${taskDetails.taskStatus.getState}")
      val taskHost = taskDetails.hostname//taskDetails.taskStatus.getContainerStatus.getNetworkInfos(0).getIpAddresses(0).getIpAddress
      val taskPort = taskDetails.taskInfo.getResourcesList.asScala.filter(_.getName == "ports").iterator.next().getRanges.getRange(0).getBegin.toInt
      log.info(this, s"ip was ${taskHost} port was ${taskPort}")
      val containerIp = new ContainerIp(taskHost,taskPort)
      val containerId = new ContainerId(taskId);
      new MesosTask(containerId, containerIp, taskId, mesosClientActor)
    }) recover {
      case t => throw new Exception(t)
    }


  }

  def buildTask(reqs:TaskReqs, offer:Offer, portIndex:Int):TaskInfo = {
    val containerPort = reqs.port
    //getting the port from the ranges is hard...
    var hostPort = 0
    var portSeekIndex = 0
    val ranges = offer.getResourcesList.asScala
      .filter(res => res.getName == "ports").iterator.next().getRanges.getRangeList.asScala
    require(ranges.size > 0, s"no available ports in resources for offer ${offer}")
    val rangesIt = ranges.iterator
    var rangeSeek = rangesIt.next()
    var nextPort = rangeSeek.getBegin
    while (portSeekIndex < portIndex ){
      while (portSeekIndex < portIndex && nextPort < rangeSeek.getEnd){
        portSeekIndex += 1
        nextPort +=1
      }
      if (portSeekIndex != portIndex) {
        rangeSeek = rangesIt.next()
        nextPort = rangeSeek.getBegin
      }
    }
    if (portSeekIndex != portIndex) {
      throw new RuntimeException("not enough ports matched in offer")
    } else {
      hostPort = nextPort.toInt
    }

    val agentHost = offer.getHostname
    val dockerImage = reqs.dockerImage

    val healthCheck = HealthCheck.newBuilder()
        .setType(HealthCheck.Type.TCP)
      .setTcp(TCPCheckInfo.newBuilder()
        .setPort(containerPort))
      .setDelaySeconds(0)
      .setIntervalSeconds(1)
      .setTimeoutSeconds(1)
      .setGracePeriodSeconds(25)

    val task = TaskInfo.newBuilder
      .setName(reqs.taskId)
      .setTaskId(TaskID.newBuilder
        .setValue(reqs.taskId))
      .setAgentId(offer.getAgentId)
      .setCommand(CommandInfo.newBuilder
        .setEnvironment(Protos.Environment.newBuilder
          .addVariables(Protos.Environment.Variable.newBuilder
            .setName("__OW_API_HOST")
            .setValue(agentHost)))
        .setShell(false)
        .build())
      .setContainer(ContainerInfo.newBuilder
        .setType(ContainerInfo.Type.DOCKER)
        .setDocker(DockerInfo.newBuilder
          .setImage(dockerImage)
          .setNetwork(DockerInfo.Network.BRIDGE)
          .addPortMappings(PortMapping.newBuilder
            .setContainerPort(containerPort)
            .setHostPort(hostPort)
            .build)
        ).build())
      .setHealthCheck(healthCheck)
      .addResources(Resource.newBuilder()
        .setName("ports")
        .setType(Value.Type.RANGES)
        .setRanges(Ranges.newBuilder()
          .addRange(Value.Range.newBuilder()
            .setBegin(hostPort)
            .setEnd(hostPort))))
      .addResources(Resource.newBuilder
        .setName("cpus")
        .setRole("*")
        .setType(Value.Type.SCALAR)
        .setScalar(Value.Scalar.newBuilder
          .setValue(reqs.cpus)))
      .addResources(Resource.newBuilder
        .setName("mem")
        .setRole("*")
        .setType(Value.Type.SCALAR)
        .setScalar(Value.Scalar.newBuilder
          .setValue(reqs.mem)))
      .build
    task
  }
}

object JsonFormatters extends DefaultJsonProtocol{
  implicit val createContainerJson = jsonFormat3(CreateContainer)
}
class MesosTask(val containerId: ContainerId, val containerIp: ContainerIp, val taskId: String, mesosClientActor: ActorRef)(
  implicit ec: ExecutionContext, logger: Logging, as:ActorSystem) extends Container {// with ActionLogDriver {//extends DockerContainer(id, ip) {
  implicit val materializer = ActorMaterializer()

//  /** HTTP connection to the container, will be lazily established by callContainer */
//  private var httpConnection: Option[HttpUtils] = None

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
    mesosClientActor.ask(DeleteTask(taskId))(MesosTask.taskDeleteTimeout).mapTo[TaskStatus].map(taskStatus => {
      logger.info(this, s"task killed ended with state ${taskStatus.getState}")
    }) recover {
      case t => throw new Exception(t)
    }
  }

  def initialize(initializer: JsObject, timeout: FiniteDuration)(implicit transid: TransactionId): Future[Interval] = {
    val start = transid.started(this, LoggingMarkers.INVOKER_ACTIVATION_INIT, s"sending initialization to $containerId $containerIp")

    val body = JsObject("value" -> initializer)
    callContainer("/init", body, timeout, retry = true).andThen { // never fails
      case Success(r: RunResult) =>
        transid.finished(this, start.copy(start = r.interval.start), s"initialization result: ${r.toBriefString}", endTime = r.interval.end)
      case Failure(t) =>
        transid.failed(this, start, s"initializiation failed with $t")
    }.flatMap { result =>
      if (result.ok) {
        Future.successful(result.interval)
      } else if (result.interval.duration >= timeout) {
        Future.failed(InitializationError(result.interval, ActivationResponse.applicationError(Messages.timedoutActivation(timeout, true))))
      } else {
        Future.failed(InitializationError(result.interval, ActivationResponse.processInitResponseContent(result.response, logger)))
      }
    }
  }

  def run(parameters: JsObject, environment: JsObject, timeout: FiniteDuration)(implicit transid: TransactionId): Future[(Interval, ActivationResponse)] = {
    val actionName = environment.fields.get("action_name").map(_.convertTo[String]).getOrElse("")
    val start = transid.started(this, LoggingMarkers.INVOKER_ACTIVATION_RUN, s"sending arguments to $actionName at $containerId $containerIp")

    val parameterWrapper = JsObject("value" -> parameters)
    val body = JsObject(parameterWrapper.fields ++ environment.fields)
    callContainer("/run", body, timeout, retry = false).andThen { // never fails
      case Success(r: RunResult) =>
        transid.finished(this, start.copy(start = r.interval.start), s"running result: ${r.toBriefString}", endTime = r.interval.end)
      case Failure(t) =>
        transid.failed(this, start, s"run failed with $t")
    }.map { result =>
      val response = if (result.interval.duration >= timeout) {
        ActivationResponse.applicationError(Messages.timedoutActivation(timeout, false))
      } else {
        ActivationResponse.processRunResponseContent(result.response, logger)
      }

      (result.interval, response)
    }
  }

  /** Obtains logs up to a given threshold from the container. Optionally waits for a sentinel to appear. */
  override def logs(limit: ByteSize, waitForSentinel: Boolean)(implicit transid: TransactionId): Future[Vector[String]] = {
    Future.successful(Vector("log retrieval not implemented"))
  }

//    val pipeline: HttpRequest => Future[String] = (
//            sendReceive
//                    ~> unmarshal[String]
//            )

  //based on http://doc.akka.io/docs/akka-http/10.0.6/scala/http/client-side/host-level.html
  val maxPendingRequests = 500
  val poolClientFlow = Http().cachedHostConnectionPool[Promise[HttpResponse]](host = containerIp.asString, port = containerIp.port)
  val queue =
    Source.queue[(HttpRequest, Promise[HttpResponse])](maxPendingRequests, OverflowStrategy.backpressure)
      .via(poolClientFlow)
      .toMat(Sink.foreach({
        case ((Success(resp), p)) => p.success(resp)
        case ((Failure(e), p))    => p.failure(e)
      }))(Keep.left)
      .run()

  def queueRequest(request: HttpRequest): Future[HttpResponse] = {
    val responsePromise = Promise[HttpResponse]()
    queue.offer(request -> responsePromise).flatMap {
      case QueueOfferResult.Enqueued    => responsePromise.future
      case QueueOfferResult.Dropped     => Future.failed(new RuntimeException("Queue overflowed. Try again later."))
      case QueueOfferResult.Failure(ex) => Future.failed(ex)
      case QueueOfferResult.QueueClosed => Future.failed(new RuntimeException("Queue was closed (pool shut down) while running the request. Try again later."))
    }
  }

//    val requestCounter = new AtomicInteger(0)
  protected def callContainer(path: String, body: JsObject, timeout: FiniteDuration, retry: Boolean = false)(implicit transid: TransactionId): Future[RunResult] = {
    val started = Instant.now()
//    val http = httpConnection.getOrElse {
//      val conn = new HttpUtils(s"${ip.asString}:${ip.port}", timeout, 1.MB)
//      httpConnection = Some(conn)
//      conn
//    }
//    Future {
//      http.post(path, body, retry)
//    }.map { response =>
//      val finished = Instant.now()
//      RunResult(Interval(started, finished), response)
//    }
//    logger.info(this, s"###### starting request to container ${requestCounter.incrementAndGet()}")
//        val request = Post(s"http://${ip.asString}:${ip.port}${path}", body)
//        pipeline(request).map (responseBody => {
//            val finished = Instant.now()
////            logger.info(this, s"###### ending request to container ${requestCounter.decrementAndGet()}")
//            RunResult(Interval(started, finished), Right(ContainerResponse(true, responseBody)))
//        })

    val req = Post(path).withEntity(ContentTypes.`application/json` ,body.toString())
    queueRequest(req).flatMap( response => {
      val finished = Instant.now()
      if (response.status.isSuccess()){

        Unmarshal(response.entity).to[String].map(respString => {
          RunResult(Interval(started, finished), Right(ContainerResponse(true, respString)))
        })
      } else {
        Unmarshal(response.entity).to[String].map(respString => {
          RunResult(Interval(started, finished), Left(ConnectionError(new Exception(response.toString()))))
        })

      }
    })


  }
}