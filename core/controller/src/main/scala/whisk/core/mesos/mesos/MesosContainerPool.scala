package whisk.core.mesos.mesos

import java.time.Instant

import spray.json.DefaultJsonProtocol._
import spray.json.JsObject
import spray.json._
import whisk.common.Logging
import whisk.common.TransactionId
import whisk.core.container.Interval
import whisk.core.containerpool.ActivationUnsuccessfulError
import whisk.core.containerpool.Container

import scala.concurrent.ExecutionContext
//import whisk.core.containerpool.Container
import whisk.core.containerpool.ContainerData
import whisk.core.containerpool.ContainerPool
import whisk.core.containerpool.ContainerProxy
import whisk.core.containerpool.InitializationError
import whisk.core.containerpool.NoData
import whisk.core.containerpool.PreWarmedData
import whisk.core.containerpool.PrewarmingConfig
import whisk.core.containerpool.Run
import whisk.core.containerpool.WarmedData
import whisk.core.entity.ActivationResponse
import whisk.core.entity.ByteSize
import whisk.core.entity.CodeExec
import whisk.core.entity.ExecManifest.ImageName
import whisk.core.entity.ExecutableWhiskAction
import whisk.core.entity.WhiskActivation
import whisk.core.entity.size._
import whisk.http.Messages

import scala.collection.mutable
import scala.concurrent.Future
import scala.util.Success

/**
  * Created by tnorris on 6/28/17.
  */
class MesosContainerPool(childFactory: (TransactionId, String, ImageName, Boolean, ByteSize) => Future[Container],
                         storeActivation: (TransactionId, WhiskActivation) => Future[Any],
                         maxActiveContainers: Int,
                         maxPoolSize: Int,
                         prewarmConfig: Option[PrewarmingConfig] = None)(implicit val logging:Logging, val ec:ExecutionContext) {

  val warmPool = mutable.Map[MesosContainerProxy, ContainerData]()
  val prewarmedPool = mutable.Map[MesosContainerProxy, ContainerData]()

  prewarmConfig.foreach { config =>
    logging.info(this, s"pre-warming ${config.count} ${config.exec.kind} containers")
    (1 to config.count).foreach { _ =>
      prewarmContainer(config.exec, config.memoryLimit)
    }
  }

  def run(job:Run):Future[WhiskActivation] = {
    val container = if (warmPool.size < maxActiveContainers) {
      logging.info(this, "room in pool, will schedule")
      // Schedule a job to a warm container
      ContainerPool.schedule(job.action, job.msg.user.namespace, warmPool.toMap).orElse {
        if (warmPool.size < maxPoolSize) {
          logging.info(this, "will try to use a prewarm...")
          takePrewarmContainer(job.action).orElse {
            logging.info(this, "will create new container...")
            Some(createContainer(job))
          }
        } else None
      }.orElse {
        // Remove a container and create a new one for the given job
        ContainerPool.remove(job.action, job.msg.user.namespace, warmPool.toMap).map { toDelete =>
          removeContainer(toDelete)
          takePrewarmContainer(job.action).getOrElse {
            createContainer(job)
          }
        }
      }
    } else None

    container match {
      case Some((actor, data)) =>
          warmPool.update(actor, data)
        logging.info(this, s"updating container ${actor} with data ${data}")
        initializeAndRun(actor, data, job)(job.msg.transid)
      case None =>
        Future.failed(new Exception("could not start container in the mesos cluster..."))
    }
  }
  /** Creates a new container and updates state accordingly. */
  def createContainer(job:Run): (MesosContainerProxy, ContainerData) = {

    val ref = childFactory(job.msg.transid,
      ContainerProxy.containerName(job.msg.user.namespace.name, job.action.name.name),
      job.action.exec.image,
      job.action.exec.pull,
      job.action.limits.memory.megabytes.MB)
    val data = NoData()
    (new MesosContainerProxy(ref), data)

  }

  /** Creates a new prewarmed container */
  def prewarmContainer(exec: CodeExec[_], memoryLimit: ByteSize) = {
    val container = childFactory(TransactionId.invokerWarmup,
      ContainerProxy.containerName("prewarm", exec.kind),
      exec.image,
      exec.pull,
      memoryLimit)

    container.map(c => {
      prewarmedPool.update(new MesosContainerProxy(container), PreWarmedData(c, exec.kind, memoryLimit))

    })
  }

  // ! Start(exec, memoryLimit)
  /**
    * Takes a prewarm container out of the prewarmed pool
    * iff a container with a matching kind is found.
    *
    * @param action the kind you want to invoke
    * @return the container iff found
    */
  def takePrewarmContainer(action: ExecutableWhiskAction): Option[(MesosContainerProxy, ContainerData)] =
    prewarmConfig.flatMap { config =>
      val kind = action.exec.kind
      val memory = action.limits.memory.megabytes.MB
      prewarmedPool.find {
        case (_, PreWarmedData(_, `kind`, `memory`)) => true
        case _                                       => false
      }.map {
        case (ref, data) =>
          // Move the container to the usual pool
          //freePool.update(ref, data)
          prewarmedPool.remove(ref)
          // Create a new prewarm container
          prewarmContainer(config.exec, config.memoryLimit)

          (ref, data)
      }
    }

  /** Removes a container and updates state accordingly. */
  def removeContainer(toDelete: MesosContainerProxy) = {
    toDelete.container.map(c => c.destroy()(TransactionId.loadbalancer))// ! Remove
    //freePool.remove(toDelete)
    warmPool.remove(toDelete)
  }

  /**
    * Runs the job, initialize first if necessary.
    * Completes the job by:
    * 1. sending an activate ack,
    * 2. fetching the logs for the run,
    * 3. indicating the resource is free to the parent pool,
    * 4. recording the result to the data store
    *
    * @param containerProxy the container to run the job on
    * @param job the job to run
    * @return a future completing after logs have been collected and
    *         added to the WhiskActivation
    */
  def initializeAndRun(containerProxy: MesosContainerProxy, containerData:ContainerData, job: Run)(implicit tid: TransactionId): Future[WhiskActivation] = {
    val actionTimeout = job.action.limits.timeout.duration

    val result = containerProxy.container.map(container => {


      // Only initialize iff we haven't yet warmed the container
      val initialize = containerData match {
        case data: WarmedData => Future.successful(Interval.zero)
        case _ =>
          logging.info(this, "about to initialize a running container...")
          container.initialize(job.action.containerInitializer, actionTimeout)
      }

      val activation: Future[WhiskActivation] = initialize.flatMap { initInterval =>

        logging.info(this, s"updating warm data for ${containerProxy}")
        warmPool.update(containerProxy,WarmedData(container, job.msg.user.namespace, job.action, Instant.now) )

        val parameters = job.msg.content getOrElse JsObject()

        val environment = JsObject(
          "api_key" -> job.msg.user.authkey.compact.toJson,
          "namespace" -> job.msg.user.namespace.toJson,
          "action_name" -> job.msg.action.qualifiedNameWithLeadingSlash.toJson,
          "activation_id" -> job.msg.activationId.toString.toJson,
          // compute deadline on invoker side avoids discrepancies inside container
          // but potentially under-estimates actual deadline
          "deadline" -> (Instant.now.toEpochMilli + actionTimeout.toMillis).toString.toJson)

        container.run(parameters, environment, actionTimeout)(job.msg.transid).map {
          case (runInterval, response) =>
            val initRunInterval = Interval(runInterval.start.minusMillis(initInterval.duration.toMillis), runInterval.end)
            ContainerProxy.constructWhiskActivation(job, initRunInterval, response)
        }
      }.recover {
        case InitializationError(interval, response) =>
          ContainerProxy.constructWhiskActivation(job, interval, response)
        case t =>
          // Actually, this should never happen - but we want to make sure to not miss a problem
          logging.error(this, s"caught unexpected error while running activation: ${t}")
          ContainerProxy.constructWhiskActivation(job, Interval.zero, ActivationResponse.whiskError(Messages.abnormalRun))
      }

      //    activation.andThen {
      //      // the activation future will always complete with Success
      //      case Success(ack) => sendActiveAck(tid, ack, job.msg.rootControllerIndex)
      activation.andThen {
            //TODO: logs
  //          container.logs(job.action.limits.logs.asMegaBytes, job.action.exec.sentinelledLogs).map { logs =>
  //            activation.withLogs(ActivationLogs(logs.toVector))
  //          }
  //        }.andThen {
            case Success(activation) => storeActivation(tid, activation)
          }.flatMap { activation =>
            //self ! ActivationCompleted

            // Fail the future iff the activation was unsuccessful to facilitate
            // better cleanup logic.
            if (activation.response.isSuccess) Future.successful(activation)
            else Future.failed(ActivationUnsuccessfulError(activation))
          }


      activation


    })
    result.flatMap(a => a)
  }

}
