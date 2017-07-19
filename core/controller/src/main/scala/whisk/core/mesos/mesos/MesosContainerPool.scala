package whisk.core.mesos.mesos

import java.time.Instant

import spray.json.DefaultJsonProtocol._
import spray.json.{JsObject, _}
import whisk.common.{Logging, TransactionId}
import whisk.core.container.Interval
import whisk.core.containerpool.ActivationUnsuccessfulError
import whisk.core.entity.EntityName
import whisk.core.mesos.MesosTask

import scala.collection.concurrent.TrieMap
//import whisk.core.containerpool.Container

import scala.concurrent.ExecutionContext
//import whisk.core.containerpool.Container
import whisk.core.containerpool.{ContainerData, ContainerPool, ContainerProxy, InitializationError, NoData, PreWarmedData, PrewarmingConfig, Run, WarmedData}
import whisk.core.entity.ExecManifest.ImageName
import whisk.core.entity.{ActivationResponse, ByteSize, CodeExec, ExecutableWhiskAction, WhiskActivation}
import whisk.core.entity.size._
import whisk.http.Messages

import scala.collection.mutable
import scala.concurrent.Future
import scala.util.Success

/**
  * Created by tnorris on 6/28/17.
  */
class MesosContainerPool(childFactory: (TransactionId, String, ImageName, Boolean, ByteSize) => Future[MesosTask],
                         storeActivation: (TransactionId, WhiskActivation) => Future[Any],
                         maxActiveContainers: Int,
                         maxPoolSize: Int,
                         prewarmConfig: Option[PrewarmingConfig] = None,
                         maxConcurrency:Int = 200
                        )(implicit val logging:Logging, val ec:ExecutionContext) {

  val warmPool = mutable.Map[MesosContainerProxy, ContainerData]()
  val initPool = new TrieMap[Any, LazyComp]()
//  val initProxy = mutable.Map[String, MesosContainerProxy]()
  val prewarmedPool = mutable.Map[MesosContainerProxy, ContainerData]()

  prewarmConfig.foreach { config =>
    logging.info(this, s"pre-warming ${config.count} ${config.exec.kind} containers")
    (1 to config.count).foreach { _ =>
      prewarmContainer(config.exec, config.memoryLimit)
    }
  }

  def run(job:Run)(implicit transid: TransactionId):Future[WhiskActivation] = {
    //TODO: if no offers are available for certain time period, we can assume no containers will be allowed
    //val container = if (warmPool.size < maxActiveContainers) {
    val container = {
      logging.info(this, "room in pool, will schedule")
      // Schedule a job to a warm container
      schedule(job, job.action, job.msg.user.namespace, warmPool.toMap).orElse {
        //if (warmPool.size < maxActiveContainers) {
          logging.info(this, "will try to use a prewarm...")

          initAction(job)

//          takePrewarmContainer(job.action).orElse {
//            logging.info(this, "will create new container...")
//            Some(createContainer(job))
//          }
//        } else {
//
//
//          logging.warn(this,s"no room in pool with ${warmPool.size} warm containers and all existing containers at max capacity")
//          warmPool.keys.foreach(proxy => {
//            logging.info(this, s"    proxy ${proxy} has data ${warmPool.get(proxy)}")
//          })
//          None
//        }
      }.orElse {
        // Remove a container and create a new one for the given job
        ContainerPool.remove(job.action, job.msg.user.namespace, warmPool.toMap).map { toDelete =>
          removeContainer(toDelete)
//          takePrewarmContainer(job.action).getOrElse {
//            createContainer(job)
//          }
          initAction(job).getOrElse {
            createContainer(job)
          }
        }

      }
    }
//    } else {
//      logging.info(this, s"no room in pool with ${warmPool.size} warm containers")
//      None
//    }

    container match {
      case Some((actor, data)) =>
          warmPool.update(actor, data)
//        warmPool(actor) match {
//          case data: WarmedData => logging.info(this, "data is already warm")
//          case _ => logging.info(this, s"updating warm data for ${actor}")
//            warmPool.update(actor, data)
//        }

        logging.info(this, s"updating container ${actor.taskId} with data ${data}")
        initializeAndRun(actor, data, job)(job.msg.transid)
      case None =>
        //TODO: when this fails (e.g. when max active reached), coudb read loop repeats, but should fail immediately
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


        warmPool(containerProxy) match {
          case data: WarmedData => logging.info(this, "data is already warm")
          case _ => logging.info(this, s"updating warm data for ${containerProxy}")
            warmPool.update(containerProxy,WarmedData(container, job.msg.user.namespace, job.action, Instant.now) )
        }

        val parameters = job.msg.content getOrElse JsObject()

        val environment = JsObject(
          "api_key" -> job.msg.user.authkey.compact.toJson,
          "namespace" -> job.msg.user.namespace.toJson,
          "action_name" -> job.msg.action.qualifiedNameWithLeadingSlash.toJson,
          "activation_id" -> job.msg.activationId.toString.toJson,
          // compute deadline on invoker side avoids discrepancies inside container
          // but potentially under-estimates actual deadline
          "deadline" -> (Instant.now.toEpochMilli + actionTimeout.toMillis).toString.toJson)

        val currentActivations = containerProxy.currentActivations.getAndIncrement()
        logging.info(this, s"starting run on task ${container.taskId} with ${currentActivations} existing in-flight activations")

        container.run(parameters, environment, actionTimeout)(job.msg.transid).map {
          case (runInterval, response) =>
            completeConcurrentRun(containerProxy, container)
            val initRunInterval = Interval(runInterval.start.minusMillis(initInterval.duration.toMillis), runInterval.end)
            ContainerProxy.constructWhiskActivation(job, initRunInterval, response)
        }
      }.recover {
        case InitializationError(interval, response) =>
          completeConcurrentRun(containerProxy, container)
          ContainerProxy.constructWhiskActivation(job, interval, response)
        case t =>
          // Actually, this should never happen - but we want t o make sure to not miss a problem
          logging.error(this, s"caught unexpected error while running activation: ${t}")
          completeConcurrentRun(containerProxy, container)
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

  private def completeConcurrentRun(containerProxy:MesosContainerProxy, container:MesosTask)(implicit transId:TransactionId, ec:ExecutionContext): Unit ={
    val currentActivations = containerProxy.currentActivations.decrementAndGet()
    logging.info(this, s"completed run on task ${container.taskId} with ${currentActivations} remaining in-flight activations")

  }

  def schedule(job:Run, action: ExecutableWhiskAction, invocationNamespace: EntityName, idles: Map[MesosContainerProxy, ContainerData])(implicit transid:TransactionId): Option[(MesosContainerProxy, ContainerData)] = {
    //TODO: more intel: find the container with least amount of concurrently active activations
    //TODO: track currentActivations in a TrieMap locally here instead of in the proxy
//    idles.foreach {
//      case (p, c) => logging.info(this, s"existing container ${p.container.isCompleted}  has  ${p.currentActivations} concurrent activations")
//    }

    var nextBest:Option[(MesosContainerProxy,ContainerData)] = None
    var worst:Option[MesosContainerProxy] = None
    val available = idles.find {
      case proxyVal@(proxy, WarmedData(_, _, `action`, _)) => {
        val currentActivations = proxy.currentActivations.get()
        if (currentActivations < maxConcurrency){
          true
        } else {
          if (nextBest.isEmpty || nextBest.get._1.currentActivations.get() > currentActivations){
            //logging.info(this, s"setting next best to ${currentActivations}")
            nextBest = Some(proxyVal)
          }
          if (worst.isEmpty || worst.get.currentActivations.get() < currentActivations){
            //logging.info(this, s"setting worst to ${currentActivations}")
            worst = Some(proxy)
          }
          false
        }
      }
      case _ => {
        false
      }
    }

    logging.info(this, s"any underused available? ${!available.isEmpty} nextBest:${nextBest}   worst:${worst}")

    if (worst.isDefined && worst.get.currentActivations.get() > maxConcurrency) {
      val currentWorst = worst.get.currentActivations.get()
      initAction(job, worst, Some(currentWorst))

    }

    available.orElse(nextBest)

  }

  def key(action:ExecutableWhiskAction):String = action.namespace.toString + action.name.toString

  def initAction(job:Run, worst:Option[MesosContainerProxy] = None, currentWorst:Option[Int] = None)(implicit transid:TransactionId):Option[(MesosContainerProxy, ContainerData)]={
    val action = job.action
    val actionKey:String = key(action)

    //some hack based on https://stackoverflow.com/questions/35484913/how-to-get-truly-atomic-update-for-triemap-getorelseupdate
    //(since TrieMap.getOrElseUpdate doesn't execute atomically)
    val v = new LazyComp {
      lazy val get = {
        //compute something
        if (!worst.isEmpty){
          logging.warn(this, s"${currentWorst} exceeded max concurrency of ${maxConcurrency} for action ${action.name} in container ${worst.get.container.map(c => c.taskId)}, launching 1 more ")
        }
        //TODO: if container creation fails/timeout then return none
        val newContainer = takePrewarmContainer(job.action).getOrElse {
          createContainer(job)
        }
        newContainer._1.container.map(container => {
          logging.info(this, "about to initialize a running container for excessive concurrency...")
          container.initialize(job.action.containerInitializer, job.action.limits.timeout.duration).map { initInterval =>
            logging.info(this, s"updating warm data for ${newContainer._1}  for excessive concurrency")
            warmPool.put(newContainer._1, WarmedData(container, job.msg.user.namespace, job.action, Instant.now))
            initPool.remove(actionKey)
          }
        })
        newContainer
      }
    }
    Some(initPool.putIfAbsent(actionKey, v).getOrElse(v).get)
  }
  trait LazyComp { val get: (MesosContainerProxy, ContainerData) }

}
