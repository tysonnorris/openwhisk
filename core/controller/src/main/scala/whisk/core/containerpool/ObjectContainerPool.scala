package whisk.core.containerpool

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.util.Success
import spray.json.DefaultJsonProtocol._
import spray.json.JsObject
import spray.json._
import whisk.common.Logging
import whisk.common.TransactionId
import whisk.core.entity.ActivationId
import whisk.core.entity.ActivationResponse
import whisk.core.entity.EntityName
import whisk.core.entity.ExecutableWhiskAction
import whisk.core.entity.WhiskActivation
import whisk.http.Messages

/**
  * Created by tnorris on 6/28/17.
  */
class ObjectContainerPool(system:ActorSystem,
                         cManager:ActorRef,
                         storeActivation: (TransactionId, WhiskActivation) => Future[Any],
                         maxConcurrency:Int = 200
                        )(implicit val logging:Logging, val ec:ExecutionContext) {

  val containerPromises = new TrieMap[String, Promise[Option[WarmContainer]]]()
  val warmPool = mutable.Map[WarmContainer, AtomicInteger]()


  val cMgrClient:ActorRef = system.actorOf(Props(new Actor {
    override def receive: Receive = {
      case ClientNeedWork(warmContainer) => {

        val actionKey:String = warmContainer.data.action.fullyQualifiedName(true).toString
        val updated = warmPool.put(warmContainer, new AtomicInteger(0))
        logging.info(this, s"received a container ${warmContainer.data.container.containerId} updated ${updated}")
        //cleanup the promise map when it completes (completes AFTER the container is added to the warmPool)
        containerPromises.remove(actionKey) match {
          case Some(p) =>
            logging.info(this, s"removed promise for ${actionKey}")
            p.success(Some(warmContainer))
          case None => logging.info(this, "no promise found after container startup")
        }

      }
      case ClientNotifyRemoval(proxy) => {
        logging.info(this, s"notified of a container removal ${proxy.data.container.taskId}")
        warmPool.get(proxy) match {
          case Some(p) => if (p.get() == 0){
            warmPool.remove(proxy)
            } else {
              //preemptively report usage to unmark this from removal
              cManager ! ReportUsage(cMgrClient, proxy, p.get())
            }
          case None =>
            logging.info(this, s"removal of unknown container ${proxy.data.container.taskId}")
        }
      }

    }
  }))
  system.scheduler.schedule(30.seconds, 30.seconds) {
    warmPool.foreach(p => {
      cManager ! ReportUsage(cMgrClient, p._1, p._2.get())

    })
  }

  def run(job:Run)(implicit transid: TransactionId):Future[Future[Either[ActivationId, WhiskActivation]]] = {
    //TODO: if no offers are available for certain time period, we can assume no containers will be allowed
    val containerOrNextBest = {
      // Schedule a job to a warm container
      schedule(job.action, maxConcurrency, Some(job.msg.user.namespace), warmPool.toMap)
    }

    val container = containerOrNextBest._1
    if (containerOrNextBest._2>0){
      //trigger a launch of new prewarm if we received a nextbest container
      //(0 to containerOrNextBest._2).foreach(_ => {
      //TODO: support launching multiple containers (for a thundering heard that exceeds capacity of a single additional container)
      initActionIdempotent(job)
      //})
    }
    container match {
      case Some((actor, data)) =>
        logging.info(this, s"running from existing warmpool container ${actor.data.container.containerId}")
        Future.successful(run(actor, job)(job.msg.transid))
      case None =>
        val newContainer = initActionIdempotent(job)
        newContainer.flatMap(c => {
          c match {
            case Some(warmContainer) =>
              logging.info(this, s"running from new warmpool container ${warmContainer.data.container.taskId}")
              Future.successful(run(warmContainer, job)(job.msg.transid))
            case None =>
              //TODO: some backoff retry?
              //outer future (posting) is successful, but inner (processing) is failed)
              Future.successful(Future.failed(new Exception("could not start container in the mesos cluster...")))
          }
        })
    }
  }



  /**
    * Runs the job, DOES NOT initialize first if necessary.
    * Completes the job by:
    * 1. sending an activate ack,
    * 2. fetching the logs for the run,
    * 3. indicating the resource is free to the parent pool,
    * 4. recording the result to the data store
    *
    * @param warmContainer the container to run the job on
    * @param job the job to run
    * @return a future completing after logs have been collected and
    *         added to the WhiskActivation
    */
  def run(warmContainer: WarmContainer, job: Run)(implicit tid: TransactionId): Future[Either[ActivationId, WhiskActivation]] = {
    val actionTimeout = job.action.limits.timeout.duration
    //val container = containerProxy.data.container
    val initInterval = Interval.zero
    val activation:Future[Either[ActivationId, WhiskActivation]]  = {
        val parameters = job.msg.content getOrElse JsObject()

        val environment = JsObject(
          "api_key" -> job.msg.user.authkey.compact.toJson,
          "namespace" -> job.msg.user.namespace.toJson,
          "action_name" -> job.msg.action.qualifiedNameWithLeadingSlash.toJson,
          "activation_id" -> job.msg.activationId.toString.toJson,
          // compute deadline on invoker side avoids discrepancies inside container
          // but potentially under-estimates actual deadline
          "deadline" -> (Instant.now.toEpochMilli + actionTimeout.toMillis).toString.toJson)

        val currentActivations = warmPool.get(warmContainer).get.getAndIncrement()

        logging.info(this, s"starting run on task ${warmContainer.data.container.containerId} with ${currentActivations} existing in-flight activations")

      warmContainer.data.container.run(parameters, environment, actionTimeout)(job.msg.transid).map {
          case (runInterval, response) =>
            completeConcurrentRun(warmContainer)
            val initRunInterval = Interval(runInterval.start.minusMillis(initInterval.duration.toMillis), runInterval.end)
            Right(ContainerProxy.constructWhiskActivation(job, initRunInterval, response))
        }
      }.recover {
        case InitializationError(interval, response) =>
          completeConcurrentRun(warmContainer)
          Right(ContainerProxy.constructWhiskActivation(job, interval, response))
        case t =>
          // Actually, this should never happen - but we want t o make sure to not miss a problem
          logging.error(this, s"caught unexpected error while running activation: ${t}")
          completeConcurrentRun(warmContainer)
          Right(ContainerProxy.constructWhiskActivation(job, Interval.zero, ActivationResponse.whiskError(Messages.abnormalRun)))
      } //TODO: activeAck from ObjectContainerPool?
      //    activation.andThen {
      //      // the activation future will always complete with Success
      //      case Success(ack) => sendActiveAck(tid, ack, job.msg.rootControllerIndex)
      activation.andThen {
            //TODO: logs
  //          container.logs(job.action.limits.logs.asMegaBytes, job.action.exec.sentinelledLogs).map { logs =>
  //            activation.withLogs(ActivationLogs(logs.toVector))
  //          }
  //        }.andThen {
            case Success(activation) => storeActivation(tid, activation.right.get)
          }.flatMap { activation =>
            //self ! ActivationCompleted

            // Fail the future iff the activation was unsuccessful to facilitate
            // better cleanup logic.
            if (activation.right.get.response.isSuccess) Future.successful(activation)
            else Future.failed(ActivationUnsuccessfulError(activation.right.get))
          }
    }


  private def completeConcurrentRun(warmContainer:WarmContainer)(implicit transId:TransactionId, ec:ExecutionContext): Unit ={
    val currentActivations = warmPool.get(warmContainer).get.decrementAndGet() //containerProxy.data.container.currentActivations.decrementAndGet()
    logging.info(this, s"completed run on task ${warmContainer.data.container.containerId} with ${currentActivations} remaining in-flight activations")

  }

  def initActionIdempotent(job:Run, worst:Option[WarmContainer] = None, currentWorst:Option[Int] = None)(implicit transid:TransactionId):Future[Option[WarmContainer]]={
    val action = job.action
    val actionKey:String = action.fullyQualifiedName(true).toString

    val newPromise = Promise[Option[WarmContainer]]()
    containerPromises.putIfAbsent(actionKey, newPromise) match {
      case Some(oldPromise) =>
        logging.info(this, s"existing promise, waiting on new container for ${actionKey}")
        oldPromise.future
      case None => {
        logging.info(this, s"requesting a new container for ${actionKey}")
        //collect knownTaskIds, so that we are not receiving a container we already know about
        val knownTaskIds = warmPool.map( p => p._1.data.container.taskId).toSeq
        //send a message to init this promise
        cManager ! GetContainer(cMgrClient, job.action, maxConcurrency, knownTaskIds)
        newPromise.future.map(p => {
          //to be safe, guarantee that the promise is cleaned up (otherwise container launching will be stalled)
          if (containerPromises.contains(actionKey)){
            logging.info(this, s"promise was not removed already??? for ${actionKey}")
            containerPromises.remove(actionKey)
          }
        })
        newPromise.future
      }
    }
  }
  def schedule(action: ExecutableWhiskAction, maxConcurrency:Int, invocationNamespace: Option[EntityName], idles: Map[WarmContainer, AtomicInteger])(implicit logging:Logging): (Option[(WarmContainer, AtomicInteger)], Int) = {
    val available = idles.filter {
      case (WarmContainer(WarmedData(_, `action`, _),_),_) => {
        true
      }
      case _ => {
        false
      }
    }

    //return the container with minimum usage
    val minUsage = if (available.size > 0) {
      available.minBy(p => p._2.get())
    } else {
      null
    }

    //optimization: don't return overUsage IFF we are already waiting on a container
    val overUsage =
      if (!containerPromises.contains(action.fullyQualifiedName(true).toString)) {
        available.filter(p => p._2.get() > maxConcurrency).size
      } else {
        0
      }



    (Option(minUsage), overUsage)

  }
}
