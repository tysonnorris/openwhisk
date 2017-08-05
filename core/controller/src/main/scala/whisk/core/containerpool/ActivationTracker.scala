package whisk.core.containerpool

import akka.actor.ActorSystem
import java.time.Clock
import java.time.Instant
import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext
import scala.concurrent.Promise
import scala.concurrent.TimeoutException
import scala.concurrent.duration.FiniteDuration
import whisk.common.Logging
import whisk.common.TransactionId
import whisk.core.entity.ActivationId
import whisk.core.entity.WhiskActivation

/**
  * Created by tnorris on 6/25/17.
  */


trait ActivationTracker {
  implicit val actorSystem: ActorSystem
  /** The execution context for futures */
  implicit val executionContext: ExecutionContext = actorSystem.dispatcher
  implicit val logging: Logging
  case class ActivationEntry(id: ActivationId, subject: String, invokerName: String, created: Instant, promise: Promise[WhiskActivation])
  type TrieSet[T] = TrieMap[T, Unit]
  private val activationById = new TrieMap[ActivationId, ActivationEntry]
  private val activationByInvoker = new TrieMap[String, TrieSet[ActivationEntry]]
  private val activationBySubject = new TrieMap[String, TrieSet[ActivationEntry]]



  /**
    * Creates an activation entry and insert into various maps.
    */
  def setupActivation(activationId: ActivationId, subject: String, invokerName: String, timeout: FiniteDuration, transid: TransactionId): ActivationEntry = {
    // either create a new promise or reuse a previous one for this activation if it exists
    val entry = activationById.getOrElseUpdate(activationId, {

      // install a timeout handler; this is the handler for "the action took longer than ActiveAckTimeout"
      // note the use of WeakReferences; this is to avoid the handler's closure holding on to the
      // WhiskActivation, which in turn holds on to the full JsObject of the response
      // NOTE: we do not remove the entry from the maps, as this is done only by processCompletion
      val promiseRef = new java.lang.ref.WeakReference(Promise[WhiskActivation])
      actorSystem.scheduler.scheduleOnce(timeout) {
        activationById.get(activationId).foreach { _ =>
          val p = promiseRef.get
          if (p != null && p.tryFailure(new ActiveAckTimeout(activationId))) {
            logging.info(this, "active response timed out")(transid)
          }
        }
      }
      ActivationEntry(activationId, subject, invokerName, Instant.now(Clock.systemUTC()), promiseRef.get)
    })

    // add the entry to our maps, for bookkeeping
    activationByInvoker.getOrElseUpdate(invokerName, new TrieSet[ActivationEntry]).put(entry, {})
    activationBySubject.getOrElseUpdate(subject, new TrieSet[ActivationEntry]).put(entry, {})
    entry
  }

  protected def processCompletion(tid:TransactionId, aid:ActivationId, response:WhiskActivation) = {
//    implicit val tid = msg.transid
//    val aid = msg.response.activationId
    logging.info(this, s"received active ack for '$aid'")
//    val response = msg.response
    activationById.remove(aid) match {
      case Some(entry @ ActivationEntry(_, subject, invokerIndex, _, p)) =>
        activationByInvoker.getOrElseUpdate(invokerIndex, new TrieSet[ActivationEntry]).remove(entry)
        activationBySubject.getOrElseUpdate(subject, new TrieSet[ActivationEntry]).remove(entry)
        p.trySuccess(response)
        logging.info(this, s"processed active response for '$aid'")
      case None =>
        logging.warn(this, s"processed active response for '$aid' which has no entry")
    }
  }
  private case class ActiveAckTimeout(activationId: ActivationId) extends TimeoutException

}
