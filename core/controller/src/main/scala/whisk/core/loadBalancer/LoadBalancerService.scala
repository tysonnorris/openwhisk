/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package whisk.core.loadBalancer

import java.nio.charset.StandardCharsets
import java.time.{Clock, Instant}

import scala.collection.concurrent.TrieMap
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.TimeoutException
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration
import scala.util.Failure
import scala.util.Success
import org.apache.kafka.clients.producer.RecordMetadata
import akka.actor.ActorRefFactory
import akka.actor.ActorSystem
import akka.pattern.ask
import akka.util.Timeout
import scaldi.Injector
import whisk.common.ConsulClient
import whisk.common.Logging
import whisk.common.LoggingMarkers
import whisk.common.TransactionId
import whisk.core.WhiskConfig
import whisk.core.WhiskConfig._
import whisk.core.connector.MessagingProvider
import whisk.core.connector.{ActivationMessage, CompletionMessage}
import whisk.core.connector.MessageProducer
import whisk.core.database.NoDocumentException
import whisk.core.entity.{ActivationId, CodeExec, WhiskAction, WhiskActivation}
import whisk.core.entity.InstanceId
import whisk.core.entity.WhiskAction
import whisk.core.entity.types.EntityStore
import whisk.spi.SpiFactoryModule
import whisk.spi.SpiModule

import scala.annotation.tailrec

trait LoadBalancer {

    /**
     * Retrieves a per subject map of counts representing in-flight activations as seen by the load balancer
     *
     * @return a map where the key is the subject and the long is total issued activations by that user
     */
    def getActiveUserActivationCounts: Map[String, Int]

    /**
     * Publishes activation message on internal bus for the invoker to pick up.
     *
     * @param msg the activation message to publish on an invoker topic
     * @param timeout the desired active ack timeout
     * @param transid the transaction id for the request
     * @return result a nested Future the outer indicating completion of publishing and
     *         the inner the completion of the action (i.e., the result)
     *         if it is ready before timeout otherwise the future fails with ActiveAckTimeout
     */
    def publish(action: WhiskAction, msg: ActivationMessage, timeout: FiniteDuration)(implicit transid: TransactionId): Future[Future[WhiskActivation]]

}

class LoadBalancerService(config: WhiskConfig, instance: InstanceId, entityStore: EntityStore)(implicit val actorSystem: ActorSystem, logging: Logging) extends LoadBalancer {

    /** The execution context for futures */
    implicit val executionContext: ExecutionContext = actorSystem.dispatcher

    /** How many invokers are dedicated to blackbox images.  We range bound to something sensical regardless of configuration. */
    private val blackboxFraction: Double = Math.max(0.0, Math.min(1.0, config.controllerBlackboxFraction))
    logging.info(this, s"blackboxFraction = $blackboxFraction")

    override def getActiveUserActivationCounts: Map[String, Int] = activationBySubject.toMap mapValues { _.size }

    override def publish(action: WhiskAction, msg: ActivationMessage, timeout: FiniteDuration)(
        implicit transid: TransactionId): Future[Future[WhiskActivation]] = {
        chooseInvoker(action, msg).flatMap { invokerName =>
            val subject = msg.user.subject.asString
            val entry = setupActivation(msg.activationId, subject, invokerName, timeout, transid)
            sendActivationToInvoker(messageProducer, msg, invokerName).map { _ =>
                entry.promise.future
            }
        }
    }

    def invokerHealth: Future[Map[String, InvokerState]] = invokerPool.ask(GetStatus)(Timeout(5.seconds)).mapTo[Map[String, InvokerState]]

    /**
     * A map storing current activations based on ActivationId.
     * The promise value represents the obligation of writing the answer back.
     */
    case class ActivationEntry(id: ActivationId, subject: String, invokerName: String, created: Instant, promise: Promise[WhiskActivation])
    type TrieSet[T] = TrieMap[T, Unit]
    private val activationById = new TrieMap[ActivationId, ActivationEntry]
    private val activationByInvoker = new TrieMap[String, TrieSet[ActivationEntry]]
    private val activationBySubject = new TrieMap[String, TrieSet[ActivationEntry]]

    /**
     * Tries to fill in the result slot (i.e., complete the promise) when a completion message arrives.
     * The promise is removed form the map when the result arrives or upon timeout.
     *
     * @param msg is the kafka message payload as Json
     */
    private def processCompletion(msg: CompletionMessage) = {
        implicit val tid = msg.transid
        val aid = msg.response.activationId
        logging.info(this, s"received active ack for '$aid'")
        val response = msg.response
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

    /**
     * Creates an activation entry and insert into various maps.
     */
    private def setupActivation(activationId: ActivationId, subject: String, invokerName: String, timeout: FiniteDuration, transid: TransactionId): ActivationEntry = {
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

    /**
     * When invoker health detects a new invoker has come up, this callback is called.
     */
    private def clearInvokerState(invokerName: String) = {
        val actSet = activationByInvoker.getOrElseUpdate(invokerName, new TrieSet[ActivationEntry])
        actSet.keySet map {
            case actEntry @ ActivationEntry(activationId, subject, invokerIndex, _, promise) =>
                promise.tryFailure(new LoadBalancerException(s"Invoker $invokerIndex restarted"))
                activationById.remove(activationId)
                activationBySubject.get(subject) map { _.remove(actEntry) }
        }
        actSet.clear()
    }

    /** Make a new immutable map so caller cannot mess up the state */
    private def getActiveCountByInvoker(): Map[String, Int] = activationByInvoker.toMap mapValues { _.size }

    /**
     * Creates or updates a health test action by updating the entity store.
     * This method is intended for use on startup.
     * @return Future that completes successfully iff the action is added to the database
     */
    private def createTestActionForInvokerHealth(db: EntityStore, action: WhiskAction): Future[Unit] = {
        implicit val tid = TransactionId.loadbalancer
        WhiskAction.get(db, action.docid).flatMap { oldAction =>
            WhiskAction.put(db, action.revision(oldAction.rev))
        }.recover {
            case _: NoDocumentException => WhiskAction.put(db, action)
        }.map(_ => {}).andThen {
            case Success(_) => logging.info(this, "test action for invoker health now exists")
            case Failure(e) => logging.error(this, s"error creating test action for invoker health: $e")
        }
    }

    /** Gets a producer which can publish messages to the kafka bus. */
    private val messageProducer = MessagingProvider(actorSystem).getProducer()

    private def sendActivationToInvoker(producer: MessageProducer, msg: ActivationMessage, invokerName: String): Future[RecordMetadata] = {
        implicit val transid = msg.transid
        val start = transid.started(this, LoggingMarkers.CONTROLLER_KAFKA, s"posting topic '$invokerName' with activation id '${msg.activationId}'")

        producer.send(invokerName, msg).andThen {
            case Success(status) => transid.finished(this, start, s"posted to ${status.topic()}[${status.partition()}][${status.offset()}]")
            case Failure(e)      => transid.failed(this, start, s"error on posting to topic $invokerName")
        }
    }

    private val invokerPool = {
        // Do not create the invokerPool if it is not possible to create the health test action to recover the invokers.
        InvokerPool.healthAction.map {
            // Await the creation of the test action; on failure, this will abort the constructor which should
            // in turn abort the startup of the controller.
            a => Await.result(createTestActionForInvokerHealth(entityStore, a), 1.minute)
        }.orElse {
            throw new IllegalStateException("cannot create test action for invoker health because runtime manifest is not valid")
        }

        val consul = new ConsulClient(config.consulServer)
        val pingConsumer = MessagingProvider(actorSystem).getConsumer(s"health${instance.toInt}", "health")
        val invokerFactory = (f: ActorRefFactory, name: String) => f.actorOf(InvokerActor.props(instance), name)

        actorSystem.actorOf(InvokerPool.props(invokerFactory, consul.kv, invoker => {
            clearInvokerState(invoker)
            logging.info(this, s"cleared load balancer state for $invoker")(TransactionId.invokerHealth)
        }, (m, i) => sendActivationToInvoker(messageProducer, m, i), pingConsumer))
    }

    /** Subscribes to active acks (completion messages from the invokers). */
    private val activeAckConsumer = MessagingProvider(actorSystem).getConsumer("completions", s"completed${instance.toInt}")
    /** Registers a handler for received active acks from invokers. */
    activeAckConsumer.onMessage((topic, _, _, bytes) => {
        val raw = new String(bytes, StandardCharsets.UTF_8)
        CompletionMessage.parse(raw) match {
            case Success(m: CompletionMessage) =>
                processCompletion(m)
                invokerPool ! InvocationFinishedMessage(m.invoker, !m.response.response.isWhiskError)

            case Failure(t) => logging.error(this, s"failed processing message: $raw with $t")
        }
    })

    /** Return a sorted list of available invokers. */
    private def availableInvokers: Future[Seq[String]] = invokerHealth.map {
        _.collect {
            case (name, Healthy) => name
        }.toSeq.sortBy(_.drop(7).toInt) // Sort by the number in "invokerN"
    }.recover {
        case _ => Seq.empty[String]
    }

    /** Compute the number of blackbox-dedicated invokers by applying a rounded down fraction of all invokers (but at least 1). */
    private def numBlackbox(totalInvokers: Int) = Math.max(1, (totalInvokers.toDouble * blackboxFraction).toInt)

    /** Return invokers (almost) dedicated to running blackbox actions. */
    private def blackboxInvokers: Future[Seq[String]] = availableInvokers.map { allInvokers =>
        allInvokers.takeRight(numBlackbox(allInvokers.length))
    }

    /** Return (at least one) invokers for running non black-box actions.  This set can overlap with the blackbox set if there is only one invoker. */
    private def managedInvokers: Future[Seq[String]] = availableInvokers.map { allInvokers =>
        val numManaged = Math.max(1, allInvokers.length - numBlackbox(allInvokers.length))
        allInvokers.take(numManaged)
    }

    /** Determine which invoker this activation should go to. Due to dynamic conditions, it may return no invoker. */
    private def chooseInvoker(action: WhiskAction, msg: ActivationMessage): Future[String] = {
        val isBlackbox = action.exec match {
            case e: CodeExec[_] => e.pull
            case _              => false
        }
        val invokers = if (isBlackbox) blackboxInvokers else managedInvokers
        val hash = hashSubjectAction(msg)

        invokers.flatMap { invokers =>
            LoadBalancerService.schedule(
                invokers,
                activationByInvoker.mapValues(_.size),
                config.loadbalancerInvokerBusyThreshold,
                hash) match {
                    case Some(invoker) => Future.successful(invoker)
                    case None =>
                        logging.error(this, s"all invokers down")(TransactionId.invokerHealth)
                        Future.failed(new LoadBalancerException("no invokers available"))
                }
        }
    }

    /*
     * The path contains more than the action per se but seems sufficiently
     * isomorphic as the other parts are constant.  Extracting just the
     * action out specifically will involve some hairy regex's that the
     * Invoker is currently using and which is better avoid if/until
     * these are moved to some common place (like a subclass of Message?)
     */
    private def hashSubjectAction(msg: ActivationMessage): Int = {
        val subject = msg.user.subject.asString
        val path = msg.action.toString
        (subject.hashCode() ^ path.hashCode()).abs
    }
}

object LoadBalancerService {
    def requiredProperties = kafkaHost ++ consulServer ++
        Map(loadbalancerInvokerBusyThreshold -> null)

    /** Memoizes the result of `f` for later use. */
    def memoize[I, O](f: I => O): I => O = new scala.collection.mutable.HashMap[I, O]() {
        override def apply(key: I) = getOrElseUpdate(key, f(key))
    }

    /** Euclidean algorithm to determine the greatest-common-divisor */
    @tailrec
    def gcd(a: Int, b: Int): Int = if (b == 0) a else gcd(b, a % b)

    /** Returns pairwise coprime numbers until x. Result is memoized. */
    val pairwiseCoprimeNumbersUntil: Int => IndexedSeq[Int] = LoadBalancerService.memoize {
        case x =>
            (1 to x).foldLeft(IndexedSeq.empty[Int])((primes, cur) => {
                if (gcd(cur, x) == 1 && primes.forall(i => gcd(i, cur) == 1)) {
                    primes :+ cur
                } else primes
            })
    }

    /**
     * Scans through all invokers and searches for an invoker, that has a queue length
     * below the defined threshold. Iff no "underloaded" invoker was found it will
     * default to the least loaded invoker in the list.
     *
     * @param availableInvokers a list of available (healthy) invokers to search in
     * @param activationsPerInvoker a map of the number of outstanding activations per invoker
     * @param invokerBusyThreshold defines when an invoker is considered overloaded
     * @param hash stable identifier of the entity to be scheduled
     * @return an invoker to schedule to or None of no invoker is available
     */
    def schedule[A](
        availableInvokers: Seq[A],
        activationsPerInvoker: collection.Map[A, Int],
        invokerBusyThreshold: Int,
        hash: Int): Option[A] = {

        val numInvokers = availableInvokers.length
        if (numInvokers > 0) {
            val homeInvoker = hash % numInvokers

            val stepSizes = LoadBalancerService.pairwiseCoprimeNumbersUntil(numInvokers)
            val step = stepSizes(hash % stepSizes.size)

            @tailrec
            def search(targetInvoker: Int, seenInvokers: Int): A = {
                // map the computed index to the actual invoker index
                val invokerName = availableInvokers(targetInvoker)

                // send the request to the target invoker if it has capacity...
                if (activationsPerInvoker.get(invokerName).getOrElse(0) < invokerBusyThreshold) {
                    invokerName
                } else {
                    // ... otherwise look for a less loaded invoker by stepping through a pre computed
                    // list of invokers; there are two possible outcomes:
                    // 1. the search lands on a new invoker that has capacity, choose it
                    // 2. walked through the entire list and found no better invoker than the
                    //    "home invoker", choose the least loaded invoker
                    val newTarget = (targetInvoker + step) % numInvokers
                    if (newTarget == homeInvoker || seenInvokers > numInvokers) {
                        // fall back to the invoker with the least load.
                        activationsPerInvoker.minBy(_._2)._1
                    } else {
                        search(newTarget, seenInvokers + 1)
                    }
                }
            }

            Some(search(homeInvoker, 0))
        } else {
            None
        }
    }

}

class InvokerLoadBalancerServiceProvider(config: WhiskConfig, instance: InstanceId, entityStore: EntityStore)(implicit val actorSystem: ActorSystem, logging: Logging) extends LoadBalancerProvider {
    override def getLoadBalancer(config: WhiskConfig, instance: InstanceId, entityStore: EntityStore): LoadBalancer =
        new LoadBalancerService(config, instance, entityStore)(actorSystem, logging)
}
class LoadBalancerServiceModule extends SpiFactoryModule[LoadBalancerProvider]{
    override def getInstance(implicit injector: Injector): LoadBalancerProvider = {
        new InvokerLoadBalancerServiceProvider(inject[WhiskConfig], inject[InstanceId], inject[EntityStore])(inject[ActorSystem], inject[Logging])
    }
}


private case class ActiveAckTimeout(activationId: ActivationId) extends TimeoutException
private case class LoadBalancerException(msg: String) extends Throwable(msg)
