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

package whisk.core.dispatcher

import akka.actor.Actor
import akka.actor.actorRef2Scala
import org.apache.kafka.clients.consumer.CommitFailedException
import whisk.common.Logging
import whisk.common.TransactionId
import whisk.core.connector.MessageConsumer

import scala.concurrent.duration.FiniteDuration
import scala.util.Try

object ActivationFeed {
    sealed class ActivationNotification

    /** Pulls new messages from the message bus. */
    case class FillQueueWithMessages()

    /** Indicates resources are available because transaction completed, may cause pipeline fill. */
    case object ContainerReleased extends ActivationNotification

    /** Indicate resources are available because transaction failed, may cause pipeline fill. */
    case class FailedActivation(tid: TransactionId) extends ActivationNotification
}

/**
 * This actor polls the message bus for new messages and dispatches them to the given
 * handler. The actor tracks the number of messages dispatched and will not dispatch new
 * messages until some number of them are acknowledged.
 *
 * This is used by the invoker to pull messages from the message bus and apply back pressure
 * when the invoker does not have resources to complete processing messages (i.e., no containers
 * are available to run new actions).
 *
 * When the invoker releases resources (by reclaiming containers) it will send a message
 * to this actor which will then attempt to fill the pipeline with new messages.
 *
 * The actor tries to fill the pipeline with additional messages while the number
 * of outstanding requests is below the pipeline fill threshold.
 */
@throws[IllegalArgumentException]
protected class ActivationFeed(
    logging: Logging,
    consumer: MessageConsumer,
    maxPipelineDepth: Int,
    longpollDuration: FiniteDuration,
    handler: (String, Array[Byte]) => Any)
    extends Actor {
    import ActivationFeed.ActivationNotification
    import ActivationFeed.FillQueueWithMessages

    require(consumer.maxPeek <= maxPipelineDepth, "consumer may not yield more messages per peek than permitted by max depth")

    private val pipelineFillThreshold = maxPipelineDepth - consumer.maxPeek
    private var pipelineOccupancy = 0
    private implicit val tid = TransactionId.dispatcher

    override def receive = {
        case FillQueueWithMessages =>
            if (pipelineOccupancy <= pipelineFillThreshold) {
                Try {
                    // Grab next batch of messages and commit offsets immediately
                    // essentially marking the activation as having satisfied "at most once"
                    // semantics (this is the point at which the activation is considered started).
                    // If the commit fails, then messages peeked are peeked again on the next poll.
                    // While the commit is synchronous and will block until it completes, at steady
                    // state with enough buffering (i.e., maxPipelineDepth > maxPeek), the latency
                    // of the commit should be masked.
                    val records = consumer.peek(longpollDuration)
                    consumer.commit()
                    (records, records.size)
                } map {
                    case (records, count) =>
                        records foreach {
                            case (topic, partition, offset, bytes) =>
                                pipelineOccupancy += 1
                                logging.info(this, s"processing $topic[$partition][$offset ($count)][pipelineOccupancy=${pipelineOccupancy} (${pipelineFillThreshold})]")
                                handler(topic, bytes)
                        }
                } recover {
                    case e: CommitFailedException => logging.error(this, s"failed to commit consumer offset: $e")
                    case e: Throwable             => logging.error(this, s"exception while pulling new records: $e")
                }
                fill()
            } else logging.debug(this, "dropping fill request until feed is drained")

        case n: ActivationNotification =>
            pipelineOccupancy -= 1
            logging.info(this, s"received ActivationNotification: $n / pipelineOccupancy=$pipelineOccupancy / pipelineFillThreshold=$pipelineFillThreshold")
            if (pipelineOccupancy < 0) {
                logging.error(this, "pipelineOccupancy<0")
            }
            fill()
    }

    private def fill() = {
        if (pipelineOccupancy <= pipelineFillThreshold) {
            logging.debug(this, s"filling activation pipeline: $pipelineOccupancy <= $pipelineFillThreshold")
            self ! FillQueueWithMessages
        } else {
            logging.info(this, s"waiting for activation pipeline to drain: $pipelineOccupancy > $pipelineFillThreshold")
        }
    }
}
