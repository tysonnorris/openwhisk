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

package whisk.core.database

import java.nio.charset.StandardCharsets

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import akka.actor.ActorSystem
import akka.actor.Props
import spray.json._
import whisk.common.Logging
import whisk.core.WhiskConfig
import whisk.core.connector.Message
import whisk.core.connector.MessageFeed
import whisk.core.connector.MessagingProvider
import whisk.core.entity.CacheKey
import whisk.core.entity.InstanceId
import whisk.core.entity.WhiskAction
import whisk.core.entity.WhiskPackage
import whisk.core.entity.WhiskRule
import whisk.core.entity.WhiskTrigger
import whisk.spi.SpiLoader

case class CacheInvalidationMessage(key: CacheKey, instanceId: String) extends Message {
  override def serialize = CacheInvalidationMessage.serdes.write(this).compactPrint
}

object CacheInvalidationMessage extends DefaultJsonProtocol {
  def parse(msg: String) = Try(serdes.read(msg.parseJson))
  implicit val serdes = jsonFormat(CacheInvalidationMessage.apply _, "key", "instanceId")
}

class RemoteCacheInvalidation(config: WhiskConfig, component: String, instance: InstanceId)(implicit logging: Logging,
                                                                                            as: ActorSystem) {

  implicit private val ec = as.dispatcher

  private val topic = "cacheInvalidation"
  private val instanceId = s"$component${instance.toInt}"

  private val msgProvider = SpiLoader.get[MessagingProvider]
  private val cacheInvalidationConsumer = msgProvider.getConsumer(config, s"$topic$instanceId", topic, maxPeek = 128)
  private val cacheInvalidationProducer = msgProvider.getProducer(config, ec)

  def notifyOtherInstancesAboutInvalidation(key: CacheKey): Future[Unit] = {
    cacheInvalidationProducer.send(topic, CacheInvalidationMessage(key, instanceId)).map(_ => Unit)
  }

  private val invalidationFeed = as.actorOf(Props {
    new MessageFeed(
      "cacheInvalidation",
      logging,
      cacheInvalidationConsumer,
      cacheInvalidationConsumer.maxPeek,
      1.second,
      removeFromLocalCache)
  })

  private def removeFromLocalCache(bytes: Array[Byte]): Future[Unit] = Future {
    val raw = new String(bytes, StandardCharsets.UTF_8)

    CacheInvalidationMessage.parse(raw) match {
      case Success(msg: CacheInvalidationMessage) => {
        if (msg.instanceId != instanceId) {
          WhiskAction.removeId(msg.key)
          WhiskPackage.removeId(msg.key)
          WhiskRule.removeId(msg.key)
          WhiskTrigger.removeId(msg.key)
        }
      }
      case Failure(t) => logging.error(this, s"failed processing message: $raw with $t")
    }
    invalidationFeed ! MessageFeed.Processed
  }
}
