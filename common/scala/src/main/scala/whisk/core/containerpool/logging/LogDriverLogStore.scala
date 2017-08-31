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

package whisk.core.containerpool.logging

import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigObject
import com.typesafe.config.ConfigValue
import java.util.Map.Entry
import scala.collection.JavaConverters._
import scala.concurrent.Future
import whisk.common.TransactionId
import whisk.core.containerpool.Container
import whisk.core.entity.ActivationLogs
import whisk.core.entity.ExecutableWhiskAction
import whisk.core.entity.WhiskActivation

trait LogDriverLogStore extends LogStore {
    val config = ConfigFactory.load()
    val logDriverOptions = (for {kindMapping: ConfigObject <- config.getObjectList("whisk.logstore.log-driver-opts").asScala
                        entry: Entry[String, ConfigValue] <- kindMapping.entrySet().asScala
                   } yield (entry.getKey, entry.getValue.unwrapped().toString)).toMap

    logDriverOptions.foreach(e => require(e._1 == "--log-opt" || e._1 == "--log-driver", s"Only --log_opt and --log_driver options may be set (found ${e._1})"))

    override def containerParameters = logDriverOptions
    def collectLogs(container:Container, action: ExecutableWhiskAction)(implicit transid: TransactionId): Future[Vector[String]] = Future.successful(Vector())//no logs collected when using docker log drivers

    def logs(activation: WhiskActivation): Future[ActivationLogs]

}

class NoLogsToApiLogStore extends LogDriverLogStore {
    override def logs(activation: WhiskActivation) = Future.successful(ActivationLogs())
}