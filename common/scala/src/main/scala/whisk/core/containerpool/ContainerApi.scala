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

package whisk.core.containerpool

import whisk.core.entity.ActivationResponse.ContainerConnectionError
import whisk.core.entity.ActivationResponse.ContainerResponse

class ContainerApi {

}

case class RunResult(interval: Interval, response: Either[ContainerConnectionError, ContainerResponse]) {
    def ok = response.right.exists(_.ok)
    def toBriefString = response.fold(_.toString, _.toString)
}

case class ContainerId(val asString: String) {
    require(asString.nonEmpty, "ContainerId must not be empty")
}
case class ContainerIp(val asString: String, val port:Int = 8080) {
    require(asString.nonEmpty, "ContainerIp must not be empty")
}