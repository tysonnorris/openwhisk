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

import spray.json.DefaultJsonProtocol._
import spray.json._
import whisk.common.Counter
import whisk.core.connector.ActivationMessage
import whisk.core.entity._
case class Run(action: ExecutableWhiskAction, msg: ActivationMessage)
object ContainerProxy {

    // Needs to be thread-safe as it's used by multiple proxies concurrently.
    private val containerCount = new Counter

    /**
     * Generates a unique container name.
     *
     * @param prefix the container name's prefix
     * @param suffix the container name's suffix
     * @return a unique container name
     */
    def containerName(instance: InstanceId, prefix: String, suffix: String) =
        s"wsk${instance.toInt}_${containerCount.next()}_${prefix}_${suffix}".replaceAll("[^a-zA-Z0-9_]", "")

    /**
     * Creates a WhiskActivation ready to be sent via active ack.
     *
     * @param job the job that was executed
     * @param interval the time it took to execute the job
     * @param response the response to return to the user
     * @return a WhiskActivation to be sent to the user
     */
    def constructWhiskActivation(job: Run, interval: Interval, response: ActivationResponse) = {
        val causedBy = if (job.msg.causedBySequence) Parameters("causedBy", "sequence".toJson) else Parameters()
        WhiskActivation(
            activationId = job.msg.activationId,
            namespace = job.msg.activationNamespace,
            subject = job.msg.user.subject,
            cause = job.msg.cause,
            name = job.action.name,
            version = job.action.version,
            start = interval.start,
            end = interval.end,
            duration = Some(interval.duration.toMillis),
            response = response,
            annotations = {
                Parameters("limits", job.action.limits.toJson) ++
                    Parameters("path", job.action.fullyQualifiedName(false).toString.toJson) ++ causedBy
            })
    }
}

/** Indicates an activation with a non-successful response */
case class ActivationUnsuccessfulError(activation: WhiskActivation) extends Exception(s"activation ${activation.activationId} failed")
