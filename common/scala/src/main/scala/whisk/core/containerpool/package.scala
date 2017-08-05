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

package whisk.core

import java.time.Instant
import scala.concurrent.duration._
import whisk.core.entity.ActivationResponse._

/**
 * This object contains type definitions that are useful when observing and timing container operations.
 */
package object containerpool {


    /**
     * Represents a time interval, which can be viewed as a duration for which
     *  the start/end instants are fully known (as opposed to being relative).
     */
    case class Interval(start: Instant, end: Instant) {
        def duration = Duration.create(end.toEpochMilli() - start.toEpochMilli(), MILLISECONDS)
    }

    object Interval {
        /** An interval starting now with zero duration. */
        def zero = {
            val now = Instant.now
            Interval(now, now)
        }
    }

    /**
     * Represents the result of accessing an endpoint in a container:
     * Start time, End time, Some(response) from container consisting of status code and payload
     * If there is no response or an exception, then None.
     */
    case class RunResult(interval: Interval, response: Either[ContainerConnectionError, ContainerResponse]) {
        def duration = interval.duration
        def ok = response.right.exists(_.ok)
        def errored = !ok
        def toBriefString = response.fold(_.toString, _.toString)
    }



}
