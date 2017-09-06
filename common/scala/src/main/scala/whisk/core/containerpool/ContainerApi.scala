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

import java.time.Instant
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.Failure
import scala.util.Success
import spray.json.JsObject
import spray.json.DefaultJsonProtocol._
import whisk.common.Logging
import whisk.common.LoggingMarkers
import whisk.common.TransactionId
import whisk.core.entity.ActivationResponse
import whisk.core.entity.ActivationResponse.ContainerConnectionError
import whisk.core.entity.ActivationResponse.ContainerResponse
import whisk.core.entity.size._
import whisk.http.Messages

trait ContainerApi { container: Container =>
  protected val id: ContainerId
  protected val ip: ContainerIp

  protected implicit val logging: Logging
  protected implicit val ec: ExecutionContext

  /** HTTP connection to the container, will be lazily established by callContainer */
  private var httpConnection: Option[HttpUtils] = None

  def initialize(initializer: JsObject, timeout: FiniteDuration)(implicit transid: TransactionId): Future[Interval] = {
    val start = transid.started(this, LoggingMarkers.INVOKER_ACTIVATION_INIT, s"sending initialization to $id $ip")

    val body = JsObject("value" -> initializer)
    callContainer("/init", body, timeout, retry = true)
      .andThen { // never fails
        case Success(r: RunResult) =>
          transid.finished(
            this,
            start.copy(start = r.interval.start),
            s"initialization result: ${r.toBriefString}",
            endTime = r.interval.end)
        case Failure(t) =>
          transid.failed(this, start, s"initializiation failed with $t")
      }
      .flatMap { result =>
        if (result.ok) {
          Future.successful(result.interval)
        } else if (result.interval.duration >= timeout) {
          Future.failed(
            InitializationError(
              result.interval,
              ActivationResponse.applicationError(Messages.timedoutActivation(timeout, true))))
        } else {
          Future.failed(
            InitializationError(
              result.interval,
              ActivationResponse.processInitResponseContent(result.response, logging)))
        }
      }
  }

  def run(parameters: JsObject, environment: JsObject, timeout: FiniteDuration)(
    implicit transid: TransactionId): Future[(Interval, ActivationResponse)] = {
    val actionName = environment.fields.get("action_name").map(_.convertTo[String]).getOrElse("")
    val start =
      transid.started(this, LoggingMarkers.INVOKER_ACTIVATION_RUN, s"sending arguments to $actionName at $id $ip")

    val parameterWrapper = JsObject("value" -> parameters)
    val body = JsObject(parameterWrapper.fields ++ environment.fields)
    callContainer("/run", body, timeout, retry = false)
      .andThen { // never fails
        case Success(r: RunResult) =>
          transid.finished(
            this,
            start.copy(start = r.interval.start),
            s"running result: ${r.toBriefString}",
            endTime = r.interval.end)
        case Failure(t) =>
          transid.failed(this, start, s"run failed with $t")
      }
      .map { result =>
        val response = if (result.interval.duration >= timeout) {
          ActivationResponse.applicationError(Messages.timedoutActivation(timeout, false))
        } else {
          ActivationResponse.processRunResponseContent(result.response, logging)
        }

        (result.interval, response)
      }
  }

  /**
   * Makes an HTTP request to the container.
   *
   * Note that `http.post` will not throw an exception, hence the generated Future cannot fail.
   *
   * @param path relative path to use in the http request
   * @param body body to send
   * @param timeout timeout of the request
   * @param retry whether or not to retry the request
   */
  protected def callContainer(path: String,
                              body: JsObject,
                              timeout: FiniteDuration,
                              retry: Boolean = false): Future[RunResult] = {
    val started = Instant.now()
    val http = httpConnection.getOrElse {
      val conn = new HttpUtils(s"${ip.asString}:8080", timeout, 1.MB)
      httpConnection = Some(conn)
      conn
    }
    Future {
      http.post(path, body, retry)
    }.map { response =>
      val finished = Instant.now()
      RunResult(Interval(started, finished), response)
    }
  }

  def destroyApi()(implicit transid: TransactionId): Future[Unit] = {
    httpConnection.foreach(_.close())
    Future.successful(Unit)
  }
}

case class RunResult(interval: Interval, response: Either[ContainerConnectionError, ContainerResponse]) {
  def ok = response.right.exists(_.ok)
  def toBriefString = response.fold(_.toString, _.toString)
}
