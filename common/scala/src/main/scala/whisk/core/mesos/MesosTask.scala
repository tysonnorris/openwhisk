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

package whisk.core.mesos

import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.pattern.ask
import akka.util.Timeout
import com.adobe.api.platform.runtime.mesos.Bridge
import com.adobe.api.platform.runtime.mesos.DeleteTask
import com.adobe.api.platform.runtime.mesos.Running
import com.adobe.api.platform.runtime.mesos.SubmitTask
import com.adobe.api.platform.runtime.mesos.TaskDef
import java.time.Instant
import org.apache.mesos.v1.Protos.TaskStatus
import scala.collection.mutable.MultiMap
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps
import spray.json._
import whisk.common.Counter
import whisk.common.Logging
import whisk.common.TransactionId
import whisk.core.containerpool.Container
import whisk.core.containerpool.ContainerApi
import whisk.core.containerpool.ContainerId
import whisk.core.containerpool.ContainerIp
import whisk.core.entity.ByteSize
import whisk.core.entity.size._

/**
 * MesosTask implementation of Container
 */
case class Environment()
case class CreateContainer(image: String, memory: String, cpuShare: String)

object MesosTask {
  val taskLaunchTimeout = Timeout(45 seconds)
  val taskDeleteTimeout = Timeout(30 seconds)
  val counter = new Counter()
  val startTime = Instant.now.getEpochSecond

  def create(mesosClientActor: ActorRef,
             transid: TransactionId,
             image: String,
             userProvidedImage: Boolean = false,
             memory: ByteSize = 256.MB,
             cpuShares: Int = 0,
             environment: Map[String, String] = Map(),
             network: String = "bridge",
             dnsServers: Seq[String] = Seq(),
             name: Option[String] = None,
             parameters: MultiMap[String, String])(implicit ec: ExecutionContext,
                                                   log: Logging,
                                                   as: ActorSystem): Future[Container] = {
    implicit val tid = transid

    log.info(this, s"creating task for image ${image}...")

    val taskId = s"task-${counter.next()}-${startTime}"

    val mesosCpuShares = cpuShares / 1024.0 //convert openwhisk (docker based) shares to mesos (cpu percentage)
    val mesosRam = memory.toMB.toInt

    //TODO: use MultiMap in mesos-actor
    val flatParams = parameters.map(e => (e._1 -> e._2.head)).toMap
    val task = new TaskDef(
      taskId,
      image, //task name is currently based on image
      image,
      mesosCpuShares,
      mesosRam,
      List(8080), //all action containers listen on 8080
      Some(0), //port index 0 used for health
      false,
      Bridge,
      flatParams,
      environment)

    val launched: Future[Running] =
      mesosClientActor.ask(SubmitTask(task))(taskLaunchTimeout).mapTo[Running]

    launched.map(taskDetails => {
      val taskHost = taskDetails.hostname
      val taskPort = taskDetails.hostports(0)
      log.info(this, s"launched task with state ${taskDetails.taskStatus.getState} at ${taskHost}:${taskPort}")
      val containerIp = new ContainerIp(taskHost, taskPort)
      val containerId = new ContainerId(taskId);
      new MesosTask(containerId, containerIp, taskId, mesosClientActor, ec, log)
    }) recover {
      case t => throw new Exception(t)
    }

  }

}

object JsonFormatters extends DefaultJsonProtocol {
  implicit val createContainerJson = jsonFormat3(CreateContainer)
}
class MesosTask(override protected val id: ContainerId,
                override protected val ip: ContainerIp,
                val taskId: String,
                mesosClientActor: ActorRef,
                override protected val ec: ExecutionContext,
                override protected val logging: Logging)
    extends Container
    with ContainerApi {

  implicit val e = ec

  /** Stops the container from consuming CPU cycles. */
  override def suspend()(implicit transid: TransactionId): Future[Unit] = {
    //suspend not supported
    Future(Unit)
  }

  /** Dual of halt. */
  override def resume()(implicit transid: TransactionId): Future[Unit] = {
    //resume not supported
    Future(Unit)
  }

  /** Completely destroys this instance of the container. */
  override def destroy()(implicit transid: TransactionId): Future[Unit] = {
    mesosClientActor
      .ask(DeleteTask(taskId))(MesosTask.taskDeleteTimeout)
      .mapTo[TaskStatus]
      .map(taskStatus => {
        logging.info(this, s"task killed ended with state ${taskStatus.getState}")
      }) recover {
      case t => throw new Exception(t)
    }
  }

  /** Obtains logs up to a given threshold from the container. Optionally waits for a sentinel to appear. */
  override def logs(limit: ByteSize, waitForSentinel: Boolean)(
    implicit transid: TransactionId): Future[Vector[String]] =
    Future.successful(Vector("Logs are not collected currently"))
}
