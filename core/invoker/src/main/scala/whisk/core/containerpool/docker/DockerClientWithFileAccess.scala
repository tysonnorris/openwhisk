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

package whisk.core.containerpool.docker

import java.io.File
import java.io.FileInputStream
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.file.Paths

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.blocking
import scala.io.Source

import spray.json.DefaultJsonProtocol._
import spray.json._
import whisk.common.Logging
import whisk.common.TransactionId

class DockerClientWithFileAccess(
  dockerHost: Option[String] = None,
  containersDirectory: File = Paths.get("containers").toFile)(executionContext: ExecutionContext)(implicit log: Logging)
    extends DockerClient(dockerHost)(executionContext)(log)
    with DockerApiWithFileAccess {

  implicit private val ec = executionContext

  /**
   * Provides the home directory of the specified Docker container.
   *
   * Assumes that property "containersDirectory" holds the location of the
   * home directory of all Docker containers. Default: directory "containers"
   * in the current working directory.
   *
   * Does not verify that the returned directory actually exists.
   *
   * @param containerId Id of the desired Docker container
   * @return canonical location of the container's home directory
   */
  protected def containerDirectory(containerId: ContainerId) = {
    new File(containersDirectory, containerId.asString).getCanonicalFile()
  }

  /**
   * Provides the configuration file of the specified Docker container.
   *
   * Assumes that the file has the well-known location and name.
   *
   * Does not verify that the returned file actually exists.
   *
   * @param containerId Id of the desired Docker container
   * @return canonical location of the container's configuration file
   */
  protected def containerConfigFile(containerId: ContainerId) = {
    new File(containerDirectory(containerId), "config.v2.json").getCanonicalFile()
  }

  /**
   * Provides the log file of the specified Docker container written by
   * Docker's JSON log driver.
   *
   * Assumes that the file has the well-known location and name.
   *
   * Does not verify that the returned file actually exists.
   *
   * @param containerId Id of the desired Docker container
   * @return canonical location of the container's log file
   */
  protected def containerLogFile(containerId: ContainerId) = {
    new File(containerDirectory(containerId), s"${containerId.asString}-json.log").getCanonicalFile()
  }

  /**
   * Provides the contents of the specified Docker container's configuration
   * file as JSON object.
   *
   * @param configFile the container's configuration file in JSON format
   * @return contents of configuration file as JSON object
   */
  protected def configFileContents(configFile: File): Future[JsObject] = Future {
    blocking { // Needed due to synchronous file operations
      val source = Source.fromFile(configFile)
      val config = try source.mkString
      finally source.close()
      config.parseJson.asJsObject
    }
  }

  /**
   * Extracts the IP of the container from the local config file of the docker daemon.
   *
   * A container may have more than one network. The container has an
   * IP address in each of these networks such that the network name
   * is needed.
   *
   * @param id the id of the container to get the IP address from
   * @param network name of the network to get the IP address from
   * @return the ip address of the container
   */
  protected def ipAddressFromFile(id: ContainerId, network: String): Future[ContainerIp] = {
    configFileContents(containerConfigFile(id)).map { json =>
      val networks = json.fields("NetworkSettings").asJsObject.fields("Networks").asJsObject
      val specifiedNetwork = networks.fields(network).asJsObject
      val ipAddr = specifiedNetwork.fields("IPAddress")
      ContainerIp(ipAddr.convertTo[String])
    }
  }

  // See extended trait for description
  override def inspectIPAddress(id: ContainerId, network: String)(
    implicit transid: TransactionId): Future[ContainerIp] = {
    ipAddressFromFile(id, network).recoverWith {
      case _ => super.inspectIPAddress(id, network)
    }
  }

  // See extended trait for description
  def rawContainerLogs(containerId: ContainerId, fromPos: Long): Future[ByteBuffer] = Future {
    blocking { // Needed due to synchronous file operations
      var fis: FileInputStream = null
      try {
        val file = containerLogFile(containerId)
        val size = file.length

        fis = new FileInputStream(file)
        val channel = fis.getChannel().position(fromPos)

        // Buffer allocation may fail if the log file is too large to hold in memory or
        // too few space is left on the heap, respectively.
        var remainingBytes = (size - fromPos).toInt
        val readBuffer = ByteBuffer.allocate(remainingBytes)

        while (remainingBytes > 0) {
          val readBytes = channel.read(readBuffer)
          if (readBytes > 0) {
            remainingBytes -= readBytes
          } else if (readBytes < 0) {
            remainingBytes = 0
          }
        }

        readBuffer
      } catch {
        case e: Exception =>
          throw new IOException(s"rawContainerLogs failed on ${containerId}", e)
      } finally {
        if (fis != null) fis.close()
      }
    }
  }
}

trait DockerApiWithFileAccess extends DockerApi {

  /**
   * Obtains the container's stdout and stderr by reading the internal docker log file
   * for the container. Said file is written by docker's JSON log driver and has
   * a "well-known" location and name.
   *
   * Reads the log file from the specified position to its end. The returned ByteBuffer
   * indicates how many bytes were actually read from the file.
   *
   * Attention: a ByteBuffer is allocated to keep the file from the specified position to its end
   * fully in memory. At the moment, there is no size limit checking which can lead to
   * out-of-memory exceptions for very large files.
   *
   * Deals with incomplete reads and premature end of file situations. Behavior is undefined
   * if the log file is changed or truncated while reading.
   *
   * @param containerId the container for which to provide logs
   * @param fromPos position where to start reading the container's log file
   * @return a ByteBuffer holding the read log file contents
   */
  def rawContainerLogs(containerId: ContainerId, fromPos: Long): Future[ByteBuffer]
}
