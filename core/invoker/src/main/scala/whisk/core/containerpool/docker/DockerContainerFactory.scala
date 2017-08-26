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

import akka.actor.ActorSystem
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import whisk.common.Logging
import whisk.common.TransactionId
import whisk.core.WhiskConfig
import whisk.core.containerpool.Container
import whisk.core.containerpool.ContainerFactory
import whisk.core.containerpool.ContainerFactoryProvider
import whisk.core.entity.ByteSize
import whisk.core.entity.ExecManifest
import whisk.spi.Dependencies
import whisk.spi.SpiFactory

class DockerContainerFactory()(implicit ec: ExecutionContext, logging: Logging) extends ContainerFactory{
    /** Initialize container clients */
    implicit val docker = new DockerClientWithFileAccess()(ec)
    implicit val runc = new RuncClient(ec)

    override def createContainer(tid: TransactionId, name: String, actionImage: ExecManifest.ImageName, userProvidedImage: Boolean, memory: ByteSize)
            (implicit config: WhiskConfig, logging: Logging): Future[Container] = {
        val image = if (userProvidedImage) {
            actionImage.publicImageName
        } else {
            actionImage.localImageName(config.dockerRegistry, config.dockerImagePrefix, Some(config.dockerImageTag))
        }

        DockerContainer.create(
            tid,
            image = image,
            userProvidedImage = userProvidedImage,
            memory = memory,
            cpuShares = config.invokerCoreShare.toInt,
            environment = Map("__OW_API_HOST" -> config.wskApiHost),
            network = config.invokerContainerNetwork,
            dnsServers = config.invokerContainerDns,
            name = Some(name))
    }
}

class DockerContainerFactoryProvider extends ContainerFactoryProvider {
    override def getContainerFactory(actorSystem: ActorSystem, logging: Logging, config: WhiskConfig): ContainerFactory =
        new DockerContainerFactory()(actorSystem.dispatcher, logging)
}

object DockerContainerFactoryProvider extends SpiFactory[ContainerFactoryProvider] {
    override def apply(dependencies: Dependencies): ContainerFactoryProvider = new DockerContainerFactoryProvider()
}