package whisk.core.mesos

import akka.actor.ActorSystem
import akka.pattern.ask
import com.adobe.api.platform.runtime.mesos.MesosClient
import com.adobe.api.platform.runtime.mesos.Subscribe
import com.adobe.api.platform.runtime.mesos.Teardown
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._
import whisk.common.Logging
import whisk.common.TransactionId
import whisk.core.WhiskConfig
import whisk.core.containerpool.Container
import whisk.core.containerpool.ContainerFactory
import whisk.core.containerpool.ContainerFactoryProvider
import whisk.core.containerpool.logging.LogStoreProvider
import whisk.core.entity.ByteSize
import whisk.core.entity.ExecManifest
import whisk.core.entity.UUID
import whisk.spi.Dependencies
import whisk.spi.SpiFactory
import whisk.spi.SpiLoader

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
class MesosContainerFactory(val actorSystem: ActorSystem,
                            val logging: Logging,
                            parameters: Map[String, String])
    extends ContainerFactory {
  //init mesos framework:
  implicit val as: ActorSystem = actorSystem
  implicit val ec: ExecutionContext = actorSystem.dispatcher
  val logStore =
    SpiLoader.get[LogStoreProvider]().logStore(actorSystem)
  val mesosMaster =
    actorSystem.settings.config.getString("whisk.mesos.master-url")
  val maxConcurrency =
    actorSystem.settings.config.getInt("whisk.mesos.max-concurrent")
  logging.info(this, s"subscribing to mesos master at ${mesosMaster}")

  val mesosClientActor = actorSystem.actorOf(
    MesosClient.props(
      "whisk-loadbalancer-" + UUID(),
      "whisk-loadbalancer-framework",
      mesosMaster,
      "*",
      2.minutes
    ))

  mesosClientActor ! Subscribe

  //handle shutdown
  sys.addShutdownHook({
    val complete: Future[Any] = mesosClientActor.ask(Teardown)(20.seconds)
    Await.result(complete, 25.seconds)
    logging.info(this, "teardown completed!")
  })

  override def createContainer(tid: TransactionId,
                               name: String,
                               actionImage: ExecManifest.ImageName,
                               userProvidedImage: Boolean,
                               memory: ByteSize)(
      implicit config: WhiskConfig,
      logging: Logging): Future[Container] = {
    //TODO: install all images in adobe
    val image = if (userProvidedImage) {
      logging.info(this, "using public image")
      actionImage.publicImageName
    } else {
      logging.info(this, "using local image")
      actionImage.localImageName(config.dockerRegistry,
                                 config.dockerImagePrefix,
                                 Some(config.dockerImageTag))
    }

    logging.info(this,
                 s"using Mesos to create a container with image ${image}...")
    val startingTask = MesosTask.create(
      mesosClientActor,
      tid,
      image = image,
      userProvidedImage = userProvidedImage,
      memory = memory,
      cpuShares = 0, //OldContainerPool.cpuShare(config),
      environment = Map("__OW_API_HOST" -> config.wskApiHost),
      network = config.invokerContainerNetwork,
      dnsServers = config.invokerContainerDns,
      name = Some(name),
      parameters
    )

    logging.info(this,
                 s"created task is completed??? ${startingTask.isCompleted}")
    startingTask.map(runningTask => {
      logging.info(this, "returning running task")
      runningTask
    })
  }

}

class MesosContainerFactoryProvider extends ContainerFactoryProvider {
  override def getContainerFactory(
      actorSystem: ActorSystem,
      logging: Logging,
      config: WhiskConfig,
      parameters: Map[String, String]): ContainerFactory =
    new MesosContainerFactory(actorSystem, logging, parameters)
}

object MesosContainerFactoryProvider
    extends SpiFactory[ContainerFactoryProvider] {
  override def apply(dependencies: Dependencies): ContainerFactoryProvider =
    new MesosContainerFactoryProvider()
}
