package whisk.core.mesos.mesos

import java.util.concurrent.atomic.AtomicLong

import akka.actor.{Actor, ActorRef, ActorRefFactory, Props}
import whisk.common.{AkkaLogging, Logging}
import whisk.core.containerpool.{PrewarmingConfig, Run}
import whisk.core.entity.size._
import whisk.core.entity.{ByteSize, CodeExec, EntityName, ExecutableWhiskAction}

import scala.collection.mutable
import scala.concurrent.Promise
import scala.concurrent.duration.{FiniteDuration, _}

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

//internal messages - from ContainerLifecycleProxy
case class ContainerCreated(job:Run, proxy:MesosContainerProxy)
case class ContainerInited(job:Run, proxy:MesosContainerProxy, data:WarmedData)

//incoming messages - from MesosContainerPool
case class GetContainer(client:ActorRef, action:ExecutableWhiskAction, maxConcurrent:Int = 1, knownTaskIds:Seq[String])
case class ReportUsage(client:ActorRef, proxy:MesosContainerProxy, currentActivations:Int)


//outgoing messages - to MesosContainerPool
case class ClientNeedWork(mesosContainerProxy: MesosContainerProxy)
case class ClientNotifyRemoval(mesosContainerProxy: MesosContainerProxy)

//internal messages from garbage collection
case class MarkUnused()
case class RemoveUnused()

class ContainerManager(childFactory: ActorRefFactory => ActorRef,
                       prewarmConfig: Option[PrewarmingConfig] = None
                             )
  extends Actor{

  implicit val logging = new AkkaLogging(context.system.log)
  implicit val ec = context.system.dispatcher
  val prewarmedPool = mutable.Map[ActorRef, PreWarmedData]()
//  val actorContainers = mutable.Map[ActorRef, MesosContainerProxy]()
//  val containerPromises = mutable.Map[String, Promise[Option[(MesosContainerProxy, ContainerData)]]]()

  val flaggedForRemoval = mutable.ListBuffer[MesosContainerProxy]()
  val subscribers = mutable.ListBuffer[ActorRef]()

  val clusterWarmPool = mutable.Map[MesosContainerProxy, (WarmedData, AtomicLong)]()
  val stats = mutable.Map[MesosContainerProxy, mutable.Map[ActorRef, Long]]()

  prewarmConfig.foreach { config =>
    logging.info(this, s"pre-warming ${config.count} ${config.exec.kind} containers")
    (1 to config.count).foreach { _ =>
      prewarmContainer(config.exec, config.memoryLimit)
    }
  }

  context.system.scheduler.schedule(30.seconds, 30.seconds) {
    self ! RemoveUnused
    self ! MarkUnused
  }



  override def receive: Receive = {
    case GetContainer(client, action, maxConcurrency, knownTaskIds) => {
      val actionKey = action.fullyQualifiedName(true).toString

      //unflag an containers scheduled for removal
      val unflagged = flaggedForRemoval.filter(p => p.data.action == action)
      flaggedForRemoval --=unflagged

      //add this client to the subscribers
      if (!subscribers.contains(client)) {
        subscribers += client
      }

      //schedule a warm or acquire a container
      ContainerManager.schedule(action, maxConcurrency, None, clusterWarmPool.toMap, knownTaskIds) match {
        case Some(p) =>
          //found a warm container with capacity
          client ! ClientNeedWork(p._1)
        case None =>
          //take a prewarm or create a container
          val container = takePrewarmContainer(action)
          container match {
            case Some(c) => logging.warn(this, s"using prewarm container ${c._2.container.taskId} for ${action.fullyQualifiedName(true)}")
            case None => //nothing
          }
          container
            .orElse {
              logging.warn(this, s"requesting new container for ${action.fullyQualifiedName(true)}")
              Some(createContainer(action))
            }
          container match {
            case Some((actorRef, data)) =>
              actorRef ! Init(action)
            case None =>
              logging.error(this, "failed to launch container")
          }
      }





    }

    case NeedWork(data, action) => {
      val proxy = new MesosContainerProxy(data, sender())
//      actorContainers.put(sender(), proxy)
      logging.info(this, s"NeedWork for container ${proxy.data.container.taskId}")

      clusterWarmPool.put(proxy, (data, new AtomicLong(0)))
      subscribers.foreach( s => s ! ClientNeedWork(proxy))

    }

    case ReportUsage(client:ActorRef, proxy, currentActivations) => {
      require(currentActivations >= 0, s"usage reported cannot be less than 0: ${currentActivations}")
      logging.info(this, s"received usage data for ${proxy.data.action.fullyQualifiedName(true)} with ${currentActivations} activations")
      stats.getOrElseUpdate(proxy, mutable.Map()).update(client, currentActivations)

      stats.foreach(p => {
        p._2.foreach(a => {
          logging.info(this, s"stats for ${p._1.data.action.fullyQualifiedName(true)} are ${a}")
        })
      })
      //in case this is marked for removal, unmark it
      if (currentActivations > 0 && flaggedForRemoval.contains(proxy)){
        flaggedForRemoval -= proxy
      }
    }

    case NeedInitWork(data) => {
      logging.info(this, "prewarm container added to prewarmedPool")
      prewarmedPool.update(sender(), data)
    }

    case ContainerRemoved => {
      logging.info(this, "container removed...")
      //TODO: remove via predicate?
      clusterWarmPool.foreach (p => {
        if (p._1.actor == sender()){
          clusterWarmPool.remove(p._1)
        }
      })

//      actorContainers.get(sender()) match {
//        case Some(proxy) => clusterWarmPool.remove(proxy)
//        case None => logging.warn(this, "removing container that was not in warmPool...")
//      }

    }

    case MarkUnused => {
      stats.foreach(p => {
        //total for all actors
        val proxyUsageTotal = p._2.foldLeft(0l)((sum, next) => sum + next._2)
        logging.info(this, s"MarkUnused: usage for ${p._1.data.action.fullyQualifiedName(true)} is ${proxyUsageTotal}")
        if (proxyUsageTotal == 0 && !flaggedForRemoval.contains(p._1)){
          logging.info(this, s"marking ${p._1.taskId} for removal")
          flaggedForRemoval += p._1
          clusterWarmPool.remove(p._1)
          subscribers.foreach( s => s ! ClientNotifyRemoval(p._1))
        }
      })
    }

    case RemoveUnused => {
      flaggedForRemoval.foreach(p => {
        val proxyUsageTotal = stats(p).foldLeft(0l)((sum, next) => sum + next._2)
        logging.info(this, s"RemoveUnused: usage for ${p.data.action.fullyQualifiedName(true)} is ${proxyUsageTotal}")
        if (proxyUsageTotal == 0){
          logging.warn(this, s"requesting removal of ${p.taskId}")
          p.actor ! Remove
        } else {
          logging.info(this, s"found active usage of ${p.taskId}, won't remove for now.")
        }
        flaggedForRemoval -= p
        stats.remove(p)
      })
    }
    case msg => logging.info(this, s"unknown msg ${msg}")
  }

  /** Creates a new container and updates state accordingly. */
  def createContainer(action:ExecutableWhiskAction): (ActorRef, ContainerData) = {
    val ref = childFactory(context)
    val data = NoData()
    (ref, data)
  }

  /** Creates a new prewarmed container */
  def prewarmContainer(exec: CodeExec[_], memoryLimit: ByteSize):Unit = {
    childFactory(context) ! Start(exec, memoryLimit)
  }

  def takePrewarmContainer(action: ExecutableWhiskAction): Option[ (ActorRef, PreWarmedData)] =
    prewarmConfig.flatMap { config =>
      val kind = action.exec.kind
      val memory = action.limits.memory.megabytes.MB
      prewarmedPool.find {
        case (_, PreWarmedData(_, `kind`, `memory`, None)) => true
        case _                                       => false
      }.map {
        case (ref, data) =>
          // Move the container to the usual pool
          //freePool.update(ref, data)
          prewarmedPool.remove(ref)
          // Create a new prewarm container
          prewarmContainer(config.exec, config.memoryLimit)

          (ref, data)
      }
    }


  //TODO: launch enough actions to support the configured request density for this action
  def launchContainersForAction(action: ExecutableWhiskAction, maxConcurrency:Int, invocationNamespace: Option[EntityName], idles: Map[MesosContainerProxy, ContainerData], promise:Promise[Option[(MesosContainerProxy, ContainerData)]]): Option[(ActorRef, ContainerData)] ={
    val allActive = idles.find {
      case (_, WarmedData(_, `action`, _)) => true
      case _ => false

    }
    val totalActiveActivations = stats.find(p => p._1.data.action == action) match {
      case Some(p) => p._2.foldLeft(0l)((sum, next) => sum + next._2)
      case None => 0
    }


    val totalContainers = allActive.size

    //add 1 to total activations to avoid startup case with 0 activations in-flight
    val requiredNew = math.ceil((totalActiveActivations + 1.0 / maxConcurrency) - totalContainers).toInt
    logging.info(this, s"launching ${requiredNew} containers for action ${action.fullyQualifiedName(true)}")

    if (requiredNew > 0){

    }
    val launched = (1 to requiredNew).map(_ => {
      takePrewarmContainer(action)
        .orElse {
          logging.info(this, "no prewarm, creating a new container...")
          Some(createContainer(action))
        }

    })
    launched.head
  }

}

object ContainerManager {

  def props(factory: (ActorRefFactory) => ActorRef,
            prewarmConfig: Option[PrewarmingConfig] = None,
            unusedTimeout: FiniteDuration = 10.minutes,
            pauseGrace: FiniteDuration = 50.milliseconds) = Props(new ContainerManager(factory, prewarmConfig))

  def schedule(action: ExecutableWhiskAction, maxConcurrency:Int, invocationNamespace: Option[EntityName], idles: Map[MesosContainerProxy, (WarmedData, AtomicLong)], knownTaskIds:Seq[String])(implicit logging:Logging): Option[(MesosContainerProxy, (WarmedData, AtomicLong))] = {
    idles.find {
      case (p, (WarmedData(_, `action`, _), currentActivations) )
        if !knownTaskIds.contains(p.data.container.taskId) && currentActivations.get() <= maxConcurrency => {
        true
      }
      case _ => {
        false
      }
    }
  }

}
