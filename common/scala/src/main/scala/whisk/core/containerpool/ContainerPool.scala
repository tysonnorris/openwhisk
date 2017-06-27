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

import java.util.UUID

import akka.actor.{Actor, ActorRef, ActorRefFactory, Props}
import whisk.common.AkkaLogging
import whisk.core.dispatcher.ActivationFeed.ContainerReleased
import whisk.core.entity.{ByteSize, CodeExec, EntityName, ExecutableWhiskAction}
import whisk.core.entity.size._
import whisk.core.mesos.{MesosClientActor, MesosTask, Subscribe}

import scala.collection.mutable

sealed trait WorkerState
case object Busy extends WorkerState
case object Free extends WorkerState

case class WorkerData(data: ContainerData, state: WorkerState)

/**
 * A pool managing containers to run actions on.
 *
 * This pool fulfills the other half of the ContainerProxy contract. Only
 * one job (either Start or Run) is sent to a child-actor at any given
 * time. The pool then waits for a response of that container, indicating
 * the container is done with the job. Only then will the pool send another
 * request to that container.
 *
 * Upon actor creation, the pool will start to prewarm containers according
 * to the provided prewarmConfig, iff set. Those containers will **not** be
 * part of the poolsize calculation, which is capped by the poolSize parameter.
 * Prewarm containers are only used, if they have matching arguments
 * (kind, memory) and there is space in the pool.
 *
 * @param childFactory method to create new container proxy actor
 * @param maxActiveContainers maximum amount of containers doing work
 * @param maxPoolSize maximum size of containers allowed in the pool
 * @param feed actor to request more work from
 * @param prewarmConfig optional settings for container prewarming
 */
class ContainerPool(
    childFactory: ActorRefFactory => ActorRef,
    maxActiveContainers: Int,
    maxPoolSize: Int,
    feed: ActorRef,
    prewarmConfig: Option[PrewarmingConfig] = None) extends Actor {
    implicit val logging = new AkkaLogging(context.system.log)

    val freePool = mutable.Map[ActorRef, ContainerData]()
    val busyPool = mutable.Map[ActorRef, ContainerData]()
    val prewarmedPool = mutable.Map[ActorRef, ContainerData]()

    //init mesos framework:
    val mesosClientActor = context.system.actorOf(MesosClientActor.props(
        "whisk-invoker-"+UUID.randomUUID(),
        "whisk-framework",
        "http://localhost:5050",
        "*",
        taskBuilder = MesosTask.buildTask
    ))

    mesosClientActor ! Subscribe
    //TODO: verify subscribed status



    prewarmConfig.foreach { config =>
        logging.info(this, s"pre-warming ${config.count} ${config.exec.kind} containers")
        (1 to config.count).foreach { _ =>
            prewarmContainer(config.exec, config.memoryLimit)
        }
    }

    def receive: Receive = {
        // A job to run on a container
        case r: Run =>
            runNow(r)

        // Container is free to take more work
        case NeedWork(data: WarmedData) =>
            logging.info(this, "need work with warmedData...")
            freePool.update(sender(), data)
            busyPool.remove(sender())

        // Container is prewarmed and ready to take work
        case NeedWork(data: PreWarmedData) =>
            prewarmedPool.update(sender(), data)

        // Container got removed
        case ContainerRemoved =>
            freePool.remove(sender())
            busyPool.remove(sender())

        // Activation completed
        case ActivationCompleted =>
            feed ! ContainerReleased
    }

    def runNow(r:Run): Unit ={
            val container = if (busyPool.size < maxActiveContainers) {
                logging.info(this, s"room in pool ${freePool.size}, will schedule")
                // Schedule a job to a warm container


                ContainerPool.schedule(r.action, r.msg.user.namespace, freePool.toMap).orElse {
                    logging.info(this, "did not schedule, will try for prewarm or create...")
                    if (busyPool.size + freePool.size < maxPoolSize) {
                        logging.info(this, "will try to use a prewarm...")
                        takePrewarmContainer(r.action).orElse {
                            logging.info(this, "will create new container...")
                            Some(createContainer())
                        }
                    } else None
                }.orElse {
                    // Remove a container and create a new one for the given job
                    ContainerPool.remove(r.action, r.msg.user.namespace, freePool.toMap).map { toDelete =>
                        removeContainer(toDelete)
                        takePrewarmContainer(r.action).getOrElse {
                            createContainer()
                        }
                    }
                }
            } else None

            container match {
                case Some((actor, data)) =>
                    busyPool.update(actor, data)
                    //freePool.remove(actor)
                    logging.info(this, s"submitting msg ${r.msg.activationId} to actor ${actor}")
                    actor ! r // forwards the run request to the container
                case None =>
                    self ! r
            }

    }

    /** Creates a new container and updates state accordingly. */
    def createContainer(): (ActorRef, ContainerData) = {
        val ref = childFactory(context)
        val data = NoData()
        freePool.update(ref, data)

        (ref, data)
    }

    /** Creates a new prewarmed container */
    def prewarmContainer(exec: CodeExec[_], memoryLimit: ByteSize) =
        childFactory(context) ! Start(exec, memoryLimit)

    /**
     * Takes a prewarm container out of the prewarmed pool
     * iff a container with a matching kind is found.
     *
     * @param kind the kind you want to invoke
     * @return the container iff found
     */
    def takePrewarmContainer(action: ExecutableWhiskAction): Option[(ActorRef, ContainerData)] =
        prewarmConfig.flatMap { config =>
            val kind = action.exec.kind
            val memory = action.limits.memory.megabytes.MB
            prewarmedPool.find {
                case (_, PreWarmedData(_, `kind`, `memory`)) => true
                case _                                       => false
            }.map {
                case (ref, data) =>
                    // Move the container to the usual pool
                    freePool.update(ref, data)
                    prewarmedPool.remove(ref)
                    // Create a new prewarm container
                    prewarmContainer(config.exec, config.memoryLimit)

                    (ref, data)
            }
        }

    /** Removes a container and updates state accordingly. */
    def removeContainer(toDelete: ActorRef) = {
        toDelete ! Remove
        freePool.remove(toDelete)
        busyPool.remove(toDelete)
    }
}

object ContainerPool {
    /**
     * Finds the best container for a given job to run on.
     *
     * Selects an arbitrary warm container from the passed pool of idle containers
     * that matches the action and the invocation namespace. The implementation uses
     * matching such that structural equality of action and the invocation namespace
     * is required.
     * Returns None iff no matching container is in the idle pool.
     * Does not consider pre-warmed containers.
     *
     * @param action the action to run
     * @param invocationNamespace the namespace, that wants to run the action
     * @param idles a map of idle containers, awaiting work
     * @return a container if one found
     */
    def schedule[A](action: ExecutableWhiskAction, invocationNamespace: EntityName, idles: Map[A, ContainerData]): Option[(A, ContainerData)] = {
        idles.find {
            case (_, WarmedData(_, `invocationNamespace`, `action`, _)) => true
            case _ => false
        }
    }

    /**
     * Finds the best container to remove to make space for the job passed to run.
     *
     * Determines the least recently used Free container in the pool.
     *
     * @param action the action that wants to get a container
     * @param invocationNamespace the namespace, that wants to run the action
     * @param pool a map of all free containers in the pool
     * @return a container to be removed iff found
     */
    def remove[A](action: ExecutableWhiskAction, invocationNamespace: EntityName, pool: Map[A, ContainerData]): Option[A] = {
        // Try to find a Free container that is initialized with any OTHER action
        val freeContainers = pool.collect {
            case (ref, w: WarmedData) if (w.action != action || w.invocationNamespace != invocationNamespace) => ref -> w
        }

        if (freeContainers.nonEmpty) {
            val (ref, _) = freeContainers.minBy(_._2.lastUsed)
            Some(ref)
        } else None
    }

    def props(factory: ActorRefFactory => ActorRef,
              maxActive: Int,
              size: Int,
              feed: ActorRef,
              prewarmConfig: Option[PrewarmingConfig] = None) = Props(new ContainerPool(factory, maxActive, size, feed, prewarmConfig))
}

/** Contains settings needed to perform container prewarming */
case class PrewarmingConfig(count: Int, exec: CodeExec[_], memoryLimit: ByteSize)
