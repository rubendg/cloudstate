/*
 * Copyright 2019 Lightbend Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.lightbend.statefulserverless

import akka.actor.{Actor, ActorLogging, DeadLetterSuppression, Props, Timers}
import com.lightbend.statefulserverless.StatsCollector.StatsCollectorSettings
import com.typesafe.config.Config
import io.prometheus.client.Gauge

import scala.collection.mutable
import scala.concurrent.duration._

/**
  * Collects stats for actions executed.
  *
  * This actor attempts to replicate the stats collection logic in https://github.com/knative/serving/blob/master/pkg/queue/stats.go.
  * That logic records the amount of time spent at each concurrency level, and uses that to periodically report
  * the weighted average concurrency.
  */
object StatsCollector {

  def props(settings: StatsCollectorSettings): Props = Props(new StatsCollector(settings))

  case class RequestReceived(proxied: Boolean)

  val NormalRequestReceived = RequestReceived(false)
  val ProxiedRequestReceived = RequestReceived(true)

  case class ResponseSent(proxied: Boolean, timeNanos: Long, grpcTimeNanos: Long)

  /**
    * A command has been sent to the user function.
    */
  case class CommandSent private (proxied: Boolean)

  val NormalCommandSent = CommandSent(false)
  val ProxiedCommandSent = CommandSent(true)

  /**
    * A reply has been received from the user function.
    */
  case class ReplyReceived private (proxied: Boolean, timeNanos: Long)

  case object DatabaseOperationStarted
  case class DatabaseOperationFinished(timeNanos: Long)

  case class StatsCollectorSettings(
    namespace: String,
    configName: String,
    revisionName: String,
    podName: String,
    reportPeriod: FiniteDuration
  ) {
    def this(config: Config) = this(
      namespace = config.getString("namespace"),
      configName = config.getString("config-name"),
      revisionName = config.getString("revision-name"),
      podName = config.getString("pod-name"),
      reportPeriod = config.getDuration("report-period").toMillis.millis
    )
  }

  private case object Tick extends DeadLetterSuppression

  // 1 second in nano seconds
  private val SecondInNanos: Long = 1000000000

  private val DestinationNsLabel = "destination_namespace"
  private val DestinationConfigLabel = "destination_configuration"
  private val DestinationRevLabel    = "destination_revision"
  private val DestinationPodLabel    = "destination_pod"

  private val labels = Array(
    DestinationNsLabel, DestinationConfigLabel, DestinationRevLabel, DestinationPodLabel
  )

  private object gauges {
    // These go here rather than in the actor because if the Actor happens to be instantiated twice, these will
    // be registered twice and an exception will be thrown.
    val OperationsPerSecond = Gauge.build("queue_operations_per_second",
      "Number of operations per second").labelNames(labels: _*).register()
    val ProxiedOperationsPerSecond = Gauge.build("queue_proxied_operations_per_second",
      "Number of proxied operations per second").labelNames(labels: _*).register()
    val AverageConcurrentRequests = Gauge.build("queue_average_concurrent_requests",
      "Number of requests currently being handled by this pod").labelNames(labels: _*).register()
    val AverageProxiedConcurrentRequests = Gauge.build("queue_average_proxied_concurrent_requests",
      "Number of proxied requests currently being handled by this pod").labelNames(labels: _*).register()
  }
}

class StatsCollector(settings: StatsCollectorSettings) extends Actor with Timers with ActorLogging {

  import StatsCollector._

  // These need to be in the same order as the labels registered
  private val labelValues = {
    val map = Map(
      DestinationNsLabel -> settings.namespace,
      DestinationConfigLabel -> settings.configName,
      DestinationRevLabel -> settings.revisionName,
      DestinationPodLabel -> settings.podName
    )
    labels.map(map.apply)
  }

  private val operationsPerSecondGauge = gauges.OperationsPerSecond.labels(labelValues: _*)
  private val proxiedOperationsPerSecondGauge = gauges.ProxiedOperationsPerSecond.labels(labelValues:_ *)
  private val averageConcurrentRequestsGauge = gauges.AverageConcurrentRequests.labels(labelValues:_ *)
  private val averageProxiedConcurrentRequestsGauge = gauges.AverageProxiedConcurrentRequests.labels(labelValues: _*)


  private var commandCount: Int = 0
  private var proxiedCommandCount: Int = 0
  private var commandTimeNanos: Long = 0
  private var proxiedCommandTimeNanos: Long = 0
  private var commandConcurrency: Int = 0
  private var proxiedCommandConcurrency: Int = 0
  private val commandTimeNanosOnConcurrency: mutable.Map[Int, Long] = new mutable.HashMap().withDefaultValue(0)
  private val commandTimeNanosOnProxiedConcurrency: mutable.Map[Int, Long] = new mutable.HashMap().withDefaultValue(0)
  private var commandLastChangedNanos: Long = System.nanoTime()

  private var requestCount: Int = 0
  private var proxiedRequestCount: Int = 0
  private var requestTimeNanos: Long = 0
  private var proxiedRequestTimeNanos: Long = 0
  private var requestGrpcTimeNanos: Long = 0
  private var requestConcurrency: Int = 0
  private var proxiedRequestConcurrency: Int = 0
  private val requestTimeNanosOnConcurrency: mutable.Map[Int, Long] = new mutable.HashMap().withDefaultValue(0)
  private val requestTimeNanosOnProxiedConcurrency: mutable.Map[Int, Long] = new mutable.HashMap().withDefaultValue(0)
  private var requestLastChangedNanos: Long = System.nanoTime()

  private var databaseCount: Int = 0
  private var databaseTimeNanos: Long = 0
  private var databaseConcurrency: Int = 0
  private val databaseTimeNanosOnConcurrency: mutable.Map[Int, Long] = new mutable.HashMap().withDefaultValue(0)
  private var databaseLastChangedNanos: Long = System.nanoTime()

  private var lastReportedNanos = System.nanoTime()

  // Report every second - this
  timers.startPeriodicTimer("tick", Tick, settings.reportPeriod)

  private def updateCommandState(): Unit = {
    val currentNanos = System.nanoTime()
    val sinceLastNanos = currentNanos - commandLastChangedNanos
    commandTimeNanosOnConcurrency.update(commandConcurrency, commandTimeNanosOnConcurrency(commandConcurrency) + sinceLastNanos)
    commandTimeNanosOnProxiedConcurrency.update(proxiedCommandConcurrency, commandTimeNanosOnProxiedConcurrency(proxiedCommandConcurrency) + sinceLastNanos)
    commandLastChangedNanos = currentNanos
  }

  private def updateRequestState(): Unit = {
    val currentNanos = System.nanoTime()
    val sinceLastNanos = currentNanos - requestLastChangedNanos
    requestTimeNanosOnConcurrency.update(requestConcurrency, requestTimeNanosOnConcurrency(requestConcurrency) + sinceLastNanos)
    requestTimeNanosOnProxiedConcurrency.update(proxiedRequestConcurrency, requestTimeNanosOnProxiedConcurrency(proxiedRequestConcurrency) + sinceLastNanos)
    requestLastChangedNanos = currentNanos
  }

  private def updateDatabaseState(): Unit = {
    val currentNanos = System.nanoTime()
    val sinceLastNanos = currentNanos - databaseLastChangedNanos
    databaseTimeNanosOnConcurrency.update(databaseConcurrency, databaseTimeNanosOnConcurrency(databaseConcurrency) + sinceLastNanos)
    databaseLastChangedNanos = currentNanos
  }


  private def weightedAverage(times: mutable.Map[Int, Long]): Double = {

    // This replicates this go code:
    //	var totalTimeUsed time.Duration
    //	for _, val := range times {
    //		totalTimeUsed += val
    //	}
    // 	avg := 0.0
    //	if totalTimeUsed > 0 {
    //		sum := 0.0
    //		for c, val := range times {
    //			sum += float64(c) * val.Seconds()
    //		}
    //		avg = sum / totalTimeUsed.Seconds()
    //	}
    //	return avg
    def toSeconds(value: Long): Double = value.asInstanceOf[Double] / SecondInNanos

    val totalTimeUsed = times.values.sum
    val average = if (totalTimeUsed > 0) {
      times.map {
        case (c, value) => c * toSeconds(value)
      }.sum / toSeconds(totalTimeUsed)
    } else 0.0

    math.max(average, 0.0)
  }

  override def receive: Receive = {
    case CommandSent(proxied) =>
      updateCommandState()
      commandCount += 1
      commandConcurrency += 1
      if (proxied) {
        proxiedCommandCount += 1
        proxiedCommandConcurrency += 1
      }

    case ReplyReceived(proxied, timeNanos) =>
      updateCommandState()
      commandConcurrency -= 1
      commandTimeNanos += timeNanos
      if (proxied) {
        proxiedCommandConcurrency -= 1
        proxiedCommandTimeNanos += timeNanos
      }

    case RequestReceived(proxied) =>
      updateRequestState()
      requestCount += 1
      requestConcurrency += 1
      if (proxied) {
        proxiedRequestCount += 1
        proxiedRequestConcurrency += 1
      }

    case ResponseSent(proxied, timeNanos, grpcTimeNanos) =>
      updateRequestState()
      requestConcurrency -= 1
      requestTimeNanos += timeNanos
      requestGrpcTimeNanos += grpcTimeNanos
      if (proxied) {
        proxiedRequestConcurrency -= 1
        proxiedRequestTimeNanos += timeNanos
      }

    case DatabaseOperationStarted =>
      updateDatabaseState()
      databaseCount += 1
      databaseConcurrency += 1

    case DatabaseOperationFinished(timeNanos) =>
      updateDatabaseState()
      databaseTimeNanos += timeNanos
      databaseConcurrency -= 1

    case Tick =>
      val currentTime = System.nanoTime()
      updateCommandState()
      val reportPeriodNanos = Math.max(currentTime - lastReportedNanos, 1).asInstanceOf[Double]
      val reportPeriodSeconds =  reportPeriodNanos / SecondInNanos

      val avgCommandConcurrency = weightedAverage(commandTimeNanosOnConcurrency)
      //val avgProxiedCommandConcurrency = weightedAverage(commandTimeNanosOnProxiedConcurrency)
      val avgRequestConcurrency = weightedAverage(requestTimeNanosOnConcurrency)
      val avgProxiedRequestConcurrency = weightedAverage(requestTimeNanosOnProxiedConcurrency)
      val avgDatabaseConcurrency = weightedAverage(databaseTimeNanosOnConcurrency)

      val avgRequestTime = if (requestCount > 0) requestTimeNanos / requestCount / 1000000 else 0
      val avgGrpcTime = if (requestCount > 0) requestGrpcTimeNanos / requestCount / 1000000 else 0
      val avgDatabaseTime = if (databaseCount > 0) databaseTimeNanos / databaseCount / 1000000 else 0
      val avgCommandTime = if (commandCount > 0) commandTimeNanos / commandCount / 1000000 else 0

      val entityCount = Serve.entityExtractCount.getAndSet(0)
      val entityTime = Serve.entityExtractTime.getAndSet(0)
      val avgEntity = if (entityCount > 0) entityTime / entityCount / 1000000 else 0

      log.debug("R:%4d  RC:%6.2f  RT:%5dms  GT:%5dms  D:%4d  DC:%6.2f  DT:%5dms  C:%4d  CC:%6.2f  CT:%5dms   E:%4d  ET:%5dms".format(
        requestCount,
        avgRequestConcurrency,
        avgRequestTime,
        avgGrpcTime,

        databaseCount,
        avgDatabaseConcurrency,
        avgDatabaseTime,

        commandCount,
        avgCommandConcurrency,
        avgCommandTime,

        entityCount,
        avgEntity
      ))

      operationsPerSecondGauge.set(requestCount.asInstanceOf[Double] / reportPeriodSeconds)
      proxiedOperationsPerSecondGauge.set(proxiedRequestCount / reportPeriodSeconds)
      averageConcurrentRequestsGauge.set(avgRequestConcurrency - avgDatabaseConcurrency)
      averageProxiedConcurrentRequestsGauge.set(avgProxiedRequestConcurrency)

      lastReportedNanos = currentTime
      commandCount = 0
      proxiedCommandCount = 0
      commandTimeNanosOnConcurrency.clear()
      commandTimeNanosOnProxiedConcurrency.clear()
      commandTimeNanos = 0
      proxiedCommandTimeNanos = 0
      requestCount = 0
      proxiedRequestCount = 0
      requestTimeNanosOnConcurrency.clear()
      requestTimeNanosOnProxiedConcurrency.clear()
      requestTimeNanos = 0
      proxiedRequestTimeNanos = 0
      requestGrpcTimeNanos = 0
      databaseCount = 0
      databaseTimeNanosOnConcurrency.clear()
      databaseTimeNanos = 0
  }
}
