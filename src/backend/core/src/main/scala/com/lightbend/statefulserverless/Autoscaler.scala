package com.lightbend.statefulserverless

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Timers}
import akka.cluster.{Cluster, MemberStatus, UniqueAddress}
import akka.cluster.ddata.Replicator.{Get, GetFailure, GetSuccess, NotFound, ReadLocal, ReadMajority, Update, UpdateSuccess, WriteLocal}
import akka.cluster.ddata.{DistributedData, LWWRegister, LWWRegisterKey}

import scala.collection.immutable.Queue
import scala.concurrent.duration._

case class AutoscalerSettings(

  /**
    * Target concurrency on user functions.
    */
  targetUserFunctionConcurrency: Int,

  /**
    * Target concurrency for requests.
    */
  targetRequestConcurrency: Int,

  /**
    * The sliding window period used to calculate the current concurrency for all nodes.
    */
  targetConcurrencyWindow: FiniteDuration,

  /**
    * While scaling up, concurrency can spike due to the time it takes for new nodes to join the cluster,
    * to warm up/jit, and rebalance etc. So we don't make any new decisions based on concurrency until this
    * deadline is met.
    */
  scaleUpStableDeadline: FiniteDuration,

  /**
    * While scaling down, concurrency can spike due to rebalancing, so we don't make any new decisions based
    * on concurrency until this deadline is met.
    */
  scaleDownStableDeadline: FiniteDuration,

  /**
    * During the scaling up/down stable deadline period, decisions to scale up further are made based on the
    * request rate. If it increases by more than this factor, then we scale the number of nodes to handle that
    * rate based on the original rate per node being handled before scale up/down.
    */
  requestRateThresholdFactor: Double,

  /**
    * The window used to determine whether the request rate has exceeded the threshold.
    */
  requestRateThresholdWindow: FiniteDuration,

  maxScaleFactor: Double,

  maxScaleAbsolute: Int,

  /**
    * Autoscale tick period.
    */
  tickPeriod: FiniteDuration
)

object Autoscaler {

  case class Sample(
    receivedNanos: Long,
    metrics: Metrics
  )

  case class Metrics(
    address: UniqueAddress,
    metricIntervalNanos: Long,
    requestConcurrency: Double,
    databaseConcurrency: Double,
    userFunctionConcurrency: Double,
    requestRate: Double
  )

  private case class Summary(
    clusterMembers: Int,
    requestConcurrency: Double,
    databaseConcurrency: Double,
    userFunctionConcurrency: Double,
    requestRate: Double
  )

  case class Deployment(ready: Int, scale: Int, upgrading: Boolean)
  case class Scale(scale: Int)

  case object Tick

  sealed trait State
  case object WaitingForState extends State
  case object Stable extends State
  case class Scaling(desired: Int, lastStableRequestRatePerNode: Double, wallClockDeadlineMillis: Long) extends State
  case class Upgrading(desired: Int, lastStableRequestRatePerNode: Double) extends State

}

class Autoscaler(settings: AutoscalerSettings, scalerProps: Props) extends Actor with Timers with ActorLogging {

  import Autoscaler._

  private val cluster = Cluster(context.system)
  private val scaler = context.actorOf(scalerProps, "scaler")

  private val ddata = DistributedData(context.system)
  import ddata.selfUniqueAddress
  private val StateKey = LWWRegisterKey[State]("autoscaler")
  private val EmptyState = LWWRegister.create[State](WaitingForState)

  private var stats = Map.empty[UniqueAddress, Queue[Sample]].withDefaultValue(Queue.empty)
  private var deployment: Option[Deployment] = None

  timers.startPeriodicTimer("tick", Tick, settings.tickPeriod)
  ddata.replicator ! Get(StateKey, ReadMajority(timeout = 5.seconds))

  private var reportHeaders = 0

  private def become(handler: Receive): Unit = {
    context become (
      handleMetrics orElse handler orElse {
        case UpdateSuccess(_, _) =>
          // Ignore
      }
    )
  }

  /**
    * This is the initial state, where we wait for every member that is up or weakly up to report its stats, along with
    * the status of the Kubernetes resources, and also querying the current autoscaler CRDT state.
    */
  private def becomeWaitingForState(state: State): Unit = {
    context become (waitingForState(state) orElse handleMetrics)
  }

  private def waitingForState(state: State): Receive = {
    // Results of ddata state read
    case NotFound(StateKey, _) =>
      becomeWaitingForState(Stable)
    case GetFailure(StateKey, _) =>
      // We failed to read from a majority, fall back to local
      ddata.replicator ! Get(StateKey, ReadLocal)
    case success @ GetSuccess(StateKey, _) =>
      // We still need to wait for all the nodes in the cluster to report stats
      becomeWaitingForState(success.get(StateKey).value)

    case Tick =>

      deployment match {
        case None =>
        case Some(deploy) =>
          val haveMetricsFromAllNodes = cluster.state.members.forall { member =>
            // If they aren't up or weakly up, ignore, otherwise ensure we have stats for them
            (member.status != MemberStatus.Up && member.status != MemberStatus.WeaklyUp) || stats.contains(member.uniqueAddress)
          }

          if (haveMetricsFromAllNodes) {
            state match {
              case WaitingForState =>
              // Do nothing, we don't have our own state yet

              case Stable =>
                if (deploy.upgrading) {
                  becomeUpgrading(deploy.scale, summarize().requestRate)
                } else {
                  become(stable)
                }

              case Scaling(desired, lastStableRequestRatePerNode, wallClockDeadline) =>
                become(scaling(desired, lastStableRequestRatePerNode,
                  Deadline.now + (wallClockDeadline - System.currentTimeMillis()).millis))

              case Upgrading(desired, lastStableRequestRatePerNode) =>
                if (deploy.upgrading) {
                  become(upgrading(desired, lastStableRequestRatePerNode))
                } else {
                  becomeStable()
                }

            }
          }
      }
  }

  /**
    * In this state, we are stable, the number of nodes is appropriate to keep the target concurrency
    */
  private def becomeStable(): Unit = {
    updateState(Stable)
    become(stable)
  }

  private def stable: Receive = {
    case Tick =>
      val summary = summarize()

      val adjustedRequestConcurrency = summary.requestConcurrency - summary.databaseConcurrency
      val desiredForUserFunction = Math.min(1, Math.ceil((summary.userFunctionConcurrency * summary.clusterMembers) / settings.targetUserFunctionConcurrency).toInt)
      val desiredForRequest = Math.min(1, Math.ceil((adjustedRequestConcurrency * summary.clusterMembers) / settings.targetRequestConcurrency).toInt)

      if (summary.userFunctionConcurrency > settings.targetUserFunctionConcurrency) {
        val desired = capScaling(desiredForUserFunction, summary.clusterMembers)

        log.debug("Scaling up from {} to {} because user function concurrency {} exceeds target {}",
          summary.clusterMembers, desired, summary.userFunctionConcurrency, settings.targetUserFunctionConcurrency)

        scaleUp(desired, summary.requestRate)

      } else if (adjustedRequestConcurrency > settings.targetRequestConcurrency) {
        val desired = capScaling(desiredForRequest, summary.clusterMembers)

        log.debug("Scaling up from {} to {} because adjusted request concurrency {} exceeds target {}",
          summary.clusterMembers, desired, adjustedRequestConcurrency, settings.targetRequestConcurrency)

        scaleUp(desired, summary.requestRate)

      } else if (desiredForUserFunction < summary.clusterMembers && desiredForRequest < summary.clusterMembers) {
        val desired = capScaling(Math.max(desiredForRequest, desiredForUserFunction), summary.clusterMembers)

        log.debug("Scaling down to {} because desired nodes for user function {} and desired nodes for request handling {} is below cluster members",
          desired, desiredForUserFunction, desiredForRequest, summary.clusterMembers)

        // Adjust request rate to match the desired nodes
        val newRequestRatePerNode = summary.requestRate * summary.clusterMembers / desired

        scaleDown(desired, newRequestRatePerNode)
      }

    case deploy: Deployment =>
      deployment = Some(deploy)
      if (deploy.upgrading) {
        becomeUpgrading(deploy.scale, summarize().requestRate)
      }
  }

  /**
    * Cap scaling to the max scaling settings
    */
  private def capScaling(desired: Int, clusterMembers: Int): Int = {
    val changeInClusterMembers = Math.abs(desired - clusterMembers)
    val factorCappedChange = if (settings.maxScaleFactor > 0) {
      Math.min(changeInClusterMembers, Math.max(1, clusterMembers * settings.maxScaleFactor).toInt)
    } else changeInClusterMembers
    val cappedChanged = if (settings.maxScaleAbsolute > 0) {
      Math.min(factorCappedChange, settings.maxScaleAbsolute)
    } else factorCappedChange
    if (desired > clusterMembers) clusterMembers + cappedChanged
    else clusterMembers - cappedChanged
  }

  /**
    * In this state, we are scaling up, and waiting for the scale up deadline to elapse before switching back to stable, or
    * scaling up further. In addition, if observed request rate exceeds the last stable request rate by the request rate
    * threshold, we may scale up more.
    */
  private def scaleUp(desired: Int, lastStableRequestRatePerNode: Double): Unit = {
    scaler ! Scale(desired)
    updateState(Scaling(desired, lastStableRequestRatePerNode, System.currentTimeMillis() + settings.scaleUpStableDeadline.toMillis))
    become(scaling(desired, lastStableRequestRatePerNode, settings.scaleUpStableDeadline.fromNow))
  }

  private def scaling(desired: Int, lastStableRequestRatePerNode: Double, deadline: Deadline): Receive = {

    case Tick =>

      if (deadline.isOverdue()) {
        log.debug("Scaling to {} stable period over", desired)
        becomeStable()
        stable(Tick)
      } else if (!checkRequestRateScaling(lastStableRequestRatePerNode) && !deployment.exists(_.scale == desired)) {
        scaler ! Scale(desired)
      }

    case deploy: Deployment =>
      deployment = Some(deploy)

  }

  private def checkRequestRateScaling(lastStableRequestRatePerNode: Double) = {
    val summary = summarize()

    if (summary.requestRate > lastStableRequestRatePerNode * settings.requestRateThresholdFactor &&
      (summary.userFunctionConcurrency > settings.targetUserFunctionConcurrency ||
        summary.requestConcurrency > settings.targetRequestConcurrency)) {

      val newDesired = Math.ceil(summary.requestRate * summary.clusterMembers / lastStableRequestRatePerNode).toInt

      log.debug("Scaling up to {} because request rate {} has exceeded last stable request rate {} by configured factor {}",
        newDesired, summary.requestRate, lastStableRequestRatePerNode, settings.requestRateThresholdFactor)
      scaleUp(newDesired, lastStableRequestRatePerNode)
      true
    } else false
  }

  private def scaleDown(desired: Int, lastStableRequestRatePerNode: Double): Unit = {
    scaler ! Scale(desired)
    updateState(Scaling(desired, lastStableRequestRatePerNode, System.currentTimeMillis() + settings.scaleDownStableDeadline.toMillis))
    become(scaling(desired, lastStableRequestRatePerNode, settings.scaleDownStableDeadline.fromNow))
  }

  private def becomeUpgrading(desired: Int, lastStableRequestRatePerNode: Double): Unit = {
    updateState(Upgrading(desired, lastStableRequestRatePerNode))
  }

  private def upgrading(desired: Int, lastStableRequestRatePerNode: Double): Receive = {

  }

  private def handleMetrics: Receive = {
    case m: Metrics =>
      stats = stats.updated(m.address, stats(m.address).enqueue(Sample(System.nanoTime(), m)))
  }

  private def updateState(state: State): Unit = {
    ddata.replicator ! Update(StateKey, EmptyState, WriteLocal)(_.withValueOf(state))
  }

  private def summarize(): Summary = {
    val summaryTime = System.nanoTime()
    val concurrencyWindowNanos = settings.targetConcurrencyWindow.toNanos
    val requestRateWindowNanos = settings.requestRateThresholdWindow.toNanos

    var totalConcurrencyNanos = 0l
    var weightedRequestConcurrencySum = 0d
    var weightedDatabaseConcurrencySum = 0d
    var weightedUserFunctionConcurrencySum = 0d

    var totalRequestRateNanos = 0l
    var weightedRequestRateSum = 0d

    val clusterMembers = cluster.state.members.count(m => m.status == MemberStatus.Up || m.status == MemberStatus.WeaklyUp)

    stats.foreach {
      case (address, samples) =>

        // First, expire old data
        val currentSamples = samples.dropWhile(sample => summaryTime - sample.receivedNanos > concurrencyWindowNanos)
        if (currentSamples.isEmpty) {
          stats -= address
        } else if (currentSamples.size != samples.size) {
          stats += (address -> currentSamples)
        }

        // Now, accumulate sample values
        samples.foreach { sample =>
          val metric = sample.metrics
          val interval = metric.metricIntervalNanos
          totalConcurrencyNanos += interval
          weightedRequestConcurrencySum += interval * metric.requestConcurrency
          weightedDatabaseConcurrencySum += interval * metric.databaseConcurrency
          weightedUserFunctionConcurrencySum += interval * metric.userFunctionConcurrency

          if (summaryTime - sample.receivedNanos < requestRateWindowNanos) {
            totalRequestRateNanos += interval
            weightedRequestRateSum += interval * metric.requestRate
          }
        }

    }

    val summary = if (totalConcurrencyNanos == 0) {
      Summary(clusterMembers, 0, 0, 0, 0)
    } else {
      val requestRate =
        if (totalRequestRateNanos == 0) 0
        else weightedRequestRateSum / totalRequestRateNanos

      Summary(
        clusterMembers = clusterMembers,
        requestConcurrency = weightedRequestConcurrencySum / totalConcurrencyNanos,
        databaseConcurrency = weightedDatabaseConcurrencySum / totalConcurrencyNanos,
        userFunctionConcurrency = weightedUserFunctionConcurrencySum / totalConcurrencyNanos,
        requestRate = requestRate
      )
    }
    logReport(summary)
    summary
  }

  private def logReport(summary: Summary): Unit = {
    if (log.isDebugEnabled) {
      reportHeaders += 1
      if (reportHeaders % 10 == 1) {
        log.debug("%10s %10s %10s %10s %10s %10s %10s".format("Members", "Scale", "Ready", "User Func", "Request", "Database", "Req Rate"))
      }
      log.debug("%10d %10d %10d %10.2f %10.2f %10.2f %10.2f".format(
        summary.clusterMembers,
        deployment.fold(0)(_.scale), deployment.fold(0)(_.ready),
        summary.userFunctionConcurrency, summary.requestConcurrency,
        summary.databaseConcurrency, summary.requestRate))
    }

  }
}
