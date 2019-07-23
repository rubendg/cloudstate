package io.cloudstate.proxy.crdt

import java.net.URLDecoder

import akka.NotUsed
import akka.actor.{Actor, ActorRef, Props, ReceiveTimeout, Stash, Status}
import akka.cluster.Cluster
import akka.cluster.ddata.LWWRegister.Clock
import akka.cluster.ddata.Replicator.{Changed, DataDeleted, Delete, DeleteSuccess, Deleted, Get, GetFailure, GetSuccess, ModifyFailure, NotFound, ReadLocal, ReadMajority, ReplicationDeleteFailure, Subscribe, Unsubscribe, Update, UpdateSuccess, UpdateTimeout, WriteAll, WriteConsistency, WriteLocal, WriteMajority}
import akka.cluster.ddata.{DistributedData, Flag, GCounter, GSet, Key, LWWRegister, ORMap, ORSet, PNCounter, ReplicatedData}
import akka.stream.{Materializer, OverflowStrategy}
import akka.stream.scaladsl.{Sink, Source}
import akka.util.Timeout
import io.cloudstate.crdt.{CrdtClient, CrdtClock, CrdtDelete, CrdtDelta, CrdtInit, CrdtReply, CrdtState, CrdtStreamIn, CrdtStreamOut, FlagDelta, FlagState, GCounterDelta, GCounterState, GSetDelta, GSetState, KeyedEqualityValue, LWWRegisterDelta, LWWRegisterState, ORMapDelta, ORMapEntry, ORMapEntryDelta, ORMapState, ORSetDelta, ORSetState, PNCounterDelta, PNCounterState}
import com.google.protobuf.any.{Any => ProtoAny}
import io.cloudstate.entity.{Command, EntityDiscovery, Failure, UserFunctionError}
import io.cloudstate.proxy.entity.{EntityCommand, UserFunctionReply}

import scala.concurrent.duration.FiniteDuration

object CrdtEntity {
  private final case class Relay(actorRef: ActorRef)
  private final case object StreamClosed
  final case object Stop

  private final case class AnyKey(_id: String) extends Key[ReplicatedData](_id)

  final case class Configuration(
    serviceName: String,
    userFunctionName: String,
    passivationTimeout: Timeout,
    sendQueueSize: Int,
    initialReadTimeout: FiniteDuration,
    writeTimeout: FiniteDuration
  )

  /**
    * This is necessary to provide custom hashCode/equals for the protobuf KeyedEqualityValue class.
    */
  case class KeyedEqualityValueWrapper(value: KeyedEqualityValue) {
    override def hashCode(): Int = value.key.hashCode

    override def equals(obj: Any): Boolean = obj match {
      case KeyedEqualityValueWrapper(wrapped) => wrapped.key == value.key
      case _ => false
    }
  }

  private sealed trait ORMapEntryAction
  private object ORMapEntryAction {
    case object NoAction extends ORMapEntryAction
    case class UpdateEntry(key: KeyedEqualityValue, delta: CrdtDelta) extends ORMapEntryAction
    case class AddEntry(entry: ORMapEntry) extends ORMapEntryAction
    case class DeleteThenAdd(key: KeyedEqualityValue, state: CrdtState) extends ORMapEntryAction
  }

  private sealed trait CrdtChange
  private object CrdtChange {
    case object NoChange extends CrdtChange
    case class Updated(delta: CrdtDelta) extends CrdtChange
    case object IncompatibleChange extends CrdtChange
  }

  private class CustomClock(clockValue: Long, autoIncrement: Boolean) extends Clock[ProtoAny] {
    override def apply(currentTimestamp: Long, value: ProtoAny): Long =
        if (autoIncrement && clockValue <= currentTimestamp) currentTimestamp + 1
        else clockValue
  }

  private case class InitiatorReply(commandId: Long, actorRef: ActorRef, userFunctionReply: UserFunctionReply)

  def props(client: CrdtClient, configuration: CrdtEntity.Configuration, entityDiscovery: EntityDiscovery)(implicit mat: Materializer) =
    Props(new CrdtEntity(client, configuration, entityDiscovery))
}

/**
  * Optimization idea: Rather than try and calculate changes, implement a custom ReplicatedData type that wraps
  * the rest, and whenever update or mergeDelta is called, keep track of the changes in a shared delta tracking
  * object. That object should get set by this actor, and once present, all calls to merge/mergeDelta/update etc
  * will add changes to the delta tracking object.
  */
final class CrdtEntity(client: CrdtClient, configuration: CrdtEntity.Configuration, entityDiscovery: EntityDiscovery)(implicit mat: Materializer) extends Actor with Stash {

  import CrdtEntity._

  private val entityId = URLDecoder.decode(self.path.name, "utf-8")

  private val ddata = DistributedData(context.system)
  import ddata.selfUniqueAddress
  private implicit val cluster: Cluster = Cluster(context.system)
  private val replicator = ddata.replicator
  private val key = AnyKey(configuration.userFunctionName + "-" + entityId)

  private var relay: ActorRef = _
  private var state: Option[ReplicatedData] = _
  private var idCounter = 0l
  private var outstandingOperations = 0
  private var outstanding = Map.empty[Long, ActorRef]
  private var stopping = false

  context.setReceiveTimeout(configuration.passivationTimeout.duration)

  override def preStart(): Unit = {
    client.handle(Source.actorRef[CrdtStreamIn](configuration.sendQueueSize, OverflowStrategy.fail)
      .mapMaterializedValue { ref =>
        self ! Relay(ref)
        NotUsed
      }).runWith(Sink.actorRef(self, StreamClosed))

    // We initially do a read to get the initial state. Try a majority read first in case this is a new node.
    replicator ! Get(key, ReadMajority(configuration.initialReadTimeout))
  }

  override def receive: Receive = {
    case Relay(r) =>
      relay = r
      maybeStart()

    case s @ GetSuccess(_, _) =>
      state = Some(s.dataValue)
      maybeStart()

    case NotFound =>
      state = None
      maybeStart()

    case GetFailure(_, _) =>
      // Retry with local consistency
      replicator ! Get(key, ReadLocal)

    case DataDeleted(_, _) =>
      if (relay != null) {
        sendDelete()
      }
      context become deleted

    case _ =>
      stash()
  }

  private def sendDelete(): Unit = {
    if (relay != null) {
      sendToRelay(CrdtStreamIn.Message.Deleted(CrdtDelete.defaultInstance))
      relay ! Status.Success(())
      relay = null
    }
    replicator ! Unsubscribe(key, self)
  }

  private def toWireState(state: ReplicatedData): CrdtState = {
    import CrdtState.{State => S}

    CrdtState(state match {
      case gcounter: GCounter =>
        S.Gcounter(GCounterState(gcounter.value.toLong))
      case pncounter: PNCounter =>
        S.Pncounter(PNCounterState(pncounter.value.toLong))
      case gset: GSet[KeyedEqualityValueWrapper @unchecked] =>
        S.Gset(GSetState(gset.elements.map(_.value).toSeq))
      case orset: ORSet[KeyedEqualityValueWrapper @unchecked] =>
        S.Orset(ORSetState(orset.elements.map(_.value).toSeq))
      case lwwregister: LWWRegister[ProtoAny @unchecked] =>
        S.Lwwregister(LWWRegisterState(Some(lwwregister.value)))
      case flag: Flag =>
        S.Flag(FlagState(flag.enabled))
      case ormap: ORMap[KeyedEqualityValueWrapper @unchecked, ReplicatedData @unchecked] =>
        S.Ormap(ORMapState(ormap.entries.map {
          case (k, value) => ORMapEntry(Some(k.value), Some(toWireState(value)))
        }.toSeq))
      case _ =>
        // todo handle better
        throw new RuntimeException("Unknown CRDT: " + state)
    })
  }

  private def maybeStart() = {

    if (relay != null && state != null) {
      val wireState = state.map(toWireState)

      sendToRelay(CrdtStreamIn.Message.Init(CrdtInit(
        serviceName = configuration.serviceName,
        entityId = entityId,
        state = wireState
      )))

      context become running
      replicator ! Subscribe(key, self)
      unstashAll()
    }

  }

  private def maybeSendAndUpdateState(data: ReplicatedData): Unit = {
    state match {
      case Some(value) =>
        // Fast path, exclude instance equality
        if (!(data eq value)) {
          detectChange(value, data) match {
            case CrdtChange.NoChange =>
            // Nothing to do
            case CrdtChange.IncompatibleChange =>
              throw new RuntimeException(s"Incompatible CRDT change from $value to ${data}")
            case CrdtChange.Updated(delta) =>
              sendToRelay(CrdtStreamIn.Message.Changed(delta))
          }
        }
      case None =>
        sendToRelay(CrdtStreamIn.Message.State(toWireState(data)))
    }
    state = Some(data)
  }

  private def running: Receive = {

    case c @ Changed(_) if outstandingOperations > 0 =>
      // As long as we have outstanding ops, we ignore any changes, to ensure that we never have simultaneous
      // changes of the actor state and the user function state

    case c @ Changed(_) =>
      maybeSendAndUpdateState(c.dataValue)

    case Deleted(_) =>
      sendDelete()
      state = None
      context become deleted

    case EntityCommand(_, commandName, payload) =>
      idCounter += 1
      outstanding = outstanding.updated(1, sender())
      outstandingOperations += 1
      sendToRelay(CrdtStreamIn.Message.Command(Command(
        entityId = entityId,
        id = idCounter,
        name = commandName,
        payload = payload
      )))

    case CrdtStreamOut(CrdtStreamOut.Message.Reply(reply)) =>
      // todo validate
      val initiator = outstanding(reply.commandId)

      val userFunctionReply = UserFunctionReply(
        sideEffects = reply.sideEffects,
        message = reply.response match {
          case CrdtReply.Response.Empty => UserFunctionReply.Message.Empty
          case CrdtReply.Response.Reply(payload) => UserFunctionReply.Message.Reply(payload)
          case CrdtReply.Response.Forward(forward) => UserFunctionReply.Message.Forward(forward)
        }
      )

      reply.action match {
        case CrdtReply.Action.Empty =>
          initiator ! userFunctionReply
          outstanding -= reply.commandId
          operationFinished()

        case CrdtReply.Action.Delete(_) =>
          replicator ! Delete(key, toDdataWriteConsistency(reply.writeConsistency), Some(InitiatorReply(reply.commandId, initiator, userFunctionReply)))
          state = None
          context become deleted
          replicator ! Unsubscribe(key, self)
          relay ! Status.Success(())
          relay = null

        case CrdtReply.Action.Update(delta) =>
          val (initial, modify) = deltaToUpdate(delta)
          // Apply to our own state first
          state = Some(modify(state.getOrElse(initial)))
          // And then to the replicator
          replicator ! Update(key, initial, toDdataWriteConsistency(reply.writeConsistency),
            Some(InitiatorReply(reply.commandId, initiator, userFunctionReply)))(modify)
      }

    case UpdateSuccess(_, Some(InitiatorReply(commandId, initiator, userFunctionReply))) =>
      initiator ! userFunctionReply
      outstanding -= commandId
      operationFinished()

    case success @ GetSuccess(_, _) =>
      outstandingOperations -= 1
      if (outstandingOperations == 0) {
        maybeSendAndUpdateState(success.dataValue)
      }

    case UpdateTimeout(_, Some(InitiatorReply(commandId, initiator, _))) =>
      outstanding -= commandId
      initiator ! UserFunctionReply(message = UserFunctionReply.Message.Failure(Failure(description = "Failed to update CRDT at requested write consistency")))
      crash("Failed to update CRDT at requested write consistency")

    case ModifyFailure(_, error, cause, Some(InitiatorReply(commandId, initiator, _))) =>
      outstanding -= commandId
      initiator ! UserFunctionReply(message = UserFunctionReply.Message.Failure(Failure(description = "Error updating CRDT")))
      crash("Error updating CRDT: " + error, Some(cause))

    case CrdtStreamOut(CrdtStreamOut.Message.Failure(failure)) =>
      if (failure.commandId != 0) {
        val initiator = outstanding(failure.commandId)
        outstanding -= failure.commandId
        initiator ! UserFunctionReply(message = UserFunctionReply.Message.Failure(failure))
        operationFinished()
      }

    case DataDeleted(_, request) =>
      // These are received in response to updates or gets for a deleted key. If it was an update, technically
      // it was successful, it's just that the delete overules it.
      request.foreach {
        case InitiatorReply(commandId, initiator, userFunctionReply) =>
          initiator ! userFunctionReply
          outstanding -= commandId
          sendDelete()
      }

    case StreamClosed =>
      crash("Unexpected entity termination due to stream closure")

    case Stop =>
      if (outstanding.isEmpty) {
        context.stop(self)
      } else {
        stopping = true
      }

    case ReceiveTimeout =>
      context.parent ! CrdtEntityManager.Passivate
  }

  private def operationFinished(): Unit = {
    if (stopping) {
      if (outstanding.isEmpty) {
        context.stop(self)
      }
    } else {
      if (outstandingOperations > 1) {
        // Just decrement it
        outstandingOperations -= 1
      } else {
        // Otherwise, do a get to restart pushing deltas to the user function
        replicator ! Get(key, ReadLocal)
      }
    }
  }

  private def crash(message: String, cause: Option[Throwable] = None): Unit = {
    outstanding.values.foreach { actor =>
      actor ! UserFunctionReply(message = UserFunctionReply.Message.Failure(Failure(description = "Entity terminating")))
    }
    outstanding = Map.empty

    val error = cause.getOrElse(new Exception(message))
    if (relay != null) {
      relay ! Status.Failure(error)
    }

    entityDiscovery.reportError(UserFunctionError(message))

    throw error
  }

  // We both apply the update to our current state, as well as produce the function that will update the
  // replicators version, since these might not be the same thing.
  private def deltaToUpdate(delta: CrdtDelta): (ReplicatedData, ReplicatedData => ReplicatedData) = {

    import CrdtDelta.{Delta => D}

    (delta.delta) match {

      case D.Gcounter(GCounterDelta(increment)) =>
        (GCounter.empty, {
          case gcounter: GCounter => gcounter :+ increment
          case other => throw new RuntimeException(s"GCounterDelta is incompatible with CRDT $other")
        })

      case D.Pncounter(PNCounterDelta(change)) =>
        (PNCounter.empty, {
          case pncounter: PNCounter => pncounter :+ change
          case other => throw new RuntimeException(s"PNCounterDelta is incompatible with CRDT $other")
        })

      case D.Gset(GSetDelta(added)) =>
        (GSet.empty[KeyedEqualityValueWrapper], {
          case gset: GSet[KeyedEqualityValueWrapper @unchecked] => added.foldLeft(gset)((gset, e) => gset + KeyedEqualityValueWrapper(e))
          case other => throw new RuntimeException(s"GSetDelta is incompatible with CRDT $other")
        })

      case D.Orset(ORSetDelta(cleared, removed, added)) =>
        (ORSet.empty[KeyedEqualityValueWrapper], {
          case orset: ORSet[KeyedEqualityValueWrapper @unchecked] =>
            val maybeCleared = if (cleared) orset.clear(selfUniqueAddress)
            else orset
            val withRemoved = removed.foldLeft(maybeCleared)((orset, key) => orset remove KeyedEqualityValueWrapper(KeyedEqualityValue(key)))
            added.foldLeft(withRemoved)((orset, value) => orset :+ KeyedEqualityValueWrapper(value))
          case other => throw new RuntimeException(s"ORSetDelta is incompatible with CRDT $other")
        })

      case D.Lwwregister(LWWRegisterDelta(maybeValue, clock, customClockValue)) =>
        val value = maybeValue.getOrElse(ProtoAny.defaultInstance)
        (LWWRegister.create(value), {
          case lwwregister: LWWRegister[ProtoAny @unchecked] =>
            lwwregister.withValue(selfUniqueAddress, value, toDdataClock(clock, customClockValue))
          case other => throw new RuntimeException(s"LWWRegisterDelta is incompatible with CRDT $other")
        })

      case D.Flag(FlagDelta(value)) =>
        (Flag.empty, {
          case flag: Flag =>
            if (value) flag.switchOn
            else flag
          case other => throw new RuntimeException(s"FlagDelta is incompatible with CRDT $other")
        })

      case D.Ormap(ORMapDelta(cleared, removed, updated, added)) =>
        (ORMap.empty[KeyedEqualityValueWrapper, ReplicatedData], {
          case ormap: ORMap[KeyedEqualityValueWrapper @unchecked, ReplicatedData @unchecked] =>

            val maybeCleared = if (cleared) ormap.entries.keySet.foldLeft(ormap)((ormap, key) => ormap remove key)
            else ormap

            val withRemoved = removed.foldLeft(maybeCleared)((ormap, key) => ormap remove KeyedEqualityValueWrapper(KeyedEqualityValue(key)))

            val withUpdated = updated.foldLeft(withRemoved) { case (ormap, ORMapEntryDelta(Some(key), Some(delta))) =>
              // While the CRDT we're using won't have changed, the CRDT in the replicator may have, so we detect that.
              val wrapped = KeyedEqualityValueWrapper(key)
              ormap.get(wrapped) match {
                case Some(data) =>
                  try {
                    val (initial, modify) = deltaToUpdate(delta)
                    ormap.updated(selfUniqueAddress, wrapped, initial)(modify)
                  } catch {
                    case _: RuntimeException =>
                      // The delta is incompatible, the value must have been removed and then added again, so ignore
                      ormap
                  }
                case None =>
                  // There is no element, it must have been removed, ignore
                  ormap
              }
            }

            added.foldLeft(withUpdated) {
              case (ormap, ORMapEntry(Some(key), Some(state))) =>
                ormap.put(selfUniqueAddress, KeyedEqualityValueWrapper(key), stateToCrdt(state))
            }
        })

      case D.Empty =>
        throw new RuntimeException("Empty delta")
    }
  }

  private def stateToCrdt(state: CrdtState): ReplicatedData = {
    import CrdtState.{State => S}
    state.state match {
      case S.Gcounter(GCounterState(value)) => GCounter.empty :+ value
      case S.Pncounter(PNCounterState(value)) => PNCounter.empty :+ value
      case S.Gset(GSetState(items)) => items.foldLeft(GSet.empty[KeyedEqualityValueWrapper])((gset, item) => gset + KeyedEqualityValueWrapper(item))
      case S.Orset(ORSetState(items)) => items.foldLeft(ORSet.empty[KeyedEqualityValueWrapper])((orset, item) => orset :+ KeyedEqualityValueWrapper(item))
      case S.Lwwregister(LWWRegisterState(value, clock, customClockValue)) => LWWRegister(selfUniqueAddress, value.getOrElse(ProtoAny.defaultInstance), toDdataClock(clock, customClockValue))
      case S.Flag(FlagState(value)) => if (value) Flag.Enabled else Flag.Disabled
      case S.Ormap(ORMapState(items)) => items.foldLeft(ORMap.empty[KeyedEqualityValueWrapper, ReplicatedData]) {
        case (ormap, ORMapEntry(Some(key), Some(state))) => ormap.put(selfUniqueAddress, KeyedEqualityValueWrapper(key), stateToCrdt(state))
      }
      case S.Empty => throw new RuntimeException("Unknown state or state not set")
    }
  }

  private def toDdataClock(clock: CrdtClock, customClockValue: Long): Clock[ProtoAny] = {
    clock match {
      case CrdtClock.DEFAULT => LWWRegister.defaultClock
      case CrdtClock.REVERSE => LWWRegister.reverseClock
      case CrdtClock.CUSTOM => new CustomClock(customClockValue, false)
      case CrdtClock.CUSTOM_AUTO_INCREMENT => new CustomClock(customClockValue, true)
      case CrdtClock.Unrecognized(_) => LWWRegister.defaultClock
    }
  }

  private def toDdataWriteConsistency(wc: CrdtReply.WriteConsistency): WriteConsistency = wc match {
    case CrdtReply.WriteConsistency.LOCAL => WriteLocal
    case CrdtReply.WriteConsistency.MAJORITY => WriteMajority(configuration.writeTimeout)
    case CrdtReply.WriteConsistency.ALL => WriteAll(configuration.writeTimeout)
    case _ => WriteLocal
  }

  // We stay active while deleted so we can cache the deletion
  private def deleted: Receive = {
    case Relay(r) =>
      relay = r
      sendDelete()

    case c @ Changed(_) =>
      // Ignore

    case Deleted(_) =>
      // Ignore, we know.

    case EntityCommand(_, _, _) =>
      sender() ! UserFunctionReply(message = UserFunctionReply.Message.Failure(Failure(description = "Entity deleted")))

    case CrdtStreamOut(CrdtStreamOut.Message.Reply(reply)) =>
      val initiator = outstanding(reply.commandId)

      val userFunctionReply = UserFunctionReply(
        sideEffects = reply.sideEffects,
        message = reply.response match {
          case CrdtReply.Response.Empty => UserFunctionReply.Message.Empty
          case CrdtReply.Response.Reply(payload) => UserFunctionReply.Message.Reply(payload)
          case CrdtReply.Response.Forward(forward) => UserFunctionReply.Message.Forward(forward)
        }
      )

      // Just send the reply. If it's an update, technically it's not invalid to update a CRDT that's been deleted,
      // it's just that the result of merging the delete and the update is that stays deleted. So we don't need to
      // fail.
      initiator ! userFunctionReply
      outstanding -= reply.commandId

    case UpdateSuccess(_, Some(InitiatorReply(commandId, initiator, userFunctionReply))) =>
      initiator ! userFunctionReply
      outstanding -= commandId

    case GetSuccess(_, _) =>
      // Possible if we issued the get before the next operation then deleted. Ignore.

    case UpdateTimeout(_, Some(InitiatorReply(commandId, initiator, _))) =>
      outstanding -= commandId
      initiator ! UserFunctionReply(message = UserFunctionReply.Message.Failure(Failure(description = "Failed to update CRDT at requested write consistency")))

    case ModifyFailure(_, error, cause, Some(InitiatorReply(commandId, initiator, _))) =>
      outstanding -= commandId
      initiator ! UserFunctionReply(message = UserFunctionReply.Message.Failure(Failure(description = "Error updating CRDT")))

    case CrdtStreamOut(CrdtStreamOut.Message.Failure(failure)) =>
      if (failure.commandId != 0) {
        val initiator = outstanding(failure.commandId)
        outstanding -= failure.commandId
        initiator ! UserFunctionReply(message = UserFunctionReply.Message.Failure(failure))
      }

    case DataDeleted(_, request) =>
      // These are received in response to updates or gets for a deleted key. If it was an update, technically
      // it was successful, it's just that the delete overules it.
      request.foreach {
        case InitiatorReply(commandId, initiator, userFunctionReply) =>
          initiator ! userFunctionReply
          outstanding -= commandId
          sendDelete()
      }

    case DeleteSuccess(_, Some(InitiatorReply(commandId, initiator, userFunctionReply))) =>
      outstanding -= commandId
      initiator ! userFunctionReply

    case ReplicationDeleteFailure(_, Some(InitiatorReply(commandId, initiator, _))) =>
      outstanding -= commandId
      initiator ! UserFunctionReply(message = UserFunctionReply.Message.Failure(Failure(description = "Failed to delete CRDT at requested write consistency")))

    case StreamClosed =>
      // Ignore

    case ReceiveTimeout =>
      context.parent ! CrdtEntityManager.Passivate

    case Stop =>
      context.stop(self)
  }

  private def sendToRelay(message: CrdtStreamIn.Message): Unit = {
    relay ! CrdtStreamIn(message)
  }

  private def detectChange(original: ReplicatedData, changed: ReplicatedData): CrdtChange = {
    import CrdtChange._
    import CrdtDelta.{Delta => D}

    changed match {

      case gcounter: GCounter =>
        original match {
          case old: GCounter =>
            if (old.value > gcounter.value) IncompatibleChange
            else if (old.value == gcounter.value) NoChange
            else Updated(CrdtDelta(D.Gcounter(GCounterDelta(gcounter.value.toLong - old.value.toLong))))
          case _ => IncompatibleChange
        }

      case pncounter: PNCounter =>
        original match {
          case old: PNCounter =>
            if (old.value == pncounter.value) NoChange
            else Updated(CrdtDelta(D.Pncounter(PNCounterDelta(pncounter.value.toLong - old.value.toLong))))
          case _ => IncompatibleChange
        }

      case gset: GSet[KeyedEqualityValueWrapper @unchecked] =>
        original match {
          case old: GSet[KeyedEqualityValueWrapper @unchecked] =>
            val diff = gset.elements -- old.elements
            if (old.elements.size + diff.size > gset.elements.size) IncompatibleChange
            else if (diff.isEmpty) NoChange
            else Updated(CrdtDelta(D.Gset(GSetDelta(diff.map(_.value).toSeq))))
          case _ => IncompatibleChange
        }

      case orset: ORSet[KeyedEqualityValueWrapper @unchecked] =>
        original match {
          case old: ORSet[KeyedEqualityValueWrapper @unchecked] =>
            // Fast path, just cleared
            if (orset.elements.isEmpty) {
              if (old.elements.isEmpty) {
                NoChange
              } else {
                Updated(CrdtDelta(D.Orset(ORSetDelta(
                  cleared = true
                ))))
              }
            } else {
              val removed = old.elements -- orset.elements
              val added = orset.elements -- old.elements
              if (removed.isEmpty && added.isEmpty) {
                NoChange
              } else {
                Updated(CrdtDelta(D.Orset(ORSetDelta(
                  removed = removed.map(_.value.key).toSeq,
                  added = added.map(_.value).toSeq
                ))))
              }
            }
          case _ => IncompatibleChange
        }

      case lwwregister: LWWRegister[ProtoAny @unchecked] =>
        original match {
          case old: LWWRegister[ProtoAny @unchecked] =>
            if (old.value == lwwregister.value) NoChange
            else Updated(CrdtDelta(D.Lwwregister(LWWRegisterDelta(Some(lwwregister.value)))))
          case _ => IncompatibleChange
        }

      case flag: Flag =>
        original match {
          case old: Flag =>
            if (old.enabled && !flag.enabled) IncompatibleChange
            else if (old.enabled == flag.enabled) NoChange
            else Updated(CrdtDelta(D.Flag(FlagDelta(flag.enabled))))
          case _ => IncompatibleChange
        }

      case ormap: ORMap[KeyedEqualityValueWrapper @unchecked, ReplicatedData @unchecked] =>

        import ORMapEntryAction._
        original match {

          case old: ORMap[KeyedEqualityValueWrapper @unchecked, ReplicatedData @unchecked] =>

            if (ormap.isEmpty) {
              if (old.isEmpty) NoChange
              else Updated(CrdtDelta(D.Ormap(ORMapDelta(cleared = true))))
            } else {

              val changes = ormap.entries.map {
                case (k, value) if !old.contains(k) => AddEntry(ORMapEntry(Some(k.value), Some(toWireState(value))))
                case (k, value) =>
                  detectChange(old.entries(k), value) match {
                    case NoChange => NoAction
                    case IncompatibleChange => DeleteThenAdd(k.value, toWireState(value))
                    case Updated(delta) => UpdateEntry(k.value, delta)
                  }
              }.toSeq

              val deleted = old.entries.keySet -- ormap.entries.keys

              if (deleted.isEmpty || changes.isEmpty) {
                NoChange
              } else {
                val allDeleted = deleted.map(_.value.key) ++ changes.collect {
                  case DeleteThenAdd(k, _) => k.key
                }
                val updated = changes.collect {
                  case UpdateEntry(key, delta) => ORMapEntryDelta(Some(key), Some(delta))
                }
                val added = changes.collect {
                  case AddEntry(entry) => entry
                  case DeleteThenAdd(key, state) => ORMapEntry(Some(key), Some(state))
                }

                Updated(CrdtDelta(D.Ormap(ORMapDelta(
                  removed = allDeleted.toSeq,
                  updated = updated,
                  added = added
                ))))
              }
            }

          case _ => IncompatibleChange

        }

      case _ =>
        // todo handle better
        throw new RuntimeException("Unknown CRDT: " + changed)

    }
  }



}
