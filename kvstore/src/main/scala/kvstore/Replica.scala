package kvstore

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorLogging, ActorRef, OneForOneStrategy, PoisonPill, Props, SupervisorStrategy, Terminated, Timers}
import kvstore.Arbiter._
import scala.concurrent.duration._

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  case class RetryPersist(key: String, valueOption: Option[String], id: Long)
  case class FailUnpersisted(id: Long)
  case class PendingOperation(
                               id: Long,
                               requester: ActorRef,
                               persistedByPrimary: Boolean,
                               acksReceived: Int,
                               acksRequired: Int,
                               replicatorsReplicated: Set[ActorRef] = Set.empty[ActorRef]
                             )
  case class OperationTimeout(id: Long)
  case class GlobalCheck()

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props)
  extends Actor with ActorLogging with Timers {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]
  var replicatorId = 1L
  var replicationRequestId = 1
  var globalCheck = 1

  val persistor = context.actorOf(persistenceProps)
  var pendingOperations = Set.empty[PendingOperation]

  override val supervisorStrategy = OneForOneStrategy() {
    case _: PersistenceException => Restart
  }

  /** Joins the arbiter and starts the persistent actor */
  override def preStart(): Unit = {
    arbiter ! Join
    timers.startTimerAtFixedRate(nextGlobalCheck(), GlobalCheck, 2.second)
  }

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case msg: Operation         => handleOperation(msg)
    case msg: Replicas          => handleReplicas(msg.replicas)
    case msg: Replicated        => handleReplicated(msg)
    case msg: Persisted         => handlePrimaryPersisted(msg)
    case msg: RetryPersist      => handleRetryPersist(msg)
    case msg: OperationTimeout  => handleOperationTimedOut(msg)
    case msg: FailUnpersisted   => handleFailUnpersisted(msg)
  }

  /** Leader Hanlders */

  def handleFailUnpersisted(unpersisted: FailUnpersisted)  {
    pendingOperations.find(_.id == unpersisted.id) match {
      case Some(op) =>
        op.requester ! OperationFailed(op.id)
        pendingOperations -= op
      case None => ()
    }
  }

  def handleOperationTimedOut(timeout: OperationTimeout) = {
    pendingOperations.find(_.id == timeout.id) match {
      case Some(operation) =>
        operation.requester ! OperationFailed(operation.id)
        log.info(s"Operation {} timed out and failed ", timeout.id)
      case None => ()
    }
  }

  def handleRetryPersist(retry: RetryPersist) = {
    pendingOperations.find(_.id == retry.id) match {
      case Some(_) =>
        persistor ! Persist(retry.key, retry.valueOption, retry.id)
        context.system.scheduler.scheduleOnce(50.milliseconds) {
          self ! RetryPersist(retry.key, retry.valueOption, retry.id)
        }
      case None =>
    }
  }

  def handlePrimaryPersisted(persisted: Persisted) = {
    pendingOperations.find(_.id == persisted.id) match {
      case Some(operation) =>
        pendingOperations -= operation
        val updatedOperation = operation.copy(persistedByPrimary = true)

        acknowledgeOrKeepPending(updatedOperation)

      case None => ()
    }
  }

  def handleReplicated(replicated: Replicated) = {
    pendingOperations.find(_.id == replicated.id) match {
      case Some(operation) =>
        //update operation state
        pendingOperations -= operation
        val updatedOperation = operation.copy(
          acksReceived = operation.acksReceived + 1,
          replicatorsReplicated = operation.replicatorsReplicated + sender())

        acknowledgeOrKeepPending(updatedOperation)

      case None => ()
    }
  }

  def acknowledgeOrKeepPending(updatedOperation: PendingOperation) = {
    (updatedOperation.acksReceived, updatedOperation.acksRequired, updatedOperation.persistedByPrimary) match {
      case (received, required, true) if received == required  =>
        updatedOperation.requester ! OperationAck(updatedOperation.id)
      case _ =>
        pendingOperations += updatedOperation
    }
  }


  def handleOperation(operation: Operation) = {
    operation match {
      case Insert(key, value, id) =>
        kv += key -> value
        pendingOperations += PendingOperation(operation.id, sender(), false, 0, replicators.size)
        replicators foreach { _ ! Replicate(key, Some(value), id)}
        persistor ! Persist(key, Some(value), id)

        context.system.scheduler.scheduleOnce(50.milliseconds) {
          self ! RetryPersist(key, Some(value), id)
        }
        context.system.scheduler.scheduleOnce(1.second) {
          self ! FailUnpersisted(id)
        }

      case Remove(key, id) =>
        kv -= key
        pendingOperations += PendingOperation(operation.id, sender(), false, 0, replicators.size)
        replicators foreach { _ ! Replicate(key, None, id) }
        persistor ! Persist(key, None, id)

        context.system.scheduler.scheduleOnce(100.milliseconds) {
          self ! RetryPersist(key, None, id)
        }
        context.system.scheduler.scheduleOnce(1.second) {
          self ! FailUnpersisted(id)
        }

      case Get(key, id) => sender() ! GetResult(key, kv.get(key),id)
    }
  }

  def handleReplicas(replicas: Set[ActorRef]) = {
    log.info(s"REPLICAS JOINED: {}", replicas)
    val currentSecondaries = replicas - self
    val previousSecondaries = secondaries.keySet

    val newSecondaries = currentSecondaries.diff(previousSecondaries)
    val removedSecondaries = previousSecondaries.diff(currentSecondaries)

    val replicatorsToKill = for {
      removedReplica <- removedSecondaries
      replicator <- secondaries.get(removedReplica)
    } yield {
      replicators -= replicator
      secondaries -= removedReplica
      context.stop(removedReplica)
      context.stop(replicator)
      replicator
    }

    newSecondaries foreach { newSecondary =>
      val newReplicator = context.actorOf(Replicator.props(newSecondary), s"replicator-${nextReplicatorId()}")
      replicators += newReplicator
      secondaries += newSecondary -> newReplicator
    }

    pendingOperations foreach { op =>
      pendingOperations -= op
      val replicatorsThatReplicated = op.replicatorsReplicated
      val removeAcks = for {
        rr <- replicatorsThatReplicated
        if replicatorsToKill.contains(rr)
      } yield rr

      val updatedOp = op.copy(
        acksRequired = replicators.size,
        acksReceived = op.acksReceived - removeAcks.size
      )
      pendingOperations += updatedOp

    }
    for {
      (key , value) <- kv
      replicator <- replicators
    } yield replicator ! Replicate(key, Some(value), nextReplicationRequestId())
  }


  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case msg: Insert            => sender() ! OperationFailed(msg.id)
    case msg: Remove            => sender() ! OperationFailed(msg.id)
    case msg: Get               => sender() ! GetResult(msg.key, kv.get(msg.key), msg.id)
    case msg: Snapshot          => handleSnapshot(msg)
    case msg: Persisted         => handleSecondaryPersisted(msg)
    case msg: RetryPersist      => handleRetryPersist(msg)
    case msg: FailUnpersisted   => handleFailUnpersisted(msg)
  }

  /** Replica handlers */

  def handleSecondaryPersisted(persisted: Persisted) = {
    pendingOperations.find(_.id == persisted.id) match {
      case Some(pendingOp: PendingOperation) =>
        log.info(s"Received confirmation of persistence for operation ${persisted.id}")
        pendingOp.requester ! SnapshotAck(persisted.key, persisted.id)
        pendingOperations -= pendingOp

      case None => ()
    }
  }

  /** Receives a Snapshot message that indicates the state of a given key.
   * with a sequence number to enforce ordering.   */
  var expectedUpdateSequenceNumber = 0L
  def handleSnapshot(snapshot: Snapshot) = {
    snapshot match {
      case _ if expectedUpdateSequenceNumber == snapshot.seq =>
        snapshot.valueOption match {
          case Some(value) => kv += snapshot.key -> value
          case None => kv -= snapshot.key
        }
        expectedUpdateSequenceNumber += 1
        persistor ! Persist(snapshot.key, snapshot.valueOption, snapshot.seq)
        pendingOperations += PendingOperation(snapshot.seq, sender(), false, 0, 0)

        context.system.scheduler.scheduleOnce(100.milliseconds) {
          self ! RetryPersist(snapshot.key, snapshot.valueOption, snapshot.seq)
        }
        context.system.scheduler.scheduleOnce(1.second) {
          self ! FailUnpersisted(snapshot.seq)
        }
      case _ if snapshot.seq < expectedUpdateSequenceNumber =>
        sender() ! SnapshotAck(snapshot.key, snapshot.seq)
      case _ if snapshot.seq > expectedUpdateSequenceNumber => ()
    }
  }

  /** Other Helpers */
  def nextReplicatorId() = {
    val newId = replicatorId
    replicatorId += 1
    newId
  }
  def nextReplicationRequestId() = {
    val newId = replicationRequestId
    replicationRequestId += 1
    newId
  }

  def nextGlobalCheck() = {
    val newCheck = globalCheck
    globalCheck += 1
    newCheck
  }
}

