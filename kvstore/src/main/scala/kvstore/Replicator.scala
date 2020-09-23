package kvstore

import akka.actor.{Actor, ActorLogging, ActorRef, Props}

import scala.concurrent.duration._

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)
  case class RetrySnapshot(key: String, valueOption: Option[String], seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor with ActorLogging {
  import Replicator._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var pendingAcknowledgment = Map.empty[Long, (ActorRef, Replicate)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]

  var _seqCounter = 0L
  def nextSeq() = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  /* TODO Behavior for the Replicator. */
  def receive: Receive = {
    case replicate: Replicate               => handleReplicate(replicate)
    case snapshotAcknowledged: SnapshotAck  => handleSnapshotAcknowledged(snapshotAcknowledged)
    case retrySnapshot: RetrySnapshot       => handleRetrySnapshot(retrySnapshot)
    case _                                  => ()
  }

  /** Initiates the replication of a given update to a key.
   * by sending a Snapshot to the corresponding Replica */
  def handleReplicate(replicate: Replicate): Unit = {
    log.info(s"Intiating replication {} of {}: {}", replicate.id, replicate.key, replicate.valueOption)
    val updateSequenceNumber = nextSeq()
    pendingAcknowledgment += updateSequenceNumber -> (sender() ,replicate)
    replica ! Snapshot(replicate.key, replicate.valueOption, updateSequenceNumber)

    context.system.scheduler.scheduleOnce(10.milliseconds) {
      self ! RetrySnapshot(replicate.key, replicate.valueOption, updateSequenceNumber)
    }
  }

  /** Notifies the primary Node that a Replicate request has been fullfiled  */
  def handleSnapshotAcknowledged(acknowledgement: SnapshotAck) = {
    pendingAcknowledgment.get(acknowledgement.seq) match {
      case Some(_) =>
        log.info(s"Received acknowledgement for sequence ${acknowledgement.seq}")
        for((replica, operation) <- pendingAcknowledgment.get(acknowledgement.seq))
          replica ! Replicated(acknowledgement.key, operation.id)

        pendingAcknowledgment -= acknowledgement.seq
      case None => ()
    }
  }

  def handleRetrySnapshot(retry: RetrySnapshot) = {
    pendingAcknowledgment.get(retry.seq) match {
      case Some(_) =>
        log.info(s"Retrying SNAPSHOT $retry")
        replica ! Snapshot(retry.key, retry.valueOption, retry.seq)
        context.system.scheduler.scheduleOnce(50.milliseconds) {
          self ! RetrySnapshot(retry.key, retry.valueOption, retry.seq)
        }
      case None => ()
    }
  }
}
