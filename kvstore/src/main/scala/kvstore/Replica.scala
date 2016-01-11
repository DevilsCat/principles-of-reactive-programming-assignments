package kvstore

import akka.actor.{ OneForOneStrategy, Props, ActorRef, Actor }
import kvstore.Arbiter._
import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart
import scala.annotation.tailrec
import akka.pattern.{ ask, pipe }
import akka.actor.Terminated
import scala.concurrent.duration._
import akka.actor.PoisonPill
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy
import akka.util.Timeout
import akka.actor.Cancellable
import akka.event.LoggingReceive

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
  
  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
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
  // unique defines replicator id
  var replicatorId = 0L
  // For replica, used to track the sequence of snapshot. Ignore the illegal one.
  var internalSnapshotSeq = 0L

  // Create a persistent actor for use
  val persistent = context.actorOf(persistenceProps)

  final val scheduler = context.system.scheduler
  
  arbiter ! Arbiter.Join

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case Insert(key, value, id) =>
      kv += { key -> value }
      //val (cancelTimeout, cancelTimer) = registerToSchedulerBeforeWait(() => persistent ! Persist(key, kv.get(key), id))
      val cancelTimer = scheduler.schedule(0.days, 100.milliseconds)(persistent ! Persist(key, kv.get(key), id))
      val cancelTimeout = scheduler.scheduleOnce(1.seconds, self, Timeout)
      context.become(waitPrimaryPersistDone(sender, key, id, cancelTimeout, cancelTimer))
    case Remove(key, id) =>
      kv -= key
      val cancelTimer = scheduler.schedule(0.days, 100.milliseconds)(persistent ! Persist(key, kv.get(key), id))
      val cancelTimeout = scheduler.scheduleOnce(1.seconds, self, Timeout)
      context.become(waitPrimaryPersistDone(sender, key, id, cancelTimeout, cancelTimer))
    case Get(key, id) => sender ! GetResult(key, kv.get(key), id)
    case Replicas(replicas) =>
      // provide each replica a replicator and insert them to data structure.
      replicas map {
        case replica =>
          if (!replica.equals(self)) {
            val replicator = context.actorOf(Replicator.props(replica), s"r$replicatorId")
            replicatorId += 1
            replicators += replicator
            secondaries += replica -> replicator
          }
      }
  }
  def waitPrimaryPersistDone(requester: ActorRef, 
                             key: String, id: Long,
                             cancelTimeout: Cancellable, cancelTimer: Cancellable): Receive = {
    case Get(key, id) => sender ! GetResult(key, kv.get(key), id)
    case Persist(key, optVal, id) => persistent ! Persist(key, optVal, id)
    case Persisted(key, id) =>
      if (replicators.isEmpty) {
        requester ! OperationAck(id)
        context.become(leader)
      } else {
        cancelTimer.cancel
        context.become(waitReplicatesDone(
            requester, replicators.zipWithIndex, 
            key, id, 
            cancelTimeout,
            scheduler.schedule(0.days, 100.milliseconds)(self ! Replicate(key, kv.get(key), id))
        ))
      }
    case Timeout =>
      cancelTimeout.cancel
      cancelTimer.cancel
      requester ! OperationFailed(id)
      context.become(leader)
  }
  
  def waitReplicatesDone(requester: ActorRef, replicators: Set[(ActorRef, Int)], 
                         key: String, requestId: Long, 
                         cancelTimeout: Cancellable, cancelTimer: Cancellable): Receive = {
   case Get(key, id) => sender ! GetResult(key, kv.get(key), id) 
   case Replicate(key, optVal, id) => for ((replicator, rid) <- replicators) replicator ! Replicate(key, optVal, rid)
   case Replicated(key, id) =>
      val filtReplicators = replicators.filter{case (_, replicatorId) =>  replicatorId != id}
      if (filtReplicators.isEmpty) {
        cancelTimeout.cancel
        cancelTimer.cancel
        requester ! OperationAck(requestId)
        context.become(leader)
      } else {
        context.become(waitReplicatesDone(requester, filtReplicators, key, requestId, cancelTimeout, cancelTimer))
      }
   case Timeout =>
      cancelTimer.cancel
      requester ! OperationFailed(requestId)
      context.become(leader)
  }
  
  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case Get(key, id) => sender ! GetResult(key, kv.get(key), id)
    case Snapshot(key, valOpt, seq) =>
      if (seq > internalSnapshotSeq) sender ! SnapshotAck(key, seq)
      else {
        storeToKV(key, valOpt, seq)
        val scheduler = context.system.scheduler
        val cancellable = scheduler.schedule(0.days, 100.millis)(persistent ! Persist(key, valOpt, seq))
        context.become(waitPersistDone(sender, cancellable))
      }

  }

  def waitPersistDone(requester: ActorRef, cancellable: Cancellable): Receive = {
    case Get(key, id) => sender ! GetResult(key, kv.get(key), id)
    case Persisted(key, id) =>
      cancellable.cancel
      requester ! SnapshotAck(key, id)
      context.become(replica)
  }

  def storeToKV(key: String, valOpt: Option[String], seq: Long): Unit = {
    if (seq == internalSnapshotSeq) {
      internalSnapshotSeq += 1
      if (!valOpt.isEmpty) kv += { key -> valOpt.get }
      else kv -= key
    }
  }
}

