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
  val leader: Receive = LoggingReceive {
    case Insert(key, value, id) =>
      kv += { key -> value }
      val cancelTimer = scheduler.schedule(0.days, 100.milliseconds)(persistent ! Persist(key, kv.get(key), id))
      val cancelTimeout = scheduler.scheduleOnce(1.seconds, self, Timeout)
      context.become(waitAllDone(sender, replicators.zipWithIndex, key, id, cancelTimeout, cancelTimer))
    case Remove(key, id) =>
      kv -= key
      val cancelTimer = scheduler.schedule(0.days, 100.milliseconds)(persistent ! Persist(key, kv.get(key), id))
      val cancelTimeout = scheduler.scheduleOnce(1.seconds, self, Timeout)
      context.become(waitAllDone(sender, replicators.zipWithIndex, key, id, cancelTimeout, cancelTimer))
    case Get(key, id) => sender ! GetResult(key, kv.get(key), id)
    case Replicas(replicas) =>
      val removedPair = secondaries.filter{case (replica, replicator) => !replicas.contains(replica) }
      for ((replica, replicator) <- removedPair) {
        replicator ! PoisonPill // stop replicator
        replicators -= replicator  
        secondaries -= replica
      }
      val addedReplicas = replicas.filter { case replica => !secondaries.contains(replica) && !replica.equals(self) }
      addedReplicas map {
        case replica =>
          val replicator = context.actorOf(Replicator.props(replica), s"replicator$replicatorId")
          replicatorId += 1
          replicators += replicator
          secondaries += replica -> replicator
          for ((k, v) <- kv; (replicator, rid) <- replicators.zipWithIndex) {
            replicator ! Replicate(k, Some(v), rid)
          }
      }
  }
  def waitAllDone(requester: ActorRef, replicators: Set[(ActorRef, Int)],
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
        context.become(waitAllDone(
            requester, replicators, 
            key, id, 
            cancelTimeout,
            scheduler.schedule(0.day, 100.milliseconds)(for ((replicator, rid) <- replicators) replicator ! Replicate(key, kv.get(key), rid))
        ))
      }
    case Replicated(key, rid) =>
      val filtReplicators = replicators.filter{case (_, replicatorId) =>  replicatorId != rid}
      if (filtReplicators.isEmpty) {
        cancelTimeout.cancel
        cancelTimer.cancel
        requester ! OperationAck(id)
        context.become(leader)
      } else {
        context.become(waitAllDone(requester, filtReplicators, key, id, cancelTimeout, cancelTimer))
      }
    case Replicas(replicas) =>
     val removedPair = secondaries.filter{case (replica, replicator) => !replicas.contains(replica) }
     for ((replica, replicator) <- removedPair) {
       replicator ! PoisonPill // stop replicator
       this.replicators -= replicator  // remove from global  
       secondaries -= replica
     }
     val filtReplicators = replicators.filter{ case (replicator, _) => !removedPair.values.toSet.contains(replicator) }
     if (filtReplicators.isEmpty) {
       requester ! OperationAck(id)
       context.become(leader)
     } else {
       context.become(waitAllDone(requester, filtReplicators, key, id, cancelTimeout, cancelTimer))
     }
    case Timeout =>
      cancelTimeout.cancel
      cancelTimer.cancel
      requester ! OperationFailed(id)
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

