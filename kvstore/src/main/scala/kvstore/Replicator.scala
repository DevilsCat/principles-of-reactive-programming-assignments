package kvstore

import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorRef
import scala.concurrent.duration._
import akka.actor.Cancellable
import akka.event.LoggingReceive


object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {
  import Replicator._
  import Replica._
  import context.dispatcher
  
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]
  
  var _seqCounter = 0L
  def nextSeq = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }
  
  /* TODO Behavior for the Replicator. */
  def receive: Receive = waiting
  
  val waiting: Receive = {
    case Replicate(key, valOpt, id) =>
      val seqNo = nextSeq
      val scheduler = context.system.scheduler
      val cancellable = scheduler.schedule(10.milliseconds, 100.milliseconds)(replica ! Snapshot(key, valOpt, seqNo))
      context.become(waitSnapshotDone(cancellable, id))
  }
  
  def waitSnapshotDone(cancellable: Cancellable, id: Long): Receive = {
    case SnapshotAck(key, seq) => cancellable.cancel
                                  context.parent ! Replicated(key, id)
                                  context.become(waiting)
  }
}
