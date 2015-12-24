package nodescala

import scala.language.postfixOps
import scala.util.{Try, Success, Failure}
import scala.collection._
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.async.Async.{async, await}
import org.scalatest._
import NodeScala._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.time.Millisecond
import scala.util.Failure

@RunWith(classOf[JUnitRunner])
class NodeScalaSuite extends FunSuite {

  test("A Future should always be completed") {
    val always = Future.always(517)

    assert(Await.result(always, Duration(0, NANOSECONDS)) == 517)
  }
  test("A Future should never be completed") {
    val never = Future.never[Int]

    try {
      Await.result(never, Duration(1, SECONDS))
      assert(false)
    } catch {
      case t: TimeoutException => // ok!
    }
  }
  test("All futures should be completed") {
    val allFts = Future.all(List(Future{1}, Future{2}, Future{3}))
    
    assert(Await.result(allFts, Duration(1, SECONDS)) == List(1,2,3))
  }

  test("One of the futures should be completed") {
    val anyFts = Future.any(List(Future{1}, Future{2}, Future{throw new Exception}))
    
    try {
      assert(List(1,2).contains(Await.result(anyFts, Duration(1, MILLISECONDS))))
    } catch {
      case t: Exception => // ok!
    }
  }
  
  test("Future should be delayed for more than 2 seconds") {
    val delayFt1 = Future.delay(Duration(2000, "millis"))
    val delayFt2 = Future.delay(Duration(2000, "millis"))
    
    try {
      Await.result(delayFt1, Duration(1500, "millis"))
      assert(false)
    } catch {
      case t: TimeoutException => // ok!
    }
    
    try {
      Await.result(delayFt2, Duration(2500, "millis"))
    } catch {
      case t: TimeoutException => assert(false)
    }
  }
  
  test("Future now should show the state and result") {
    val uft = Future.always(1)
    assert(uft.now == 1)
    try {
      Future.never[Int].now
      assert(false)
    } catch {
      case t: NoSuchElementException => // ok!
    }
  }
  
  test("ContinueWith") {
    val uft = Future{4}.continueWith { self => 
      val res = Await.result(self, Duration(1, MILLISECONDS))
      res + 42
    }
    assert(Await.result(uft, Duration(100, MILLISECONDS)) == 46)
  }
  
  test("Continue") {
    val uft = Future{4}.continue { res => res match {
      case Success(x) => x + 42
      case Failure(error) => throw error
    }}
    assert(Await.result(uft, Duration(100, MILLISECONDS)) == 46)
  }
  
  test("Future should be able to be cancelled") {
    val working = Future.run() { ct =>
      Future {
        while (ct.nonCancelled) {}
        println("done")
      }
    }
    Await.result(Future.delay(Duration(5, SECONDS)), Duration(10, SECONDS))
    working.unsubscribe()
  }
  
  class DummyExchange(val request: Request) extends Exchange {
    @volatile var response = ""
    val loaded = Promise[String]()
    def write(s: String) {
      response += s
    }
    def close() {
      loaded.success(response)
    }
  }

  class DummyListener(val port: Int, val relativePath: String) extends NodeScala.Listener {
    self =>

    @volatile private var started = false
    var handler: Exchange => Unit = null

    def createContext(h: Exchange => Unit) = this.synchronized {
      assert(started, "is server started?")
      handler = h
    }

    def removeContext() = this.synchronized {
      assert(started, "is server started?")
      handler = null
    }

    def start() = self.synchronized {
      started = true
      new Subscription {
        def unsubscribe() = self.synchronized {
          started = false
        }
      }
    }

    def emit(req: Request) = {
      val exchange = new DummyExchange(req)
      if (handler != null) handler(exchange)
      exchange
    }
  }

  class DummyServer(val port: Int) extends NodeScala {
    self =>
    val listeners = mutable.Map[String, DummyListener]()

    def createListener(relativePath: String) = {
      val l = new DummyListener(port, relativePath)
      listeners(relativePath) = l
      l
    }

    def emit(relativePath: String, req: Request) = this.synchronized {
      val l = listeners(relativePath)
      l.emit(req)
    }
  }
  test("Server should serve requests") {
    val dummy = new DummyServer(8191)
    val dummySubscription = dummy.start("/testDir") {
      request => for (kv <- request.iterator) yield (kv + "\n").toString
    }

    // wait until server is really installed
    Thread.sleep(500)

    def test(req: Request) {
      val webpage = dummy.emit("/testDir", req)
      val content = Await.result(webpage.loaded.future, Duration(1, SECONDS))
      val expected = (for (kv <- req.iterator) yield (kv + "\n").toString).mkString
      assert(content == expected, s"'$content' vs. '$expected'")
    }

    test(immutable.Map("StrangeRequest" -> List("Does it work?")))
    test(immutable.Map("StrangeRequest" -> List("It works!")))
    test(immutable.Map("WorksForThree" -> List("Always works. Trust me.")))

    dummySubscription.unsubscribe()
  }

}




