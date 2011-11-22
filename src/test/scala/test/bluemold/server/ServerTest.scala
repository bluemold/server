package test.bluemold.server

import junit.framework._
import bluemold.actor.Actor._
import bluemold.server.{ServerActor, Main}
import bluemold.actor._

object ServerTest {
    def suite: Test = {
        val suite = new TestSuite(classOf[ServerTest]);
        suite
    }

    def main(args : Array[String]) {
        junit.textui.TestRunner.run(suite);
    }
}

/**
 * Unit test for actor clustering.
 */
class ServerTest extends TestCase("server") {
  val tooLong = 10000 // ten seconds is too long
  def testBasics() {
    val start = System.currentTimeMillis()

    val server = Main.getServerActor
    val interActor = getInterActor

    synchronized { wait( 1000 ) } // wait one second

    interActor ! "start"

    while( interActor.isActive || server.isActive ) {
      synchronized { wait( 200 ) } // check every fifth of a second
      val now = System.currentTimeMillis()
      if ( now > start + tooLong )
        assert( false, "Test too too long. Something is wrong." )
    }
  }
  
  def getInterActor: ActorRef = {
    implicit val cluster = UDPCluster.getCluster( "bluemold-repl", "default" )
    implicit val strategyFactory = new FiberStrategyFactory()
    actorOf( new InterActor() ).start()
  }
    
  class InterActor extends RegisteredActor  {
    var outstanding: List[ServerActor.ServerRequest] = _
    var requestCount: Long = _
  
    protected def init() {
      outstanding = Nil
      requestCount = 0
    }
  
    protected def react = {
      case "start" =>
        requestCount += 1
        val sReq = ServerActor.ServerRequest( requestCount, "stop" )
        outstanding ::= sReq
        getCluster.sendAll( classOf[ServerActor], sReq )
      case res: ServerActor.GenericServerResponse =>
        if ( res.res startsWith "stopping " )
          self.stop()
      case a => println( "Error - I don't understand: " + a )  
    }
  }
}
