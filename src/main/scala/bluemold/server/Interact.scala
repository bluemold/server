package bluemold.server

import bluemold.actor.Actor._
import bluemold.actor.{FiberStrategyFactory, UDPNode, ActorRef}


object Interact {
  def main(args: Array[String]) {
    var ok = true
    val actor = getReplActor
    print( "> " )
    while( ok ) {
      val ln = readLine()
      ok = ln != null && ln != "exit" && ln != "quit"
      if( ok ) {
        actor ! InterActor.UserRequest( ln )
      } else println( "goodbye!" )
    }
    actor.stop()
  }

  def getReplActor: ActorRef = {
    implicit val node = UDPNode.getNode( "bluemold-repl", "default" )
    implicit val strategyFactory = new FiberStrategyFactory()
    actorOf( new InterActor() ).start()
  }
}

