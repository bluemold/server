package bluemold.server

import bluemold.actor.{FiberStrategyFactory, UDPNode, ActorRef}
import bluemold.actor.Actor._


object Main {
  def main( args: Array[String] ) {
    val server = getServerActor
    while ( server.isActive ) {
      synchronized { wait( 200 ) }
    }
  }

  def getServerActor: ActorRef = {
    implicit val node = UDPNode.getNode( "bluemold", "default" )
    implicit val strategyFactory = new FiberStrategyFactory()
    actorOf( new ServerActor() ).start()
  }
}