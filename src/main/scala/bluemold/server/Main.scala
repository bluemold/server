package bluemold.server

import bluemold.actor.{FiberStrategyFactory, UDPCluster, ActorRef}
import bluemold.actor.Actor._


object Main {
  def main( args: Array[String] ) {
    val server = getServerActor
    while ( server.isActive ) {
      synchronized { wait( 200 ) }
    }
  }

  def getServerActor: ActorRef = {
    implicit val cluster = UDPCluster.getCluster( "bluemold", "default" )
    implicit val strategyFactory = new FiberStrategyFactory()
    actorOf( new ServerActor() ).start()
  }
}