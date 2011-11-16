package bluemold.server

import bluemold.actor.RegisteredActor

object ServerActor {
  final case class ServerRequest( id: Long, req: String ) { val created = System.currentTimeMillis() }
  final case class ServerResponse( id: Long,  res: String ) { val created = System.currentTimeMillis() }
}
class ServerActor extends RegisteredActor  {
  import ServerActor._
  protected def init() {}

  protected def react = {
    case ServerRequest( id, "status" ) =>
      reply( ServerResponse( id, "ok" ) )
    case ServerRequest( id, "stop" ) =>
      reply( ServerResponse( id, "stopping..." ) )
      onTimeout( 1000 ) { self.stop() }
    case ServerRequest( id, other ) =>
      reply( ServerResponse( id, "I don't understand: " + other ) )
  }
}