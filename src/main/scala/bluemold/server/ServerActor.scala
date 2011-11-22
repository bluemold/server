package bluemold.server

import bluemold.actor.{CancelableEvent, ClusterIdentity, RegisteredActor}


object ServerActor {
  final case class ServerDownBeat() { val created = System.currentTimeMillis() }
  final case class ServerUpBeat( node: ClusterIdentity ) { val created = System.currentTimeMillis() }
  final case class ServerRequest( id: Long, req: String ) { val created = System.currentTimeMillis() }
  case class ServerResponse( id: Long, res: String ) { val created = System.currentTimeMillis() }
  final case class GenericServerResponse( override val id: Long, override val res: String ) extends ServerResponse( id, res )
  final case class ServerListNodesResponse( override val id: Long, override val res: String, nodes: List[ClusterIdentity] ) extends ServerResponse( id, res )
}
class ServerActor extends RegisteredActor  {
  import ServerActor._

  var localNodes: List[(ClusterIdentity,Long)] = _
  var heartBeatTimeout: CancelableEvent = _
  val shortDelay = 1000L
  val heartBeatDelay = 5000L
  val timeoutForNodeExisting = 300000L

  protected def init() {
    localNodes = Nil
    heartBeatTimeout = onTimeout( shortDelay ) { heartBeat() }
  }

  def getCurrentLocalNodes = localNodes filter { _._2 > System.currentTimeMillis() - timeoutForNodeExisting }

  def heartBeat() {
    getCluster.sendAll( classOf[ServerActor], ServerActor.ServerDownBeat() )
    heartBeatTimeout = onTimeout( heartBeatDelay ) { heartBeat() }
  }

  protected def react = myReact
  protected val myReact: PartialFunction[Any, Unit] = {
    case ServerUpBeat( node ) =>
      localNodes = (node, System.currentTimeMillis()) :: ( getCurrentLocalNodes filterNot { _._1 == node } )
    case ServerDownBeat() =>
      reply( ServerUpBeat( getCluster.getClusterId ) )
    case ServerRequest( id, "status" ) =>
      reply( GenericServerResponse( id, "status of " + getCluster.getClusterId + " is ok" ) )
    case ServerRequest( id, "list nodes" ) =>
      reply( ServerListNodesResponse( id, "nodes near " + getCluster.getClusterId, ( getCurrentLocalNodes map { _._1 } ).toList ) )
    case ServerRequest( id, "stop" ) =>
      reply( GenericServerResponse( id, "stopping " + getCluster.getClusterId ) )
      onTimeout( shortDelay ) { self.stop() }
    case ServerRequest( id, other ) =>
      reply( GenericServerResponse( id, "I don't understand: " + other ) )
  }
}