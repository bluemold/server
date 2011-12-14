package bluemold.server

import bluemold.actor.{CancelableEvent, NodeIdentity, RegisteredActor}

object InterActor {
  final case class UserRequest( req: String )
}
class InterActor extends RegisteredActor  {
  import InterActor._
  import ServerActor._
  var outstanding: List[ServerRequest] = _
  var received: List[ServerResponse] = _
  var requestCount: Long = _
  var localNodes: List[(NodeIdentity,Long)] = _
  var lastNodeList: List[NodeIdentity] = _
  var location: Option[NodeIdentity] = _
  var heartBeatTimeout: CancelableEvent = _
  val heartBeatDelay = 1000L
  val timeoutForNodeExisting = 300000L

  protected def init() {
    outstanding = Nil
    received = Nil
    requestCount = 0
    localNodes = Nil
    lastNodeList = Nil
    location = None
    heartBeatTimeout = onTimeout( heartBeatDelay ) { heartBeat() }
  }

  def getCurrentLocalNodes = localNodes filter { _._2 > System.currentTimeMillis() - timeoutForNodeExisting }

  def heartBeat() {
    getNode.sendAll( classOf[ServerActor], ServerDownBeat() )
    heartBeatTimeout = onTimeout( heartBeatDelay ) { heartBeat() }
  }

  val ListNodes = """list nodes""".r
  val CurrentLocation = """current location""".r
  val ResetLocation = """reset location""".r
  val GotoNode = """goto node ([a-zA-Z0-9\-]+)""".r
  val CreateBuild = """create build ([a-zA-Z]+) using ([a-zA-Z0-9_\-/\\:.]+)""".r 
  val CreateDeploy = """create deploy ([a-zA-Z]+) from ([a-zA-Z]+)""".r 

  val GotoLast = """goto last""".r 
  val ShowNetworkSnapshot = """show network snapshot""".r 
  val ShowNetworkSnapshotX = """show network snapshot ([0-9])""".r 

  val expireTime = 86400000L // one day
  
  protected def react = {
    case ServerUpBeat( node ) =>
      localNodes = (node, System.currentTimeMillis()) :: ( getCurrentLocalNodes filterNot { _._1 == node } )
    case UserRequest( req ) =>
      if ( ! req.isEmpty ) {
        req match {
          case GotoLast() => gotoLastLocation()
          case ShowNetworkSnapshot() => showNetworkSnapshot( 1000L )
          case ShowNetworkSnapshotX( duration ) => showNetworkSnapshot( duration.toLong * 1000L )
          case CurrentLocation() => currentLocation()
          case ResetLocation() => resetLocation()
          case ListNodes() => listNodes()
          case GotoNode( name ) => gotoNode( name )
          case _ =>
            location match {
              case Some( node ) =>
                req match {
                  case CreateBuild( name, filename ) => createBuild( name, filename )
                  case CreateDeploy( name, build ) => createDeploy( name, build )
                  case unmatchedReq =>
                    requestCount += 1
                    val sReq = GenericServerRequest( requestCount, unmatchedReq )
                    outstanding ::= sReq
                    println( "Req["+requestCount+"]: " + unmatchedReq )
                    getNode.sendAll( node, classOf[ServerActor], sReq )
                }
              case None =>
                println( "Can't perform that request without a current location." )
            }

        }
      }
      // Handle incoming data
      if ( ! received.isEmpty ) {
        received foreach {
          res => res match {
            case GenericServerResponse( id, msg ) => println( "Res["+id+"]: " + msg )
            case ServerListNodesResponse( id, msg, nodes ) =>
              lastNodeList = nodes
              println( "Res["+id+"]: " + msg )
              nodes foreach { node => println( "Res["+id+"]: " + node ) }
          }
        }
        received = Nil
      }
      if ( ! outstanding.isEmpty ) {
        val now = System.currentTimeMillis()
        val expired = outstanding.foldLeft ( 0: Int ) { ( b, req ) => b + ( if ( req.created >= now - expireTime ) 0 else 1 ) }
        if ( expired > 0 ) {
          outstanding = outstanding filter { _.created >= now - expireTime }
          println( "Expired: " + expired )
        }
        val pending = outstanding.size
        if ( pending > 0 )
          println( "Pending: " + pending )
      }
      print( "> " )
    case res: ServerResponse =>
      if ( ! ( outstanding forall { _.id != res.id } ) ) {
        outstanding = outstanding filter { r => r.id != res.id }
        received ::= res 
      }
    case a => println( "Error - I don't understand: " + a )
  }
  
  def currentLocation() {
    print( "Current Node: " )
    location match {
      case Some( node ) => println( node + " path= " + node.path )
      case None => println( "Not Set" );
    }
  }

  def resetLocation() {
    location = None
    currentLocation()
  }

  def listNodes() {
    location match {
      case Some( node ) =>
        val req = "list nodes"
        requestCount += 1
        val sReq = GenericServerRequest( requestCount, req )
        outstanding ::= sReq
        getNode.sendAll( location.get, classOf[ServerActor], sReq )
        println( "Req["+requestCount+"]: " + req )
      case None =>
        println( "List Local Nodes: " )
        getCurrentLocalNodes foreach { entry => println( entry._1 ) }
    }
  }

  def showNetworkSnapshot( duration: Long ) {
    getNode.showNetworkSnapshot( duration )
  }
  def gotoLastLocation() {
    getNode.getStore.get( "lastLocation" ) match {
      case Some(name) =>
        println( "Attempting to go to: " + name )
        gotoNode( name )
      case None => println( "No last node" ) 
    }
  }
  def saveLastLocation() {
    location match {
      case Some( nodeIdentity ) =>
        val store = getNode.getStore
        store.put( "lastLocation", nodeIdentity.toString )
        store.flush()
      case None => // ignore
    }
  }

  def gotoNode( name: String ) {
    localNodes find { _._1.toString == name } match {
      case Some((node,time)) => location = Some(node); saveLastLocation(); currentLocation()
      case None => lastNodeList find { _.toString == name } match {
        case Some(node) => location = Some(node); saveLastLocation(); currentLocation()
        case None => println( "No node found by that name" )
      }
    }
  }

  def createBuild( name: String, filename: String ) {
    val req = "create build"
    requestCount += 1
    val build = Build( name, filename )
    val sReq = CreateBuildRequest( requestCount, req, build )
    outstanding ::= sReq
    println( "Req["+requestCount+"]: " + req + ": " + name + " = " + filename )
    actorOf( new BuildPusher( location.get, sReq, self ) ).start()
  }

  def createDeploy( name: String, build: String ) {
    println( "Create Deploy: " + name + ", build = " + build )
  }
}
