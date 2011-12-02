package bluemold.server

import bluemold.server.ServerActor._
import bluemold.actor._
import java.io._
import scala.math._
import bluemold.actor.Persist.{ReadBytes, WroteBytes}

object ServerActor {
  final case class ServerDownBeat() { val created = System.currentTimeMillis() }
  final case class ServerUpBeat( node: NodeIdentity ) { val created = System.currentTimeMillis() }

  abstract class ServerRequest { val id: Long; val req: String; val created = System.currentTimeMillis() }
  final case class GenericServerRequest( override val id: Long, override val req: String ) extends ServerRequest

  abstract class ServerResponse {  val id: Long; val res: String; val created = System.currentTimeMillis() }
  final case class GenericServerResponse( override val id: Long, override val res: String ) extends ServerResponse
  final case class ServerListNodesResponse( override val id: Long, override val res: String, nodes: List[NodeIdentity] ) extends ServerResponse

  final case class CreateBuildRequest( override val id: Long, override val req: String, build: Build ) extends ServerRequest
  final case class CreateBuildMeta( override val id: Long, override val req: String, build: Build, totalSize: Long, blockSize: Long, nextSetStart: Long, nextSetSize: Long ) extends ServerRequest
  final case class CreateBuildData( override val id: Long, override val req: String, build: Build, index: Long, data: Array[Byte] ) extends ServerRequest
  final case class CreateBuildReceivedQuery( override val id: Long, override val req: String, build: Build, totalSize: Long, blockSize: Long, nextSetStart: Long, nextSetSize: Long ) extends ServerRequest

  final case class CreateBuildReady( override val id: Long, override val res: String, build: Build, nextSetStart: Long ) extends ServerResponse
  final case class CreateBuildReceived( override val id: Long, override val res: String, build: Build, nextSetStart: Long ) extends ServerResponse
  final case class CreateBuildMissing( override val id: Long, override val res: String, build: Build, startIndex: Long, endIndex: Long ) extends ServerResponse
  final case class CreateBuildCompleted( override val id: Long, override val res: String, build: Build ) extends ServerResponse
}
class ServerActor extends RegisteredActor  {
  import ServerActor._

  var localNodes: List[(NodeIdentity,Long)] = _
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
    getNode.sendAll( classOf[ServerActor], ServerActor.ServerDownBeat() )
    heartBeatTimeout = onTimeout( heartBeatDelay ) { heartBeat() }
  }

  protected def react = {
    case ServerUpBeat( node ) =>
      localNodes = (node, System.currentTimeMillis()) :: ( getCurrentLocalNodes filterNot { _._1 == node } )
    case ServerDownBeat() =>
      reply( ServerUpBeat( getNode.getNodeId ) )
    case CreateBuildRequest( id, "create build", build ) =>
      actorOf( new BuildPuller( id, build, replyChannel ) ).start()
    case GenericServerRequest( id, "status" ) =>
      reply( GenericServerResponse( id, "status of " + getNode.getNodeId + " is ok" ) )
    case GenericServerRequest( id, "list nodes" ) =>
      reply( ServerListNodesResponse( id, "nodes near " + getNode.getNodeId, ( getCurrentLocalNodes map { _._1 } ).toList ) )
    case GenericServerRequest( id, "stop" ) =>
      reply( GenericServerResponse( id, "stopping " + getNode.getNodeId ) )
      onTimeout( shortDelay ) { self.stop() }
    case GenericServerRequest( id, other ) =>
      reply( GenericServerResponse( id, "I don't understand: " + other ) )
    case _ => // ignore
  }
}

class BuildPuller( id: Long, build: Build, replyChannel: ReplyChannel ) extends RegisteredActor {
  import ServerActor._

  var out: OutputStream = _
  var data: List[(Long,Array[Byte])] = _
  var totalSize: Long = _
  var blockSize: Long = _
  var totalBlocks: Long = _
  var blocksCompleted: Long = _
  var waitingForStart: Long = _
  var waitingForSize: Long = _
  var waitingFor: List[Long] = _
  protected def init() {
    out = new FileOutputStream( new File( getNode.getStore.getFileContainer( build.name ), build.filename ) )
    totalSize = -1
    blockSize = -1
    totalBlocks = -1
    blocksCompleted = -1
    waitingForStart = -1
    waitingForSize = -1
    waitingFor = null
    replyChannel issueReply GenericServerResponse( id, "begin" )
  }
  protected def react = {
    case CreateBuildMeta( rId, res, rBuild, rTotalSize, rBlockSize, rNextSetStart, rNextSetSize ) =>
//      println( "CreateBuildMeta" )
      data = Nil
      if ( totalSize == -1 ) {
        totalSize = rTotalSize
        blockSize = rBlockSize
        blocksCompleted = 0
        totalBlocks = totalSize / blockSize + ( if ( totalSize % blockSize == 0 ) 0 else 1 )
        // assert rNextSetStart == 0
        waitingForStart = rNextSetStart
        waitingForSize = rNextSetSize
        waitingFor = ( rNextSetStart until ( rNextSetStart+rNextSetSize ) ).toList
        reply( CreateBuildReady( id, "ok", build, waitingForStart ) )
      } else if ( waitingFor.isEmpty && blocksCompleted < totalBlocks ) {
        waitingForStart = rNextSetStart
        waitingFor = ( rNextSetStart until ( rNextSetStart+rNextSetSize ) ).toList
        reply( CreateBuildReady( id, "ok", build, waitingForStart ) )
      }
    case CreateBuildData( rId, res, rBuild, rIndex, rData ) if totalSize != -1 =>
//      println( "CreateBuildData" )
      if ( waitingFor contains rIndex ) {
        data ::= (rIndex,rData)
        waitingFor = waitingFor filterNot { _ == rIndex }
        if ( waitingFor.isEmpty ) {
          blocksCompleted += waitingForSize
          val datas = data sortWith { _._1 < _._1 } map { _._2 } 
          Persist.writeList( datas, out, Some(( waitingForStart, datas, 0, replyChannel )) )
          data = Nil
        }
      }
    case CreateBuildReceivedQuery( rId, res, rBuild, rTotalSize, rBlockSize, rNextSetStart, rNextSetSize ) if totalSize != -1 =>
//      println( "CreateBuildReceivedQuery" )
      if ( waitingForStart == rNextSetStart ) {
        if ( waitingFor.isEmpty ) {
          if ( data.isEmpty )
            reply( CreateBuildReceived( id, "ok", build, waitingForStart ) )
          // else be silent. We are waiting for it to save to disk. 
        } else reply( CreateBuildMissing( id, "ok", build, waitingFor.min, waitingFor.max ) )
      } else {
        // todo
      }
    case WroteBytes( true, Some(( start: Long, datas: List[Array[Byte]], count: Int, channel: ReplyChannel )) ) if start == waitingForStart =>
//      println( "WroteBytes success" )
      if ( blocksCompleted < totalBlocks )
        channel.issueReply( CreateBuildReceived( id, "ok", build, waitingForStart ) )
      else {
        out.close()
        channel.issueReply( CreateBuildCompleted( id, "ok", build ) )
        onTimeout( 1000 ) { self.stop() }
      }
    case WroteBytes( false, Some(( start: Long, datas: List[Array[Byte]], count: Int, channel: ReplyChannel )) ) if start == waitingForStart =>
//      println( "WroteBytes failed" )
      if ( count < 3 )
        onTimeout( 1000 ) {
          Persist.writeList( datas, out, Some(( start, datas, count + 1, channel )) ) 
        }
      else {
        // Fail miserably
      }
    case msg => println( "BuildPuller got: " + msg ) // ignore
  }
}

class BuildPusher( target: NodeIdentity, request: CreateBuildRequest, parent: ActorRef ) extends RegisteredActor {
  val blockConcurrency = 20L
  var in: InputStream = _
  var totalSize: Long = _
  var blockSize: Long = _
  var totalBlocks: Long = _
  var waitingForStart: Long = _
  var waitingForSize: Long = _
  val startTime = System.currentTimeMillis()
  protected def init() {
    val file = new File( request.build.filename )
    if ( file.exists() ) {
      in = new FileInputStream( file )
      totalSize = file.length()
      blockSize = 65536
      totalBlocks = totalSize / blockSize + ( if ( totalSize % blockSize == 0 ) 0 else 1 )
      waitingForStart = 0
      waitingForSize = min( blockConcurrency, totalBlocks - waitingForStart )
      getNode.sendAll( target, classOf[ServerActor], request )
    } else {
      parent ! GenericServerResponse( request.id, "build " + request.build.name + " failed to be created on " + target + ". File does not exist." )
      onTimeout( 1000 ) { self.stop() }
    }
  }
  protected def react = {
    case GenericServerResponse( id, "begin" ) if id == request.id =>
//      println( "BuildPusher got begin" )
      reply( CreateBuildMeta( id, "init", request.build, totalSize, blockSize, waitingForStart, waitingForSize ) )      
    case CreateBuildReady( id, res, build, nextSetStart ) if id == request.id =>
//      println( "CreateBuildReady" )
      if ( nextSetStart == waitingForStart ) {
        val currentSize = waitingForStart * blockSize
        val currentTime = System.currentTimeMillis()
        val seconds = ( currentTime - startTime ) / 1000
        val rate = if ( seconds > 0 ) currentSize / seconds else 0
        println( "BuildPusher: " + currentSize + " bytes in " + seconds + " seconds at " + rate + " bytes per second" )
        val bytes = new Array[Byte]( blockSize.toInt )
        Persist.read( bytes, in, Some(( waitingForStart, bytes, 0, replyChannel )) )
      } else throw new RuntimeException( "What Happened!" )
    case ReadBytes( numBytes, Some(( index: Long, bytes: Array[Byte], count: Int, channel: ReplyChannel )) ) =>
//      println( "ReadBytes" )
      // todo checks for errors like ioException?
      val bytesToSend = if ( numBytes == bytes.length ) bytes else bytes.slice( 0, numBytes )
      channel.issueReply( CreateBuildData( request.id, "data", request.build, index, bytesToSend ) )
      if ( index + 1 < waitingForStart + waitingForSize ) {
        val bytes = new Array[Byte]( blockSize.toInt )
        Persist.read( bytes, in, Some(( index + 1, bytes, 0, channel )) )
      }
    case CreateBuildReceived( id, res, build, nextSetStart ) if id == request.id =>
//      println( "CreateBuildReceived" )
      if ( nextSetStart == waitingForStart ) {
        waitingForStart = waitingForStart + waitingForSize
        waitingForSize = min( blockConcurrency, totalBlocks - waitingForStart )
        reply( CreateBuildMeta( id, "init", request.build, totalSize, blockSize, waitingForStart, waitingForSize ) )
      }
    case CreateBuildMissing( id, res, build, startIndex, endIndex ) if id == request.id =>
      println( "CreateBuildMissing: " + startIndex + " to " + endIndex  )
      // todo, oops need to support this
    case CreateBuildCompleted( id, res, build ) if id == request.id =>
//      println( "CreateBuildCompleted" )
      val endTime = System.currentTimeMillis()
      val seconds = ( endTime - startTime ) / 1000
      val rate = if ( seconds > 0 ) totalSize / seconds else 0
      parent ! GenericServerResponse( id, "build " + request.build.name + " created on " + target + " with " + totalSize + " bytes in " + seconds + " seconds at " + rate + " bytes per second" )
      onTimeout( 1000 ) { self.stop() }
    case msg =>  println( "BuildPusher got: " + msg ) // ignore
  }
}