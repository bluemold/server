package bluemold.server

import bluemold.server.ServerActor._
import bluemold.actor._
import java.io._
import scala.math._
import bluemold.actor.Persist.{ReadBytes, WroteBytes}
import bluemold.io.{AsyncWriter, AsyncReader}
import collection.immutable.HashMap

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

  final case class CreateDeployRequest( override val id: Long, override val req: String, deployName: String, buildName: String ) extends ServerRequest

  final case class UnDeployRequest( override val id: Long, override val req: String, deployName: String ) extends ServerRequest
}
class ServerActor extends RegisteredActor  {
  import ServerActor._

  var localNodes: List[(NodeIdentity,Long)] = _
  var heartBeatTimeout: CancelableEvent = _
  val shortDelay = 1000L
  val heartBeatDelay = 5000L
  val timeoutForNodeExisting = 300000L

  var builds: HashMap[String, Build] = new HashMap[String, Build]
  var deployed: HashMap[String, String] = new HashMap[String, String]
  var deployments: HashMap[String, Deployment] = new HashMap[String, Deployment]

  protected def init() {
    localNodes = Nil
    loadBuilds()
    loadDeployed()
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
      actorOf( new BuildPuller( id, build, replyChannel, self ) ).start()
    case CreateBuildCompleted( id, "ok", build ) =>
      saveBuild( build )
    case GenericServerRequest( id, "status" ) =>
      reply( GenericServerResponse( id, "status of " + getNode.getNodeId + " is ok" ) )
    case GenericServerRequest( id, "list nodes" ) =>
      reply( ServerListNodesResponse( id, "nodes near " + getNode.getNodeId, ( getCurrentLocalNodes map { _._1 } ).toList ) )
    case GenericServerRequest( id, "list builds" ) =>
      val buildList = builds.keys.foldLeft( new StringBuilder )( (sb,name) => sb.append(':').append(name) ).toString()
      reply( GenericServerResponse( id, "builds on " + getNode.getNodeId + " = " + buildList ) )
    case GenericServerRequest( id, "list deploys" ) =>
      val buildList = deployed.keys.foldLeft( new StringBuilder )( (sb,name) => sb.append(':').append(name) ).toString()
      reply( GenericServerResponse( id, "deploys on " + getNode.getNodeId + " = " + buildList ) )
    case CreateDeployRequest( id, "create deploy", deployName, buildName ) =>
      createDeploy( deployName, buildName, id, replyChannel )
    case UnDeployRequest( id, "undeploy", deployName ) =>
      undeploy( deployName, id, replyChannel )
    case GenericServerRequest( id, "stop" ) =>
      reply( GenericServerResponse( id, "stopping " + getNode.getNodeId ) )
      onTimeout( shortDelay ) { self.stop() }
    case GenericServerRequest( id, other ) =>
      reply( GenericServerResponse( id, "I don't understand: " + other ) )
    case _ => // ignore
  }

  def loadBuilds() {
    getNode.getStore.get( "builds" ) match {
      case Some(clob) =>
        clob.split('|') foreach { value: String =>
          try {
            val build = Build.deSerialize( value )
            builds += ((build.name,build))
          } catch {
            case t => t.printStackTrace() // log and ignore
          }
        }
      case None => // ignore  
    }
  }
  def saveBuild( build: Build ) {
    builds += ((build.name,build))
    val sb = new StringBuilder
    builds foreach { kv =>
      if ( sb.size > 0 ) sb.append('|')
      sb.append( kv._2.serialize )
    }
    val store = getNode.getStore
    store.put( "builds", sb.toString() )
    store.flush()
  }
  def loadDeployed() {
    getNode.getStore.get( "deployed" ) match {
      case Some(clob) =>
        clob.split('|') foreach { value: String =>
          try {
            val pair = value.split('=')
            if ( pair.length > 1 ) {
              deployed += ((pair(0),pair(1)))
              launchDeployment(pair(0),pair(1))
            }
          } catch {
            case t => t.printStackTrace() // log and ignore
          }
        }
      case None => // ignore  
    }
  }
  private def saveDeploy() {
    val sb = new StringBuilder
    deployed foreach { kv =>
      if ( sb.size > 0 ) sb.append('|')
      sb.append( kv._1 ).append('=').append(kv._2)
    }
    val store = getNode.getStore
    store.put( "deployed", sb.toString() )
    store.flush()
  }
  def addDeploy( deployName: String, buildName: String ) {
    deployed += ((deployName,buildName))
    saveDeploy()
  }
  def removeDeploy( deployName: String ) {
    deployed -= deployName
    saveDeploy()
  }
  def launchDeployment( deployName: String, buildName: String ) {
    val ( matching, remaining ) = deployments partition { _._1 == deployName }
    matching foreach { _._2.stop() }
    deployments = remaining
    builds get buildName foreach { build =>
      deployments += ((deployName,new Deployment(getNode,deployName,build)))
    }
  }
  def unLaunchDeployment( deployName: String ) {
    val ( matching, remaining ) = deployments partition { _._1 == deployName }
    matching foreach { _._2.stop() }
    deployments = remaining
  }
  def createDeploy( deployName: String, buildName: String, id: Long, replyChannel: ReplyChannel ) {
    var success = false
    try {
      launchDeployment( deployName, buildName )
      success = true
    } catch {
      case t => t.printStackTrace() // failed to launch
    }
    if ( success ) {
      addDeploy(deployName,buildName)
      replyChannel.issueReply( GenericServerResponse( id, "created deploy '" + deployName + "' of build '" + buildName + "' on " + getNode.getNodeId ) )
    } else {
      replyChannel.issueReply( GenericServerResponse( id, "failed to create deploy '" + deployName + "' of build '" + buildName + "' on " + getNode.getNodeId ) )
    }
  }
  def undeploy( deployName: String, id: Long, replyChannel: ReplyChannel ) {
    var success = false
    try {
      unLaunchDeployment( deployName )
      success = true
    } catch {
      case t => t.printStackTrace() // failed to launch
    }
    if ( success ) {
      removeDeploy(deployName)
      replyChannel.issueReply( GenericServerResponse( id, "removed deploy '" + deployName + "' on " + getNode.getNodeId ) )
    } else {
      replyChannel.issueReply( GenericServerResponse( id, "failed to removed deploy '" + deployName + "' on " + getNode.getNodeId ) )
    }
  }
}

class BuildPuller( id: Long, build: Build, replyChannel: ReplyChannel, parentServer: ActorRef ) extends RegisteredActor {
  import ServerActor._

  var out: AsyncWriter = _
  var data: List[(Long,Array[Byte])] = _
  var totalSize: Long = _
  var blockSize: Long = _
  var totalBlocks: Long = _
  var blocksCompleted: Long = _
  var waitingForStart: Long = _
  var waitingForSize: Long = _
  var waitingFor: List[Long] = _
  var waitingForWrite: List[Long] = _
  protected def init() {
    val file = new File( getNode.getStore.getFileContainer( build.name ), build.filename )
    out = Persist.getAsyncWriter( file )
    totalSize = -1
    blockSize = -1
    totalBlocks = -1
    blocksCompleted = -1
    waitingForStart = -1
    waitingForSize = -1
    waitingFor = null
    waitingForWrite = Nil
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
//      println( "CreateBuildData: " + rIndex )
      if ( waitingFor contains rIndex ) {
        waitingFor = waitingFor filterNot { _ == rIndex }
        waitingForWrite ::= rIndex
        Persist.writeAsync( rData, rIndex * blockSize, out, Some(( rIndex, rData, 0, replyChannel )) )
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
    case WroteBytes( true, Some(( index: Long, data: Array[Byte], count: Int, channel: ReplyChannel )) ) if index >= waitingForStart && index < waitingForStart + waitingForSize =>
//      println( "WroteBytes success: " + index )
      if ( waitingForWrite contains index ) {
        waitingForWrite = waitingForWrite filterNot { _ == index }
      }      
      
      if ( waitingFor.isEmpty && waitingForWrite.isEmpty ) {
        blocksCompleted += waitingForSize
//        println( "blocksCompleted: " + blocksCompleted )
        if ( blocksCompleted < totalBlocks )
          channel.issueReply( CreateBuildReceived( id, "ok", build, waitingForStart ) )
        else {
          out.close()
          parentServer ! CreateBuildCompleted( id, "ok", build )
          channel.issueReply( CreateBuildCompleted( id, "ok", build ) )
          onTimeout( 1000 ) { self.stop() }
        }
      } // else println( "waitingFor: " + waitingFor + " waitingForWrite: " + waitingForWrite )

    case WroteBytes( false, Some(( index: Long, data: Array[Byte], count: Int, channel: ReplyChannel )) ) if index >= waitingForStart && index < waitingForStart + waitingForSize =>
//      println( "WroteBytes failed" )
      if ( count < 3 )
        onTimeout( 100 ) {
          Persist.writeAsync( data, index * blockSize, out, Some(( index, data, count + 1, channel )) ) 
        }
      else {
        // Fail miserably
        throw new RuntimeException( "What Happened!" )
      }
    case msg => println( "BuildPuller got: " + msg ) // ignore
  }
}

class BuildPusher( target: NodeIdentity, request: CreateBuildRequest, parent: ActorRef ) extends RegisteredActor {
  val blockConcurrency = 20L
  var in: AsyncReader = _
  var totalSize: Long = _
  var blockSize: Long = _
  var totalBlocks: Long = _
  var waitingForStart: Long = _
  var waitingForSize: Long = _
  val startTime = System.currentTimeMillis()

  val eventTimeout = 20000L
  var waitingForEvent: CancelableEvent = _
  protected def init() {
    val file = new File( request.build.filename )
    if ( file.exists() ) {
      in = Persist.getAsyncReader( file )
      totalSize = file.length()
      if ( totalSize < 1024 * 1024 ) blockSize = 65536
      else blockSize = 65536 * 4
      totalBlocks = totalSize / blockSize + ( if ( totalSize % blockSize == 0 ) 0 else 1 )
      waitingForStart = 0
      waitingForSize = min( blockConcurrency, totalBlocks - waitingForStart )
      getNode.sendAll( target, classOf[ServerActor], request )
    } else {
      parent ! GenericServerResponse( request.id, "build " + request.build.name + " failed to be created on " + target + ". File does not exist." )
      onTimeout( 1000 ) { self.stop() }
    }
  }
  private def replyWithTimeout( name: String )( msg: Any ) {
    val channel = replyChannel
    def replyWithMessage() {
      channel.issueReply( msg )
      waitingForEvent = onTimeout( eventTimeout ) {
        println( name + " timed out" )
        replyWithMessage()
      }
    }
    replyWithMessage()
  }
  private def doWithTimeout( name: String )( body: => Unit ) {
    def doBody() {
      body
      waitingForEvent = onTimeout( eventTimeout ) {
        println( name + " timed out" )
        doBody()
      }
    }
    doBody()
  }
  protected def react = {
    case GenericServerResponse( id, "begin" ) if id == request.id =>
//      println( "BuildPusher got begin" )
      replyWithTimeout( "initial CreateBuildMeta")( CreateBuildMeta( id, "init", request.build, totalSize, blockSize, waitingForStart, waitingForSize ) )
    case CreateBuildReady( id, res, build, nextSetStart ) if id == request.id =>
//      println( "CreateBuildReady" )
      if ( nextSetStart == waitingForStart ) {
        waitingForEvent.cancel()
        doWithTimeout( "Read in from file" ) {
          val currentSize = waitingForStart * blockSize
          val currentTime = System.currentTimeMillis()
          val seconds = ( currentTime - startTime ) / 1000
          val rate = if ( seconds > 0 ) currentSize / seconds else 0
          println( "BuildPusher: " + currentSize + " bytes in " + seconds + " seconds at " + rate + " bytes per second" )
          ( waitingForStart until (waitingForStart + waitingForSize ) ) foreach { index =>
            val bytes = new Array[Byte]( blockSize.toInt )
            Persist.readAsync( bytes, index * blockSize, in, Some(( index, bytes, 0, replyChannel )) )
          }
        }
      } else throw new RuntimeException( "What Happened!" )
    case ReadBytes( numBytes, Some(( index: Long, bytes: Array[Byte], count: Int, channel: ReplyChannel )) ) =>
//      println( "ReadBytes: " + index )
      // todo checks for errors like ioException?
      // timeout in place in case this message is lost
      if ( numBytes == 0 ) {
        println( "Read returned 0 bytes?" )
      } else if ( numBytes > 0 ) {
        val bytesToSend = if ( numBytes == bytes.length ) bytes else bytes.slice( 0, numBytes )
        channel.issueReply( CreateBuildData( request.id, "data", request.build, index, bytesToSend ) )
      } else {
        println( "Read returned negative bytes?" )
      }
    case CreateBuildReceived( id, res, build, nextSetStart ) if id == request.id =>
//      println( "CreateBuildReceived" )
      if ( nextSetStart == waitingForStart ) {
        waitingForEvent.cancel()
        waitingForStart = waitingForStart + waitingForSize
        waitingForSize = min( blockConcurrency, totalBlocks - waitingForStart )
        replyWithTimeout( "next CreateBuildMeta[" + waitingForStart + "]" ) {
          CreateBuildMeta( id, "init", request.build, totalSize, blockSize, waitingForStart, waitingForSize )
        }
      } else throw new RuntimeException( "What Happened!" )
    case CreateBuildMissing( id, res, build, startIndex, endIndex ) if id == request.id =>
      println( "CreateBuildMissing: " + startIndex + " to " + endIndex  )
      // todo, oops need to support this
    case CreateBuildCompleted( id, res, build ) if id == request.id =>
//      println( "CreateBuildCompleted" )
      waitingForEvent.cancel()
      val endTime = System.currentTimeMillis()
      val seconds = ( endTime - startTime ) / 1000
      val rate = if ( seconds > 0 ) totalSize / seconds else 0
      parent ! GenericServerResponse( id, "build " + request.build.name + " created on " + target + " with " + totalSize + " bytes in " + seconds + " seconds at " + rate + " bytes per second" )
      onTimeout( 1000 ) { self.stop() }
    case msg =>  println( "BuildPusher got: " + msg ) // ignore
  }
}
