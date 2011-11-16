package bluemold.server

import bluemold.actor.RegisteredActor

object InterActor {
  final case class UserRequest( req: String )
}
class InterActor extends RegisteredActor  {
  import InterActor._
  var outstanding: List[ServerActor.ServerRequest] = _
  var received: List[ServerActor.ServerResponse] = _
  var requestCount: Long = _

  protected def init() {
    outstanding = Nil
    received = Nil
    requestCount = 0
  }

  protected def react = {
    case UserRequest( req ) =>
      if ( ! req.isEmpty ) {
        requestCount += 1
        val sReq = ServerActor.ServerRequest( requestCount, req )
        outstanding ::= sReq
        println( "Req["+requestCount+"]: " + req )
        getCluster.sendAll( classOf[ServerActor].getName, sReq )
      }
      if ( ! received.isEmpty ) {
        received foreach { res => 
          println( "Res["+res.id+"]: " + res.res )
        }
        received = Nil
      }
      if ( ! outstanding.isEmpty ) {
        val now = System.currentTimeMillis()
        val expired = outstanding.foldLeft ( 0: Int ) { ( b, req ) => b + ( if ( req.created >= now - 10000 ) 0 else 1 ) }
        if ( expired > 0 ) {
          outstanding = outstanding filter { _.created >= now - 10000 }
          println( "Expired: " + expired )
        }
        val pending = outstanding.size
        if ( pending > 0 )
          println( "Pending: " + pending )
      }
      print( "> " )
    case res: ServerActor.ServerResponse =>
      if ( ! ( outstanding forall { _.id != res.id } ) ) {
        outstanding = outstanding filter { r => r.id != res.id }
        received ::= res 
      }
    case a => println( "Error - I don't understand: " + a )  
  }
}