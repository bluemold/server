package bluemold.server

import java.net.{URL, URLClassLoader}
import bluemold.actor.{Node, FiberStrategyFactory}
import java.lang.reflect.{Modifier, InvocationTargetException}
import java.io._
import javax.xml.bind.DatatypeConverter

object Build {
  def deSerialize( value: String ): Build = {
    val bytes = DatatypeConverter.parseBase64Binary( value )
    new ObjectInputStream( new ByteArrayInputStream( bytes ) ).readObject().asInstanceOf[Build]
  }
}
case class Build( name: String, filename: String, entryClass: Option[String] = None, version: Option[Long] = None) {
  def urls = Array[URL]( new File( name  + "/" + filename ).toURI.toURL )
  def serialize: String = {
    val out = new ByteArrayOutputStream
    val objectOut = new ObjectOutputStream( out )
    objectOut.writeObject( this )
    objectOut.flush()
    DatatypeConverter.printBase64Binary( out.toByteArray )
  }
}

class Deployment( node: Node, name: String, build: Build ) {
  def urls = Array[URL]( new File( node.getStore.getFileContainer( build.name ), build.filename ).toURI.toURL )
  val classLoader = new URLClassLoader( urls, Thread.currentThread.getContextClassLoader )
  val actorStrategyFactory = new FiberStrategyFactory()(node,classLoader)
  val mainGroup = new ThreadGroup( "Deployment-"+name )
  val mainThread = new Thread( mainGroup, new MainThread )
  mainThread.setContextClassLoader( classLoader )
  mainThread.start()
  class MainThread extends Runnable {
    def run() {
      try {
        val clazz = classLoader.loadClass( build.entryClass.getOrElse( "Main" ) )
        val emptyParams = Array[String]()
        val method = clazz.getMethod( "main", classOf[Array[String]] )
        if ( Modifier.isStatic( method.getModifiers ) )
          method.invoke( null, emptyParams )
        else println( "Main method was not static" )
      } catch {
        case e: ClassNotFoundException => e.printStackTrace()
        case e: NoSuchMethodException => e.printStackTrace()
        case e: SecurityException => e.printStackTrace()
        case e: IllegalAccessException => e.printStackTrace()
        case e: IllegalArgumentException => e.printStackTrace()
        case e: InvocationTargetException =>
          e.getCause match {
            case _: ThreadDeath => println( "Killed main thread of Deployment: " + name )
            case _ => e.printStackTrace()
          }
        case e: Throwable =>
          println( "Unhandled exception caught in main thread of Deployment: " + name )
          e.printStackTrace()
      }
    }
  }
  def stop() {
    // Todo: stop should be a last resort
    mainGroup.stop()
  }
}

