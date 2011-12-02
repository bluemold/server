package bluemold.server

import java.net.{URL, URLClassLoader}
import java.io.File
import bluemold.actor.{Node, FiberStrategyFactory}
import java.lang.reflect.{Modifier, InvocationTargetException}

case class Build( name: String, filename: String, entryClass: Option[String] = None, version: Option[Long] = None) {
  def urls = Array[URL]( new File( name + "/" + filename ).toURI.toURL )
}

case class DeploymentDescriptor( name: String, build: Build )

class Deployment( node: Node, descriptor: DeploymentDescriptor ) {
  val classLoader = new URLClassLoader( descriptor.build.urls, Thread.currentThread.getContextClassLoader )
  val actorStrategyFactory = new FiberStrategyFactory()(node,classLoader)
  val mainThread = new Thread( new MainThread )
  mainThread.setContextClassLoader( classLoader )
  mainThread.start()
  class MainThread extends Runnable {
    def run() {
      try {
        val clazz = classLoader.loadClass( descriptor.build.entryClass.getOrElse( "Main" ) )
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
        case e: InvocationTargetException => e.printStackTrace()
        case e: Throwable =>
          println( "Unhandled exception caught in main thread of Deployment: " + descriptor.name )
          e.printStackTrace()
      }
    }
  }
}

