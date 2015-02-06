package org.mrgeo.job

import java.io.{OutputStream, InputStream, File}
import java.net.URL
import java.nio.ByteBuffer

import org.apache.hadoop.util.ClassUtil
import org.apache.spark.serializer.{SerializationStream, DeserializationStream, SerializerInstance}
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.mrgeo.utils.DependencyLoader
import scala.collection.JavaConversions._
import scala.reflect.ClassTag
import scala.tools.nsc.util.ScalaClassLoader.URLClassLoader
import scala.util.control.Breaks

abstract class MrGeoDriver {

  def run(args:Array[String] = new Array[String](0)) = {
    val job = new JobArguments(args)

    val classname = getClass.getName
    if (classname.endsWith("$")) {
      job.driverClass = classname.substring(0, classname.lastIndexOf("$"))
    }
    else {
      job.driverClass = classname
    }

    println("Configuring application")

    setupDependencies(job)


    var parentLoader:ClassLoader = Thread.currentThread().getContextClassLoader
    if (parentLoader == null) {
      parentLoader = getClass.getClassLoader
    }

    val urls = job.jars.map(jar => new File(jar).toURI.toURL)
    val cl = new URLClassLoader(urls.toSeq, parentLoader)

    if (job.isYarn) {
      addYarnClasses(cl)
    }
    val jobclass = cl.loadClass(job.driverClass)

    val jobinstance = jobclass.newInstance().asInstanceOf[MrGeoJob]
    jobinstance.run(job.toArgArray)
  }

  private def addYarnClasses(cl: URLClassLoader) = {
    // need to do this by reflection, since we may support non YARN setups

    throw new NotImplementedError("This method needs to be implemented as reflection!")

//    val conf:YarnConfiguration = new YarnConfiguration
//
//    // get the yarn classpath from the configuration...
//    var cp = conf.get(YarnConfiguration.YARN_APPLICATION_CLASSPATH, "")
//
//    // replace any variables $<xxx> with their environmental value
//    val envMap = System.getenv()
//    for ( entry <- envMap.entrySet()) {
//      val key = entry.getKey
//      val value = entry.getValue
//
//      cp = cp.replaceAll("\\$" + key, value)
//    }
//
//    // add the urls to the classloader
//    for (str <- cp.split(",")) {
//      val url = new File(str.trim).toURI.toURL
//      cl.addURL(url)
//    }
  }

  protected def setupDependencies(job:JobArguments): Unit = {

    val dependencies = DependencyLoader.getDependencies(getClass)
    val jars:StringBuilder = new StringBuilder

    for (jar <- dependencies) {
      // spark-yarn is automatically included in yarn jobs, and adding it here conflicts...
      //if (!jar.contains("spark-yarn")) {
      if (jars.length > 0) {
        jars ++= ","
      }
      jars ++= jar
      //}
    }
    job.setJars(jars.toString())

    job.driverJar = getClass.getProtectionDomain.getCodeSource.getLocation.getPath

    if (!job.driverJar.endsWith(".jar")) {

      val jar: String = ClassUtil.findContainingJar(getClass)

      if (jar != null) {
        job.driverJar = jar
      }
      else {
        // now the hard part, need to look in the dependencies...

        //val urls:Array[URL] = Array.ofDim(dependencies.size())
        val urls = dependencies.map(jar => new File(jar).toURI.toURL)

        val cl = new URLClassLoader(urls.toSeq, Thread.currentThread().getContextClassLoader)

        val classFile: String = getClass.getName.replaceAll("\\.", "/") + ".class"
        val iter =  cl.getResources(classFile)

        val break = new Breaks

        break.breakable(
          while (iter.hasMoreElements) {
            val url: URL = iter.nextElement
            if (url.getProtocol == "jar") {
              val path: String = url.getPath
              if (path.startsWith("file:")) {
                // strip off the "file:" and "!<classname>"
                job.driverJar = path.substring("file:".length).replaceAll("!.*$", "")

                break.break()
              }
            }
          })
      }

    }



  }

}

class MySerializer(conf: SparkConf)
    extends org.apache.spark.serializer.Serializer
            with Logging
            with Serializable {

  println("***** Serializer *****")

  val cl = this.getClass.getClassLoader
  val rs = cl.loadClass("org.mrgeo.data.tile.TileIdWritable")

  println("--- this.getClass.getClassLoader")
  println(if (rs == null)  "null" else rs.getCanonicalName)

  val tl = Thread.currentThread().getContextClassLoader
  //    for (r <- sl.asInstanceOf[URLClassLoader].getURLs) {
  //      println(r)
  //    }
  val ts = tl.loadClass("org.mrgeo.data.tile.TileIdWritable")

  println("---Thread.currentThread().getContextClassLoader")
  println(if (ts == null)  "null" else ts.getCanonicalName)

  val sl = ClassLoader.getSystemClassLoader
  //    for (r <- sl.asInstanceOf[URLClassLoader].getURLs) {
  //      println(r)
  //    }
  println("---ClassLoader.getSystemClassLoader")
  val ss = sl.loadClass("org.mrgeo.data.tile.TileIdWritable")

  println(if (ss == null)  "null" else ss.getCanonicalName)
  println("---")

  override def newInstance(): SerializerInstance = {
    new MySerializerInstance(this)
  }
}

private[job] class MySerializerInstance(ks: MySerializer) extends SerializerInstance {
  override def serialize[T](t: T)(implicit evidence$1: ClassTag[T]): ByteBuffer = { null }

  override def serializeStream(s: OutputStream): SerializationStream = { null }

  override def deserializeStream(s: InputStream): DeserializationStream = { null }

  override def deserialize[T](bytes: ByteBuffer)(implicit evidence$2: ClassTag[T]): T = { null.asInstanceOf[T] }

  override def deserialize[T](bytes: ByteBuffer, loader: ClassLoader)(implicit evidence$3: ClassTag[T]): T = { null.asInstanceOf[T]  }
}


abstract class MrGeoJob  {
  def registerClasses(): Array[Class[_]]
  def setup(job:JobArguments): Boolean
  def execute(context:SparkContext):Boolean
  def teardown(job:JobArguments): Boolean

  private[job] def run(args:Array[String] = new Array[String](0)) = {
    val job = new JobArguments(args)


    println("Configuring application")
    setup(job)

    val context = prepareJob(job)
    try {
      println("Running application")
      execute(context)
    }
    finally {
      println("Stopping spark context")
      context.stop()
    }

    teardown(job)
  }


  final private def prepareJob(job:JobArguments): SparkContext = {

    throw new NotImplementedError("Need to fix registerKryoClasses, which isn't in pre spark 1.2.0")

    val conf:SparkConf = new SparkConf()
        .setAppName(job.name)
        .setMaster(job.cluster)
        .setJars(job.jars)
        //.registerKryoClasses(registerClasses())
        //    .set("spark.driver.extraClassPath", "")
        //    .set("spark.driver.extraJavaOptions", "")
        //    .set("spark.driver.extraLibraryPath", "")

        .set("spark.yarn.preserve.staging.files", "true")
        .set("spark.storage.memoryFraction","0.25")

        // setup the kryo serializer
        //.set("spark.serializer","org.mrgeo.job.MySerializer")
        //.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
        //.set("spark.kryo.registrator","org.mrgeo.job.KryoRegistrar")
        //.set("spark.kryoserializer.buffer.mb","128")


        // driver memory and cores
        .set("spark.driver.memory", if (job.driverMem != null) job.driverMem else "128m")
        .set("spark.driver.cores", if (job.cores > 0) job.cores.toString else "1")

    if (job.isYarn) {
      //conf.set("spark.yarn.jar","")
      conf.set("spark.executor.memory", if (job.executorMem != null) job.executorMem else "128m")
          .set("spark.cores.max", if (job.executors > 0) job.executors.toString else "1")
          // running in "client" mode, the driver runs outside of YARN, but submits tasks
          .setMaster(job.YARN + "-client")

      System.setProperty("SPARK_YARN_MODE", "true")

      val sb = new StringBuilder

      job.jars.foreach(jar => {
        sb ++= jar
        sb += ','
      })

      //conf.set("spark.yarn.dist.files", sb.substring(0, sb.length - 1))

      // need to initialize the ApplicationMaster
      //      val appargs = new ArrayBuffer[String]()
      //
      //      appargs += "--jar"
      //      appargs += job.driverJar
      //
      //      appargs += "--class"
      //      appargs += job.driverClass
      //
      //      appargs +=
      //ApplicationMaster.main(appargs.toArray)
      //      ApplicationMaster.main(job.toArgArray)
    }
    else if (job.isSpark) {
      conf.set("spark.driver.memory", if (job.driverMem != null) job.driverMem else "128m")
          .set("spark.driver.cores", if (job.cores > 0) job.cores.toString else "1")
    }

    val context = new SparkContext(conf)

    //    job.jars.foreach(jar => {
    //      println(jar)
    //      context.addJar(jar)})

    context
  }

  //  final def main(args:Array[String]): Unit = {
  //    val job:JobArguments = new JobArguments(args)
  //
  //    println("Starting application")
  //
  //    val conf:SparkConf = new SparkConf()
  //        .setAppName(System.getProperty("spark.app.name", "generic-spark-app"))
  //        .setMaster(System.getProperty("spark.master", "local[1]"))
  //    //        .setJars(System.getProperty("spark.jars", "").split(","))
  //
  //    val context:SparkContext = new SparkContext(conf)
  //    try
  //    {
  //      execute(context)
  //    }
  //    finally
  //    {
  //      println("Stopping spark context")
  //      context.stop()
  //    }
  //  }

}
