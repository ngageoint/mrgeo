package org.mrgeo.spark.job

import java.io.PrintStream

import org.apache.spark.SparkContext

import scala.collection.mutable.ArrayBuffer

object SubmitJob {
   private[mrgeo] var printStream: PrintStream = System.err
   private[mrgeo] var exit: () => Unit = () => System.exit(-1)

   def run(job:JobArguments): Boolean =
   {
//     val sparkArgs = new ArrayBuffer[String]()
//
//     // job options must come first
//     if (job.verbose)
//     {
//       sparkArgs += "--verbose"
//     }
//
//     sparkArgs += "--name"
//     sparkArgs += job.name
//
//     sparkArgs += "--master"
//     sparkArgs += job.cluster
//
//     sparkArgs += "--class"
//     sparkArgs += job.driverClass
//
//     // comma separated list of 3rd party jars
//     if (job.jars != null && job.jars.length > 0)
//     {
//       sparkArgs += "--jars"
//       sparkArgs += job.jars
//     }
//
//     if (job.executorMem != null) {
//       sparkArgs += "--executor-memory"
//       sparkArgs += job.executorMem
//     }
//
//     if (job.driverMem != null) {
//       sparkArgs += "--driver-memory"
//       sparkArgs += job.driverMem
//     }
//
//     if (job.cores > 0) {
//       if (job.cluster == "yarn") {
//         sparkArgs += "--executor-cores"
//       }
//       else {
//         sparkArgs += "--driver-cores"
//       }
//       sparkArgs += job.cores.toString
//     }
//
//     if ((job.cluster == "yarn") && (job.executors < 2)) {
//       job.executors = 2
//     }
//
//     if (job.executors > 0) {
//       sparkArgs += "--num-executors"
//       sparkArgs += job.executors.toString
//     }
//
//     if (job.cluster == "yarn") {
//       sparkArgs += "--deploy-mode"
//       sparkArgs += "cluster"
//
//       sparkArgs += "--total-executor-cores"
//       sparkArgs += (job.cores * job.executors).toString
//     }
//
//     sparkArgs ++= addSparkConfigs
//
//
//
//     // app jar
//     sparkArgs +=  job.driverJar
//
//     // app options
//
//
//     // call SparkSubmit to kick off the job
//     //SparkSubmit.main(Array[String]{"--help"})
//     SparkSubmit.main(sparkArgs.toArray)


     val context = new SparkContext()


     true
   }

  def addSparkConfigs():ArrayBuffer[String] = {
    val configs = new ArrayBuffer[String]()

    configs += "--conf"
    configs += "spark.storage.memoryFraction=0.25"

    configs += "--conf"
    configs += "spark.serializer=org.apache.spark.serializer.KryoSerializer"

    configs += "--conf"
    configs += "spark.kryo.registrator=org.mrgeo.job.KryoRegistrar"

    configs += "--conf"
    configs += "spark.kryoserializer.buffer.mb=128"

//    configs += "--conf"
//    configs += "spark.kryo.registrationRequired=true"

//    configs += "--conf"
//    configs += "spark.executor.memory=20g"

    configs
  }
 }


