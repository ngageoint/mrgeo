package org.mrgeo.spark.job.yarn

import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.mrgeo.spark.job.{PrepareJob, JobArguments, MrGeoJob}

object MrGeoYarnJob extends Logging {

  def main(args:Array[String]): Unit = {
    logInfo("MrGeoYarnJob!!!")

    args.foreach(p => logInfo("   " + p))

    val job: JobArguments = new JobArguments(args)

    if (job.params.contains(MrGeoYarnDriver.DRIVER)) {
      val driver: String = job.params.getOrElseUpdate(MrGeoYarnDriver.DRIVER, "")

      // need to get the Yarn config by reflection, since we support non YARN setups
      val clazz = getClass.getClassLoader.loadClass(driver)
      if (clazz != null) {
        val mrgeo: MrGeoJob = clazz.newInstance().asInstanceOf[MrGeoJob]

        logInfo("Setting up job")
        mrgeo.setup(job)

        // set all the spark settings back...
        val conf = new SparkConf()

        conf.set("spark.files.overwrite", "true")

        val context = new SparkContext(conf)

        try {
          logInfo("Running job")
          mrgeo.execute(context)
        }
        finally {
          logInfo("Stopping spark context")
          context.stop()
        }

        mrgeo.teardown(job)
      }
    }
  }
}
