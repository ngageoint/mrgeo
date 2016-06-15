/*
 * Copyright 2009-2016 DigitalGlobe, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 *
 */

package org.mrgeo.job.yarn

import java.io.{BufferedReader, InputStreamReader}

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.apache.hadoop.fs.Path
import org.apache.spark.{Logging, SparkContext}
import org.mrgeo.hdfs.utils.HadoopFileUtils
import org.mrgeo.job.{JobArguments, MrGeoJob}
import org.mrgeo.utils.SparkUtils

@SuppressFBWarnings(value=Array("ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD"), justification = "Scala generated code")
object MrGeoYarnJob extends Logging {

  def main(args:Array[String]): Unit = {
    logInfo("Running a MrGeoYarnJob!")

    logInfo("Job Arguments: ")
    args.foreach(p => logInfo("   " + p))

    val job: JobArguments = new JobArguments(args)

    // if we have an argfile, read the parameters and put them into the job
    if (job.hasSetting(MrGeoYarnDriver.ARGFILE))
    {
      val filename = new Path(job.getSetting(MrGeoYarnDriver.ARGFILE))

      val stream = HadoopFileUtils.open(filename)
      try
      {
        val isr = new InputStreamReader(stream)
        try {
          val input = new BufferedReader(isr)
          try {
            var key: String = ""
            var value: String = ""

            key = input.readLine()
            value = input.readLine()

            while (key != null && value != null) {
              key = key.replaceFirst("^--", "") // strip initial "--"
              if (value.startsWith("--")) {
                // The key is an on/off switch because the value is not really
                // a value, so continue parsing with the value

                job.setSetting(key, null)
                key = value.replaceFirst("^--", "") // strip initial "--"
                value = input.readLine()
                while (value != null && value.startsWith("--")) {
                  job.setSetting(key, null)
                  key = value.replaceFirst("^--", "") // strip initial "--"
                  value = input.readLine()
                }
              }
              else {
                job.setSetting(key, value)
              }

              key = input.readLine()
              value = input.readLine()
            }

            if (key != null) {
              job.setSetting(key, null)
            }

            job.params -= MrGeoYarnDriver.ARGFILE

            logInfo("*******************")
            logInfo("Arguments")
            job.params.foreach(kv => {
              logInfo("  " + kv._1 + ": " + kv._2)
            })
            logInfo("*******************")
          }
          finally {
            input.close()
          }
        }
        finally
        {
          isr.close()
        }
      }
      finally
      {
        if (stream != null) {
          stream.close()
        }
      }
      HadoopFileUtils.delete(filename)

    }

    if (job.params.contains(MrGeoYarnDriver.DRIVER)) {
      val driver: String = job.params.getOrElseUpdate(MrGeoYarnDriver.DRIVER, "")

      val clazz = getClass.getClassLoader.loadClass(driver)
      if (clazz != null) {
        logInfo("Found MrGeo driver: " + driver)
        val mrgeo: MrGeoJob = clazz.newInstance().asInstanceOf[MrGeoJob]

        // set all the spark settings back...
        val conf = SparkUtils.getConfiguration

        // need to do this here, so we can call registerClasses() on the job.
        MrGeoJob.setupSerializer(mrgeo, conf)

        logInfo("Setting up job: " + job.name)
        if (mrgeo.setup(job, conf)) {
          logInfo("SparkConf parameters")
          conf.getAll.foreach(kv => {logDebug("  " + kv._1 + ": " + kv._2)})

          val context = new SparkContext(conf)

          //context.addSparkListener(new MrGeoListener)
          val checkpointDir = HadoopFileUtils.createJobTmp(context.hadoopConfiguration).toString
          try {
            logInfo("Running job: " + job.name)
            context.setCheckpointDir(checkpointDir)
            if (!mrgeo.execute(context)) {
              logError("Error in execute")
            }
          }
          finally {
            logInfo("Stopping spark context")
            context.stop()

            HadoopFileUtils.delete(context.hadoopConfiguration, checkpointDir)
          }

          logInfo("Teardown job: " + job.name)
          if (!mrgeo.teardown(job, conf)) {
            logError("Error in teardown")
          }
        }
        else {
          logError("Error in setup")
        }
      }
    }
  }
}
