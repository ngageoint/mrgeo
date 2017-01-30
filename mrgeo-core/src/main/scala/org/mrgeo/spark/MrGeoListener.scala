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

package org.mrgeo.spark

import org.apache.spark.SparkContext
import org.apache.spark.scheduler._

//class MrGeoListener(val context:SparkContext) extends SparkFirehoseListener with Logging {
//  override def onEvent(event: SparkListenerEvent): Unit = {
//    log.info("*** " + event.getClass.getName)
//    super.onEvent(event)
//  }
//}


class MrGeoListener(val context:SparkContext) extends SparkListener {
  override def onStageCompleted(stageCompleted:SparkListenerStageCompleted):Unit = {
    println("Stage Completed " + stageCompleted)
    super.onStageCompleted(stageCompleted)
  }

  override def onStageSubmitted(stageSubmitted:SparkListenerStageSubmitted):Unit = {
    println("Stage Submitted " + stageSubmitted.stageInfo.name)

    println("  RDDs in stage")
    stageSubmitted.stageInfo.rddInfos.foreach(info => {
      println("    " + info.name)
    })

    //    val pools = context.getAllPools
    //    val rdds = context.getRDDStorageInfo
    val tracker = context.statusTracker

    tracker.getActiveJobIds().foreach(id => {
      val jobinfo = tracker.getJobInfo(id)
      println(id)
    })
    tracker.getActiveStageIds().foreach(id => {
      val stageinfo = tracker.getStageInfo(id)
      println(id)
    })
    super.onStageSubmitted(stageSubmitted)
  }

  override def onTaskStart(taskStart:SparkListenerTaskStart):Unit = {
    println("Task Start " + taskStart)

    //    val pools = context.getAllPools
    //    val rdds = context.getRDDStorageInfo
    //    val tracker = context.statusTracker

    super.onTaskStart(taskStart)
  }

  override def onTaskGettingResult(taskGettingResult:SparkListenerTaskGettingResult):Unit = {
    println("Task Get Result " + taskGettingResult)
    super.onTaskGettingResult(taskGettingResult)
  }

  override def onTaskEnd(taskEnd:SparkListenerTaskEnd):Unit = {
    println("Task End " + taskEnd)

    //    val pools = context.getAllPools
    //    val rdds = context.getRDDStorageInfo
    //    val tracker = context.statusTracker

    super.onTaskEnd(taskEnd)
  }

  override def onJobStart(jobStart:SparkListenerJobStart):Unit = {
    println("Job Start " + jobStart)
    super.onJobStart(jobStart)
  }

  override def onJobEnd(jobEnd:SparkListenerJobEnd):Unit = {
    println("Job End " + jobEnd)
    super.onJobEnd(jobEnd)
  }

  override def onEnvironmentUpdate(environmentUpdate:SparkListenerEnvironmentUpdate):Unit = {
    println("Environment Update ")
    super.onEnvironmentUpdate(environmentUpdate)
  }

  override def onBlockManagerAdded(blockManagerAdded:SparkListenerBlockManagerAdded):Unit = {
    println("BlockManager Added " + blockManagerAdded)
    super.onBlockManagerAdded(blockManagerAdded)
  }

  override def onBlockManagerRemoved(blockManagerRemoved:SparkListenerBlockManagerRemoved):Unit = {
    println("BlockManager Removed " + blockManagerRemoved)
    super.onBlockManagerRemoved(blockManagerRemoved)
  }

  override def onUnpersistRDD(unpersistRDD:SparkListenerUnpersistRDD):Unit = {
    println("UnpersistRDD " + unpersistRDD)
    super.onUnpersistRDD(unpersistRDD)
  }

  override def onApplicationStart(applicationStart:SparkListenerApplicationStart):Unit = {
    println("Application Start " + applicationStart)
    super.onApplicationStart(applicationStart)
  }

  override def onApplicationEnd(applicationEnd:SparkListenerApplicationEnd):Unit = {
    println("Application End " + applicationEnd)
    super.onApplicationEnd(applicationEnd)
  }

  override def onExecutorMetricsUpdate(executorMetricsUpdate:SparkListenerExecutorMetricsUpdate):Unit = {
    println("ExecutorMetrics Update " + executorMetricsUpdate)
    super.onExecutorMetricsUpdate(executorMetricsUpdate)
  }

  //  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
  //    println("Executor Added " + executorAdded)
  //    super.onExecutorAdded(executorAdded)
  //  }
  //
  //  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
  //    println("Executor Removed " + executorRemoved)
  //    super.onExecutorRemoved(executorRemoved)
  //  }
}
