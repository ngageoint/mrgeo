/*
 * Copyright 2009-2015 DigitalGlobe, Inc.
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
 */

package org.mrgeo.mapalgebra;

import org.apache.hadoop.conf.Configuration;
import org.mrgeo.aggregators.MeanAggregator;
import org.mrgeo.buildpyramid.BuildPyramid;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.mapalgebra.old.MapOpHadoop;
import org.mrgeo.mapalgebra.old.RasterMapOpHadoop;
import org.mrgeo.mapreduce.job.JobCancelFailedException;
import org.mrgeo.mapreduce.job.JobListener;
import org.mrgeo.mapreduce.job.RunnableJob;
import org.mrgeo.mapreduce.job.TaskProgress;
import org.mrgeo.progress.Progress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapAlgebraJob implements RunnableJob
{
  private static final Logger _log = LoggerFactory.getLogger(MapAlgebraJob.class);
  String _expression;
  String _output;
  Progress _progress;
  JobListener jobListener = null;
  private String protectionLevel;
  private ProviderProperties providerProperties;

  public MapAlgebraJob(String expression, String output,
      final String protectionLevel,
      final ProviderProperties providerProperties)
  {
    _expression = expression;
    _output = output;
    this.protectionLevel = protectionLevel;
    this.providerProperties = providerProperties;
  }
  
  @Override
  public void setProgress(Progress p) {
    _progress = p;
  }
  
  @Override
  public void run()
  {
    _progress.starting();
    _progress.failed("Need to reimplement MapAlgebraJob.run()");

//    try
//    {
//      _progress.starting();
//      OpImageRegistrar.registerMrGeoOps();
//
//      Configuration conf = HadoopUtils.createConfiguration();
//      MrsImageDataProvider dp = DataProviderFactory.getMrsImageDataProvider(_output, AccessMode.OVERWRITE, conf);
//      String useProtectionLevel = ProtectionLevelUtils.getAndValidateProtectionLevel(dp, protectionLevel);
//      MapAlgebraParser parser = new MapAlgebraParser(conf, useProtectionLevel,
//          providerProperties);
//      MapOpHadoop op = null;
//      try
//      {
//        op = parser.parse(_expression);
//      }
//      catch (ParserException e)
//      {
//        throw new IOException(e);
//      }
//
//      ProgressHierarchy ph = new ProgressHierarchy(_progress);
//      ph.starting();
//      ph.createChild(2.0f);
//      ph.createChild(1.0f);
//      MapAlgebraExecutioner exec = new MapAlgebraExecutioner();
//      exec.setJobListener(jobListener);
//      exec.setOutputName(_output);
//      exec.setRoot(op);
//
//      //do not build pyramids now
//      exec.execute(conf, ph.getChild(0), false);
//
//      if (_progress != null && _progress.isFailed())
//      {
//        throw new JobFailedException(_progress.getResult());
//      }
//
//      ph.complete();
//      _progress.complete();
//
//      //Build pyramid is a post processing step and should happen
//      //at the end, the job should be marked complete, but the
//      //build pyramid processing will still go on.
//      buildPyramid(op, conf, providerProperties);
//    }
//    catch (JobCancelledException j)
//    {
//      _log.error("JobCancelledException occurred while processing mapalgebra job " + j.getMessage(), j);
//      _progress.cancelled();
//      cancel();
//    }
//    catch (JobFailedException j) {
//      _log.error("JobFailedException occurred while processing mapalgebra job " + j.getMessage(), j);
//      _progress.failed(j.getMessage());
//    }
//    catch (Exception e)
//    {
//      _log.error("Exception occurred while processing mapalgebra job " + e.getMessage(), e);
//      _progress.failed(e.getMessage());
//    }
//    catch (Throwable e)
//    {
//      _log.error("Throwable error occurred while processing mapalgebra job " + e.getMessage(), e);
//      _progress.failed(e.getMessage());
//    }
  }
  
  private void buildPyramid(MapOpHadoop op, Configuration conf,
                            ProviderProperties providerProperties) throws Exception
  {
    TaskProgress taskProg = new TaskProgress(_progress);
    try {
      if (op instanceof RasterMapOpHadoop)
      {        
        taskProg.starting();
        BuildPyramid.build(_output, new MeanAggregator(),
            conf, taskProg, null, providerProperties);
        taskProg.complete();
      }
      else
      {
        taskProg.notExecuted();
      }
    }
    catch (Exception e)
    {
      e.printStackTrace();
      _log.error("Exception occurred while processing mapalgebra job " + e.getMessage());      
      taskProg.cancelled();     
    }
  }

  private void cancel()
  {
    try
    {
      jobListener.cancelAll();
    }
    catch (JobCancelFailedException j)
    {
      _log.error("Cancel failed due to " + j.getMessage());
    }
  }

  @Override
  public void setJobListener(JobListener jListener)
  {
    jobListener = jListener;   
  }
  
}

