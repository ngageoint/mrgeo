/*
 * Copyright 2009-2014 DigitalGlobe, Inc.
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

package org.mrgeo.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.codehaus.jackson.map.ObjectMapper;
import org.mrgeo.mapreduce.job.JobCancelledException;
import org.mrgeo.mapreduce.job.JobFailedException;
import org.mrgeo.mapreduce.job.JobListener;
import org.mrgeo.progress.Progress;
import org.mrgeo.data.adhoc.AdHocDataProvider;
import org.mrgeo.utils.Bounds;
import org.mrgeo.utils.HadoopUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class MapReduceUtils
{
  private static final Logger log = LoggerFactory.getLogger(MapReduceUtils.class);

  public MapReduceUtils()
  {
    super();
  }

  public static boolean runJob(Job job, Progress progress, JobListener jl) throws JobFailedException, JobCancelledException
  {
    boolean success=false;
    if (jl !=null) {
      //append job id to the job name for easy identification
      job.setJobName("ID_" + jl.getUserJobId() + "_" + job.getJobName()); 
      jl.addJob(job); 
    }
    
    long start = System.currentTimeMillis();
    log.info("Running job {}", job.getJobName());
    
    try
    {
      job.submit();
      log.info("Job {} startup: {}ms", job.getJobName(), (System.currentTimeMillis() - start));
      if (progress == null)
      {
        job.waitForCompletion(true);
      }
      else
      {
        float initP = progress.get();
        float percentP = 100 - initP;
        while (job.isComplete() == false)
        {
          float p = job.mapProgress() * .9f + job.reduceProgress() * .1f;
          progress.set(p * percentP + initP);
          try
          {
            Thread.sleep(500);
          }
          catch (InterruptedException e)
          {
            log.info("Job Cancelled by user");
            throw new JobCancelledException("Job Cancelled by user.");
          }
        }
      }
      
      log.info("Job {} time: {}ms", job.getJobName(), (System.currentTimeMillis() - start));

      if (job.isSuccessful() == false)
      {
        throw new JobFailedException("Job failed: " + job.getTrackingURL());
      }
      success = job.isSuccessful();
    }
    catch (InterruptedException e)
    {
      e.printStackTrace();
      throw new JobFailedException(e.getMessage());
    }
    catch (ClassNotFoundException e)
    {
      e.printStackTrace();
      throw new JobFailedException(e.getMessage());
    }
    // when submitting jobs under JBoss, Exception doesn't appear to be caught
    catch ( Throwable e ) {
        e.printStackTrace();
        throw new JobFailedException(e.getMessage());
    }
    return success;
  }

  public static void runJobAsynchronously(Job job, JobListener jl)
      throws IOException, JobFailedException, JobCancelledException
  {
    if (jl != null)
    {
      //append job id to the job name for easy identification
      job.setJobName("ID_" + jl.getUserJobId() + "_" + job.getJobName());       
      jl.addJob(job);      
    }
    
    try
    {
      long start = System.currentTimeMillis();
      log.info("Running asynchronous job {}", job.getJobName());
      job.submit();
      log.info("Job {} startup: {}ms", job.getJobName(), (System.currentTimeMillis() - start));
    }
    catch (InterruptedException e)
    {
      e.printStackTrace();
      throw new JobFailedException(e.getMessage());
    }
    catch (ClassNotFoundException e)
    {
      e.printStackTrace();
      throw new JobFailedException(e.getMessage());
    }
  }

  /**
   * Check on the progress of a job and return true if the job has completed. Note
   * that a return value of true does not mean the job was successful, just that
   * it completed.
   * 
   * @param job
   * @param progress
   * @param jl
   * @return
   * @throws IOException
   * @throws FileNotFoundException
   * @throws JobFailedException
   * @throws JobCancelledException
   */
  public static boolean checkJobProgress(Job job, Progress progress, JobListener jl)
      throws IOException, JobFailedException, JobCancelledException
  {
    boolean result = job.isComplete();
    if (progress != null)
    {
      float initP = progress.get();
      float percentP = 100 - initP;
      if (!result)
      {
        float p = job.mapProgress() * .9f + job.reduceProgress() * .1f;
        progress.set(p * percentP + initP);
      }
    }
    
    if (result)
    {
      if (!job.isSuccessful())
      {
        if (jl !=null && jl.isCancelled()) {
          throw new JobCancelledException(job.getJobName() + " - Job Cancelled by user");
        }
        throw new JobFailedException("Job failed: " + job.getTrackingURL());
      }
    }
    return result;
  }
  
  /**
   * Serialize bounds as JSON resource to a data provider.
   * 
   * @param provider
   *          the data provider through which to write out the bounds
   * @param Bounds
   *          the Bounds object to serialize
   * @throws IOException
   * @throws InterruptedException
   * @throws Exception
   */
  static public void writeBounds(final AdHocDataProvider provider, 
      Bounds bounds) throws IOException, InterruptedException
    {
      OutputStream stream = null;

      try
      {
        stream = provider.add();
        final ObjectMapper mapper = new ObjectMapper();
        mapper.writeValue(stream, bounds);
      }
      catch (IOException ex)
      {
        log.error("Could not create temp file for storing bounds", ex);
        throw ex;
      }
      finally
      {
        if (stream != null)
        {
          stream.close();
        }
      }
    }
    
  /**
   * Read all of the Bounds JSON resources for a job, compute the bounds that envelopes
   * all of them. An example usage is a map/reduce driver in which either the mapper
   * or reducer generates a piece of a new image and needs to store the Bounds of that
   * new image. The mappers or reducers all use the same AdHoc provider to store a new
   * Bounds resource (using the writeBounds method in this class). When the job completes,
   * the driver can then pass that same AdHoc provider to this method to aggregate the
   * individual Bounds stored by the mappers or reducers into a single Bounds that
   * envelopes all of the individual BOunds generated during the job.
   * 
   * @return Bounds object that corresponds to the envelope of all the bounds resources
   * stored in the data provider
   */
  static public Bounds aggregateBounds(final AdHocDataProvider provider) throws IOException
  {
    //read bounds json files from output path, then delete them
    try
    {
      Bounds finalBounds = null;
      ObjectMapper mapper = new ObjectMapper();
      final int size = provider.size();
      for (int i = 0; i < size; i++)
      {
        final InputStream stream = provider.get(i);
        try
        {
          Bounds bnds = mapper.readValue(stream, Bounds.class);
          if (finalBounds == null) {
            finalBounds = bnds;
          }
          else if (bnds != null) {
            finalBounds.expand(bnds);
          }
        }
        finally
        {
          stream.close();
        }
      }
      return finalBounds;
      
    } catch (IOException ex)
    {
      log.error("Unable to read bounds files.", ex);
      throw ex;
    }
  }

  // creates a job that will ultimately use the tileidpartitioner for partitioning
  public static Job createTiledJob(final String name, final Configuration conf) throws IOException
  {
    final Job job = new Job(conf, name);
  
    setupTiledJob(job);
  
    return job;
  }

  public static void setupTiledJob(final Job job)
  {
    // the TileidPartitioner sets the number of reducers, but we need to prime it to 0
    job.setNumReduceTasks(0);
  }
}