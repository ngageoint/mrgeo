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

package org.mrgeo.mapreduce.job;

import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Vector;

public class JobListener
{
  private static Logger _log = LoggerFactory.getLogger(JobListener.class);
  Vector<Job> jobsList = new Vector<Job>();
  boolean isCancelled = false;
  Object cancelledLock = new Object();
  Object jobsListLock = new Object();
  long _userJobId = -1;
  
  public JobListener(long jobId) {
    _userJobId = jobId;
  }
  
  public long getUserJobId() {
    return _userJobId;
  }
  
  public void setCancelled() {
    synchronized (cancelledLock)
    {
      isCancelled = true;      
    }
  }

  public boolean isCancelled() {
    synchronized (cancelledLock)
    {
      return isCancelled;      
    }
  }
  
  public void addJob(Job job) throws JobCancelledException {
    if (isCancelled()) {
      throw new JobCancelledException("Job cancelled, no new hadoop jobs can be added.");
    }
    synchronized (jobsListLock)
    {
      jobsList.add(job);      
    }
    try {
      JobManager.getInstance().addHadoopJob(_userJobId, job);      
    }
    catch (JobNotFoundException j) {
      //log it and move on
      _log.warn("Unable to find job " + _userJobId + " " + j.getMessage());
    }
  }
  
  public boolean cancelAll() throws JobCancelFailedException {
    boolean success = true;
    setCancelled();
    synchronized (jobsListLock)
    {
      for (Job job : jobsList) {
        _log.info("User requested cancellation - killing job " + job.getJobName());
        //this is a hadoop job, so kill it.
        try {
          job.killJob();         
        }
        catch (IOException e) {
          //log it, make a note of the fact that the job cancel failed 
          //so you can propagate the exception back
          _log.error("Kill job failed for " + job.getJobID());
          success = false;          
        }
      }
      if (!success) {
        throw new JobCancelFailedException("Cancel failed for some of the hadoop jobs, see log for details.");
      }
    }
    return success;      
    }
}
