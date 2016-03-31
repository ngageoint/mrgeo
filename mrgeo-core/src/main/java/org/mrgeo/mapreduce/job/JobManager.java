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

package org.mrgeo.mapreduce.job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


public class JobManager
{
  //class handles Job related operations, CRUD, get all jobs. It manages the underlying Jobs collection.
  //The user of this class does not need to be concerned about the details of the underlying persistence mechanism
  //of jobs.
  private static final Logger _log = LoggerFactory.getLogger(JobManager.class);
  protected static JobManager theInstance;
  protected JobManager(JobCollection j) { jobCollection = j;}
  JobCollection jobCollection = null;
  synchronized public static JobManager getInstance()
  {
    if (theInstance == null)
    {
      //for now we will use a jobcollection that is a memory map.
      theInstance = new JobManager(new JobsListImpl());
    }
    return theInstance;
  }
  private Map<Long, Future<RunnableJob>> runningJobs = new HashMap<Long, Future<RunnableJob>>();

  private ExecutorService threadPool = Executors.newCachedThreadPool();

  public long submitJob(String name, RunnableJob job) {
    long jobId = createJob(name, null, null);
    JobProgress p = new JobProgress(jobId);
    JobListener jl = new JobListener(jobId);
    job.setProgress(p);
    job.setJobListener(jl);
    if (jobId != -1) {
      Future<RunnableJob> f = (Future<RunnableJob>) threadPool.submit(job);
      synchronized (runningJobs)
      {
        runningJobs.put(jobId, f);               
      }
    }
    else {
      _log.error("Submit failed for job " + name );
    }
    return jobId;
  }
  
  public long createJob(String name, String instructions, String type) {
    return jobCollection.addJob(name, instructions, type); 
  }
  
  public void updateJobProgress(long jobId, float progress) throws JobNotFoundException {
    jobCollection.updateJobProgress(jobId, progress);
  }
  public void updateJobFailed(long jobId, String result) throws JobNotFoundException {
    jobCollection.updateJobFailed(jobId, result);
  }
  public void updateJobCancelled(long jobId) throws JobNotFoundException {
    jobCollection.updateJobCancelled(jobId);
  }

  public void updateJobSuccess(long jobId, String result, String kml) throws JobNotFoundException {
    jobCollection.updateJobSuccess(jobId, result, kml);
  }
  
  public void setJobStarting(long jobId) throws JobNotFoundException {
    jobCollection.setJobStarting(jobId);
  }

  public void deleteJob(long jobId) throws JobNotFoundException {
    jobCollection.deleteJob(jobId);
  }
  
  public JobDetails getJob(long jobId) throws JobNotFoundException {
    return jobCollection.getJob(jobId);
  }

//  public JobDetails[] getAllJobs() {
//    return jobCollection.getAllJobs();
//  }
  public int getJobCount() {
    return jobCollection.getJobCount();
  }  

  public JobDetails[] getJobs(int pageSize, int startNdx) {
    return jobCollection.getJobs(pageSize, startNdx);
  }
  public float getJobProgress(long jobId) throws JobNotFoundException {
    return jobCollection.getJobProgress(jobId);
  }
  public boolean isJobFailed(long jobId) throws JobNotFoundException {
    return jobCollection.isJobFailed(jobId);
  }
  public String getJobResult(long jobId) throws JobNotFoundException {
    return jobCollection.getJobResult(jobId);
  }
  public void cancelJob(long jobId) {
    synchronized (runningJobs)
    {
      Future<RunnableJob> f = runningJobs.get(jobId);
      _log.info("Cancelling job for jobId " + jobId);
      f.cancel(true);
    }
  }
  public void addHadoopJob(long jobId, org.apache.hadoop.mapreduce.Job job) throws JobNotFoundException {
    jobCollection.addHadoopJob(jobId, job);
  }
  
  public void updateTaskProgress(long jobId, long taskId, float progress) throws JobNotFoundException {
    jobCollection.updateTaskProgress(jobId, taskId, progress);
  }
  public void updateTaskFailed(long jobId, long taskId, String result) throws JobNotFoundException {
    jobCollection.updateTaskFailed(jobId, taskId, result);
  }
  public void updateTaskCancelled(long jobId, long taskId) throws JobNotFoundException {
    jobCollection.updateTaskCancelled(jobId, taskId);
  }

  public void updateTaskSuccess(long jobId, long taskId, String result, String kml) throws JobNotFoundException {
    jobCollection.updateTaskSuccess(jobId, taskId);
  }
  public void updateTaskNotExecuted(long jobId, long taskId) throws JobNotFoundException {
    jobCollection.updateTaskNotExecuted(jobId, taskId);
  }
  public void setTaskStarting(long jobId, long taskId) throws JobNotFoundException {
    jobCollection.setTaskStarting(jobId, taskId);
  }  
  public float getTaskProgress(long jobId, long taskID) throws JobNotFoundException {
    return jobCollection.getTaskProgress(jobId, taskID);
  }
  public boolean isTaskFailed(long jobId, long taskId) throws JobNotFoundException {
    return jobCollection.isTaskFailed(jobId, taskId);
  }
}
