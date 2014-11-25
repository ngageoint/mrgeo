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

package org.mrgeo.mapreduce.job;

import org.apache.hadoop.mapreduce.Job;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

public class JobsListImpl implements JobCollection
{
  Map<Long, JobDetails> jobsMap = new HashMap<Long, JobDetails>();
  public JobsListImpl() {    
  }
  
  @Override
  public long addJob(String name, String instructions, String type)
  {
    JobDetails jobDetails = new JobDetails();
    jobDetails.setName(name);
    jobDetails.setInstructions(instructions);
    jobDetails.setType(type);
    jobDetails.setStatus(JobDetails.PENDING);
    synchronized (jobsMap)
    {
      long jobId = jobsMap.size() + 1;
      jobDetails.setJobId(jobId);
      jobsMap.put(jobId, jobDetails);
    }
    return jobDetails.getJobId();
  }

  @Override
  public void updateJobProgress(long jobId, float progress) throws JobNotFoundException
  {
    synchronized (jobsMap)
    {
     JobDetails jobDetail = jobsMap.get(jobId);
     if (jobDetail != null) {
       jobDetail.setProgress(progress);
     }
     else {
       throw new JobNotFoundException("Job not found for job id " + jobId);
     }
    }
  }

  @Override
  public void updateJobSuccess(long jobId, String result, String kml) throws JobNotFoundException
  {
    synchronized (jobsMap)
    {
     JobDetails jobDetail = jobsMap.get(jobId);
     if (jobDetail != null) {
       jobDetail.setResult(result);
       jobDetail.setKml(kml);
       jobDetail.setProgress(100);
       jobDetail.setStatus(JobDetails.COMPLETE);
       long duration = Calendar.getInstance().getTimeInMillis() - jobDetail.getStartedDateTime();
       jobDetail.setDuration(duration);
     }
     else {
       throw new JobNotFoundException("Job not found for job id " + jobId);
     }
    }
  }

  @Override
  public void deleteJob(long jobId) throws JobNotFoundException
  {
    synchronized (jobsMap)
    {
      jobsMap.remove(jobId);
    }
  }

  @Override
  public JobDetails getJob(long jobId) throws JobNotFoundException
  {
    JobDetails jobDetails = null;
    synchronized (jobsMap)
    {
     jobDetails = jobsMap.get(jobId);
     if (jobDetails == null) {
       throw new JobNotFoundException("Job not found for job id " + jobId);
     }
    }
    
    return jobDetails;
  }

//  @Override
//  public JobDetails[] getAllJobs()
//  {
//    JobDetails[] jobDetails = null; 
//    synchronized (jobsMap)
//    {
//      jobDetails = new JobDetails[jobsMap.size()];
//      System.err.println("jobs map " + jobsMap.size());
//      jobDetails= jobsMap.values().toArray(jobDetails);
//    }
//    return jobDetails;
//  }
  
  @Override
  public int getJobCount()
  {
    int cnt=0;
    synchronized (jobsMap)
    {
      cnt = jobsMap.size();
    }
    return cnt;
  }
  
  @Override
  public JobDetails[] getJobs(int pageSize, int startNdx)
  {
    JobDetails[] result = null;
    synchronized (jobsMap) {
      JobDetails[] jobDetails = new JobDetails[jobsMap.size()];
      jobDetails= jobsMap.values().toArray(jobDetails);
        int arraySz = pageSize;
        
        if (startNdx + pageSize - 1 > jobDetails.length) {
          arraySz = jobDetails.length - startNdx + 1;
        }
        result = new JobDetails[arraySz];
        for (int i=0; i < arraySz; i++) {
          result[i] = jobDetails[startNdx-1 + i];
        }
    }
    return result;
  }

  @Override
  public void updateJobFailed(long jobId, String result) throws JobNotFoundException
  {
    synchronized (jobsMap)
    {
     JobDetails jobDetail = jobsMap.get(jobId);
     if (jobDetail != null) {
       jobDetail.setStatus(JobDetails.FAILED);
       jobDetail.setMessage(result);
     }
     else {
       throw new JobNotFoundException("Job not found for job id " + jobId);
     }
    }
  }

  @Override
  public float getJobProgress(long jobId) throws JobNotFoundException
  {
    JobDetails jdets = getJob(jobId);
    return jdets.getProgress();    
  }

  @Override
  public boolean isJobFailed(long jobId) throws JobNotFoundException
  {
    JobDetails jdets = getJob(jobId);
    return jdets.getStatus().equals(JobDetails.FAILED);    
  }

  @Override
  public String getJobResult(long jobId) throws JobNotFoundException
  {
    JobDetails jdets = getJob(jobId);
    return jdets.getResult();        
  }

  @Override
  public void setJobStarting(long jobId) throws JobNotFoundException
  {
    synchronized (jobsMap)
    {
     JobDetails jobDetail = jobsMap.get(jobId);
     if (jobDetail != null) {
       jobDetail.setProgress(0.0f);
       jobDetail.setStatus(JobDetails.RUNNING);
       jobDetail.setStartedDateTime(System.currentTimeMillis());
     }
     else {
       throw new JobNotFoundException("Job not found for job id " + jobId);
     }
    }
  }

  @Override
  public void updateJobCancelled(long jobId) throws JobNotFoundException
  {
    synchronized (jobsMap)
    {
     JobDetails jobDetail = jobsMap.get(jobId);
     if (jobDetail != null) {
       jobDetail.setStatus(JobDetails.CANCELLED);
     }
     else {
       throw new JobNotFoundException("Job not found for job id " + jobId);
     }
    }
  }

  @Override
  public void addHadoopJob(long jobId, Job job) throws JobNotFoundException
  {
    JobDetails jobDetails = getJob(jobId);
    if (jobDetails != null) {
      jobDetails.addHadoopJob(job);
    }
  }

  @Override
  public Vector<Job> getHadoopJobs(long jobId) throws JobNotFoundException
  {
    JobDetails jobDetails = getJob(jobId);
    if (jobDetails != null) {
      return jobDetails.getHadoopJobs();
    }
    return null;
  }

  @Override
  public long addTask(long jobId, String taskName) throws JobNotFoundException
  {
    synchronized (jobsMap)
    {
     JobDetails jobDetail = jobsMap.get(jobId);
      if (jobDetail != null) {
        return jobDetail.addTask(taskName);
      }
    }
    return -1;    
  }

  @Override
  public void updateTaskProgress(long jobId, long taskId, float progress)
      throws JobNotFoundException
  {
    synchronized (jobsMap)
    {
     JobDetails jobDetail = jobsMap.get(jobId);
      if (jobDetail != null) {
        jobDetail.setProgress(progress, taskId);
      }
    }
  }

  @Override
  public void updateTaskFailed(long jobId, long taskId, String result) throws JobNotFoundException
  {
    synchronized (jobsMap)
    {
     JobDetails jobDetail = jobsMap.get(jobId);
      if (jobDetail != null) {
        jobDetail.setStatus(JobDetails.FAILED, taskId);
        jobDetail.setMessage(result, taskId);
      }
    }
  }

  @Override
  public void updateTaskSuccess(long jobId, long taskId) throws JobNotFoundException
  {
    synchronized (jobsMap)
    {
     JobDetails jobDetail = jobsMap.get(jobId);
     if (jobDetail != null) {
       jobDetail.setProgress(100, taskId);
       jobDetail.setStatus(JobDetails.COMPLETE, taskId);
       long duration = Calendar.getInstance().getTimeInMillis() - jobDetail.getStartedDateTime();
       jobDetail.setDuration(duration, taskId);
     }
     else {
       throw new JobNotFoundException("Job not found for job id " + jobId);
     }
    }
  }

  @Override
  public void updateTaskNotExecuted(long jobId, long taskId) throws JobNotFoundException
  {
    synchronized (jobsMap)
    {
     JobDetails jobDetail = jobsMap.get(jobId);
     // The task was never executed, so we just mark it as complete
     if (jobDetail != null) {
       jobDetail.setStatus(JobDetails.COMPLETE, taskId);
     }
     else {
       throw new JobNotFoundException("Job not found for job id " + jobId);
     }
    }
  }

  @Override
  public void updateTaskCancelled(long jobId, long taskId) throws JobNotFoundException
  {
    synchronized (jobsMap)
    {
     JobDetails jobDetail = jobsMap.get(jobId);
      if (jobDetail != null) {
        jobDetail.setStatus(JobDetails.CANCELLED, taskId);
      }
    }        
  }

  @Override
  public float getTaskProgress(long jobId, long taskId) throws JobNotFoundException
  {
    JobDetails jdets = getJob(jobId);
    return jdets.getProgress(taskId);        
  }

  @Override
  public boolean isTaskFailed(long jobId, long taskId) throws JobNotFoundException
  {
    JobDetails jdets = getJob(jobId);
    return jdets.getStatus(taskId).equals(JobDetails.FAILED);  
  }

  @Override
  public void setTaskStarting(long jobId, long taskId) throws JobNotFoundException
  {
    synchronized (jobsMap)
    {
     JobDetails jobDetail = jobsMap.get(jobId);
     if (jobDetail != null) {
       jobDetail.setProgress(0.0f, taskId);
       jobDetail.setStatus(JobDetails.RUNNING, taskId);
       jobDetail.setStartedDateTime(System.currentTimeMillis(), taskId);
     }
     else {
       throw new JobNotFoundException("Job not found for job id " + jobId);
     }
    }
  }

  @Override
  public void addTaskHadoopJob(long jobId, long taskId, Job job) throws JobNotFoundException
  {
    synchronized (jobsMap)
    {
     JobDetails jobDetail = jobsMap.get(jobId);
      if (jobDetail != null) {
        jobDetail.addHadoopJob(job, taskId);
      }
    }    
  }

  @Override
  public Vector<Job> getTaskHadoopJobs(long jobId, long taskId) throws JobNotFoundException
  {
    JobDetails jdets = getJob(jobId);
    return jdets.getHadoopJobs(taskId);        
  }

}
