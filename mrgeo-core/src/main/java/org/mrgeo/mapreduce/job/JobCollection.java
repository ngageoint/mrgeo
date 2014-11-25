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

import java.util.Vector;

public interface JobCollection
{
  public long addJob(String name, String instructions, String type);
  public void updateJobProgress(long jobId, float progress) throws JobNotFoundException;
  public void updateJobFailed(long jobId, String result) throws JobNotFoundException;
  public void updateJobSuccess(long jobId, String result, String kml) throws JobNotFoundException;
  public void updateJobCancelled(long jobId) throws JobNotFoundException;
  public void deleteJob(long jobId) throws JobNotFoundException;
  public JobDetails getJob(long jobId) throws JobNotFoundException;
  //public JobDetails[] getAllJobs();
  public JobDetails[] getJobs(int pageSize, int startNdx);
  public float getJobProgress(long jobId) throws JobNotFoundException;
  public boolean isJobFailed(long jobId) throws JobNotFoundException;
  public String getJobResult(long jobId) throws JobNotFoundException;
  public int getJobCount();
  public void setJobStarting(long jobId) throws JobNotFoundException;
  public void addHadoopJob(long jobId, Job job) throws JobNotFoundException;
  public Vector<Job> getHadoopJobs(long jobId) throws JobNotFoundException;
  
  //task related methods
  public long addTask(long jobId, String taskName) throws JobNotFoundException;
  public void updateTaskProgress(long jobId, long taskId, float progress) throws JobNotFoundException;
  public void updateTaskFailed(long jobId, long taskId, String result) throws JobNotFoundException;
  public void updateTaskSuccess(long jobId, long taskId) throws JobNotFoundException;
  public void updateTaskNotExecuted(long jobId, long taskId) throws JobNotFoundException;
  public void updateTaskCancelled(long jobId, long taskId) throws JobNotFoundException;
  public float getTaskProgress(long jobId, long taskId) throws JobNotFoundException;
  public boolean isTaskFailed(long jobId, long taskId) throws JobNotFoundException;
  public void setTaskStarting(long jobId, long taskId) throws JobNotFoundException;
  public void addTaskHadoopJob(long jobId, long taskId, Job job) throws JobNotFoundException;
  public Vector<Job> getTaskHadoopJobs(long jobId, long taskId) throws JobNotFoundException;    
}
