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


import org.mrgeo.progress.Progress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskProgress implements Progress
{
  private static final Logger _log = LoggerFactory.getLogger(TaskProgress.class);
  long _jobId;
  long _taskId;
  public TaskProgress(Progress progress) 
  {
    if (progress instanceof JobProgress) {
      JobProgress jp = (JobProgress)progress;
      _jobId = jp._jobId;
      //for now set task id to job id, there is only one post processing
      //task per job
      _taskId = _jobId;      
    }
  }
  
  @Override
  public void failed(String result)
  {
    try
    {
      JobManager.getInstance().updateTaskFailed(_jobId, _taskId, result);
    }
    catch (JobNotFoundException e) {
      _log.error("Error occurred while updating status for task : jobId=" + _jobId + " taskId=" +_taskId + " " + e.getMessage());      
    }
  }

  @Override
  public float get()
  {
    float progress = 0.0f;
    try {
      progress = JobManager.getInstance().getTaskProgress(_jobId, _taskId);     
    }
    catch (JobNotFoundException e) {
      _log.error("Error occurred while getting status for task : jobId=" + _jobId + " taskId=" +_taskId + " " + e.getMessage());     
    }
    return progress;
  }

  @Override
  public String getResult()
  {
    //tasks results are not tracked
    return null;
  }

  @Override
  public boolean isFailed()
  {
    try {
      boolean failed = JobManager.getInstance().isTaskFailed(_jobId, _taskId);
      return failed;
    }
    catch (JobNotFoundException e) {
      _log.error("Error occurred while getting status for task : jobId=" + _jobId + " taskId=" +_taskId + " " + e.getMessage());     
    }
    return false;
  }

  @Override
  public void set(float progress)
  {
    try {
      JobManager.getInstance().updateTaskProgress(_jobId, _taskId, progress);   
    }
    catch (JobNotFoundException e) {
      _log.error("Error occurred while setting status for task : jobId=" + _jobId + " taskId=" +_taskId + " " + e.getMessage());     
      
    }
  }

  @Override
  public void starting()
  {
    try {
      JobManager.getInstance().setTaskStarting(_jobId, _taskId);
    }
    catch (JobNotFoundException e) {
      _log.error("Error occurred while setting task start for task : jobId=" + _jobId + " taskId=" +_taskId + " " + e.getMessage());     
    }
  }
  
  @Override
  public void cancelled()
  {
    try {
      JobManager.getInstance().updateTaskCancelled(_jobId, _taskId);     
    }
    catch (JobNotFoundException e) {
      _log.error("Error occurred while cancelling task : jobId=" + _jobId + " taskId=" +_taskId + " " + e.getMessage());     
    }
  }

  @Override
  public void complete(String result, String kml)
  {
    try {
      JobManager.getInstance().updateTaskSuccess(_jobId, _taskId, result, kml);
    }
    catch (JobNotFoundException e) {
      _log.error("Error occurred while setting task complete : jobId=" + _jobId + " taskId=" +_taskId + " " + e.getMessage());      
    }        
  }


  @Override
  public void complete()
  {
    complete(null, null);    
  }

  public void notExecuted()
  {
    try {
      JobManager.getInstance().updateTaskNotExecuted(_jobId, _taskId);
    }
    catch (JobNotFoundException e) {
      _log.error("Error occurred while setting task not executed : jobId=" + _jobId + " taskId=" +_taskId + " " + e.getMessage());      
    }        
  }
}
