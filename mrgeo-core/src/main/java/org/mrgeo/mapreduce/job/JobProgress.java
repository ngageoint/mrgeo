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


import org.mrgeo.progress.Progress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobProgress implements Progress
{
  private static final Logger _log = LoggerFactory.getLogger(JobProgress.class);
  long _jobId;
  public JobProgress(long jobId) 
  {
    _jobId = jobId;
  }
  @Override
  public void complete()
  {
    complete(null, null);
  }

  @Override
  public void complete(String result, String kml)
  {
    try {
      JobManager.getInstance().updateJobSuccess(_jobId, result, kml);
    }
    catch (JobNotFoundException e) {
      _log.error("Error occurred while updating status for job " + _jobId + " " + e.getMessage());      
    }
  }

  @Override
  public void failed(String result)
  {
    try
    {
      JobManager.getInstance().updateJobFailed(_jobId, result);
    }
    catch (JobNotFoundException e) {
      _log.error("Error occurred while updating status for job " + _jobId + " " + e.getMessage());      
    }
  }

  @Override
  public float get()
  {
    float progress = 0.0f;
    try {
      progress = JobManager.getInstance().getJobProgress(_jobId);      
    }
    catch (JobNotFoundException e) {
      _log.error("Error occurred while updating status for job " + _jobId + " " + e.getMessage());      
    }
    return progress;
  }

  @Override
  public String getResult()
  {
    try {
      String result = JobManager.getInstance().getJobResult(_jobId); 
      return result;
    }
    catch (JobNotFoundException e) {
      _log.error("Error occurred while updating status for job " + _jobId + " " + e.getMessage());      
    }
    return null;
  }

  @Override
  public boolean isFailed()
  {
    try {
      boolean failed = JobManager.getInstance().isJobFailed(_jobId);
      return failed;
    }
    catch (JobNotFoundException e) {
      _log.error("Error occurred while updating status for job " + _jobId + " " + e.getMessage());      
    }
    return false;
  }

  @Override
  public void set(float progress)
  {
    try {
      JobManager.getInstance().updateJobProgress(_jobId, progress);      
    }
    catch (JobNotFoundException e) {
      _log.error("Error occurred while updating status for job " + _jobId + " " + e.getMessage());      
    }
  }

  @Override
  public void starting()
  {
    try {
      JobManager.getInstance().setJobStarting(_jobId);      
    }
    catch (JobNotFoundException e) {
      _log.error("Error occurred while updating status for job " + _jobId + " " + e.getMessage());      
    }
  }
  
  @Override
  public void cancelled()
  {
    try {
      JobManager.getInstance().updateJobCancelled(_jobId);      
    }
    catch (JobNotFoundException e) {
      _log.error("Error occurred while updating status for job " + _jobId + " " + e.getMessage());      
    }
  }
}
