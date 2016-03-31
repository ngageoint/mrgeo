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

package org.mrgeo.resources.job;

import org.apache.hadoop.mapreduce.Job;
import org.mrgeo.mapreduce.job.JobDetails;

import java.util.*;

public class JobResponseFormatter
{
  public static JobsListResponse createJobsListResponse(JobDetails[] jDetails, String jobUri, String prevUrl, String nextUrl) {
    List<JobInfoResponse> jobInfo = new ArrayList<JobInfoResponse>();
    JobsListResponse jres = new JobsListResponse();
    jres.setJobInfo(jobInfo);      
    for (int i = 0; i < jDetails.length; i++)
    {
      JobInfoResponse jInfo = new JobInfoResponse();
      populateJobInfo(jInfo, jDetails[i], jobUri);
      jobInfo.add(jInfo);
    }
    jres.setPrevURL(prevUrl);
    jres.setNextURL(nextUrl);
    return jres;
  }

  public static JobInfoResponse createJobResponse(JobDetails jDetails, String jobUri) {
    JobInfoResponse jInfo = new JobInfoResponse();
    populateJobInfo(jInfo, jDetails, jobUri);
    return jInfo;
  }
  
  public static JobInfoDetailedResponse createJobResponseDetailed(JobDetails jDetails, String jobUri) {
      JobInfoDetailedResponse jInfoDetaild = new JobInfoDetailedResponse();
      populateJobInfoDetailed(jInfoDetaild, jDetails, jobUri);
      return jInfoDetaild;
  }
  
  private static void populateJobInfoDetailed(JobInfoDetailedResponse jInfoDetaild, JobDetails jDetails, String jobUri) {
    populateJobInfo(jInfoDetaild, jDetails, jobUri);
    jInfoDetaild.setInstructions(jDetails.getInstructions());
    Vector<Job> hadoopJobs = jDetails.getHadoopJobs();
    for (@SuppressWarnings("rawtypes")
    Iterator iterator = hadoopJobs.iterator(); iterator.hasNext();)
    {
      Job job = (Job) iterator.next();
      jInfoDetaild.addHadoopJob(job);
    }
  }
  
  private static void populateJobInfo(JobInfoResponse jInfo, JobDetails jDetails, String jobUri) {
    
    jInfo.setJobId(jDetails.getJobId());
    jInfo.setName(jDetails.getName());
    jInfo.setStatusUrl(jobUri + jDetails.getJobId());
    jInfo.setType(jDetails.getType());
    JobStateResponse jsr = new JobStateResponse();
    jsr.setPercent(jDetails.getProgress());
    jsr.setState(jDetails.getStatus());
    jsr.setMessage(jDetails.getMessage());
    jsr.setJobFinished(jDetails.isFinished());
    if (jDetails.getStartedDateTime() != -1) {
      jsr.setStartTime(new Date(jDetails.getStartedDateTime()));      
    }
    if (jDetails.getDuration() != -1) {
      jsr.setDuration(jDetails.getDuration());      
    }
    jInfo.setJobState(jsr);
    
    TaskStateResponse tsr = new TaskStateResponse();
    long taskId = jDetails.getJobId(); //task id is the same as job id
    tsr.setPercent(jDetails.getProgress(taskId));
    tsr.setState(jDetails.getStatus(taskId));
    tsr.setMessage(jDetails.getMessage(taskId));
    tsr.setTaskFinished((jDetails.isFinished(taskId)));
    if (jDetails.getStartedDateTime(taskId) != -1) {
      tsr.setStartTime(new Date(jDetails.getStartedDateTime(taskId)));      
    }
    if (jDetails.getDuration(taskId) != -1) {
      tsr.setDuration(jDetails.getDuration(taskId));      
    }
    jInfo.setBuildPyramidTaskState(tsr);    
  }
}
