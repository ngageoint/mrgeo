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

package org.mrgeo.job;

public class JobDetails
{
public final static String COMPLETE = "Complete";
public final static String FAILED = "Failed";
public final static String PENDING = "Pending";
public final static String RUNNING = "Running";
public final static String CANCELLED = "Cancelled";

TaskDetails taskDetails = null;
long _jobId;
String _name;
String _status;
String _result;
String _kml;
String _instructions;
String _type;
long _started = -1;
long _duration = -1; //time in ms that job ran
String _message;

public long getStartedDateTime()
{
  return _started;
}

public void setStartedDateTime(long d)
{
  _started = d;
}

public long getDuration()
{
  return _duration;
}

public void setDuration(long d)
{
  _duration = d;
}

public long getJobId()
{
  return _jobId;
}

public void setJobId(long jobId)
{
  _jobId = jobId;
}

public String getName()
{
  return _name;
}

public void setName(String name)
{
  _name = name;
}

public String getStatus()
{
  return _status;
}

public void setStatus(String status) throws IllegalArgumentException
{
  if (status.equals(COMPLETE) ||
      status.equals(FAILED) ||
      status.equals(PENDING) ||
      status.equals(RUNNING) ||
      status.equals(CANCELLED))
  {
    _status = status;
  }
  else
  {
    throw new IllegalArgumentException("Invalid status - " + status);
  }
}

public String getResult()
{
  return _result;
}

public void setResult(String result)
{
  _result = result;
}

public String getKml()
{
  return _kml;
}

public void setKml(String kml)
{
  _kml = kml;
}

public String getInstructions()
{
  return _instructions;
}

public void setInstructions(String instructions)
{
  _instructions = instructions;
}

public String getType()
{
  return _type;
}

public void setType(String type)
{
  _type = type;
}

public String getMessage()
{
  return _message;
}

public void setMessage(String msg)
{
  _message = msg;
}

public boolean isFinished()
{
  if (_status.equals(COMPLETE) ||
      _status.equals(FAILED) ||
      _status.equals(CANCELLED))
  {
    return true;
  }
  return false;
}

public long addTask(String taskName)
{
  //for now taskID is the job ID, one task per job, this can be
  //expanded later
  getTaskDetails(_jobId).setName(taskName);
  return _jobId;
}

public long getStartedDateTime(long taskID)
{
  return getTaskDetails(taskID).getStartedDateTime();
}

public void setStartedDateTime(long d, long taskID)
{
  getTaskDetails(taskID).setStartedDateTime(d);
}

public long getDuration(long taskID)
{
  return getTaskDetails(taskID).getDuration();
}

public void setDuration(long d, long taskID)
{
  getTaskDetails(taskID).setDuration(d);
}

public String getTaskName(long taskID)
{
  return getTaskDetails(taskID).getName();
}

public void setTaskName(String name, long taskID)
{
  getTaskDetails(taskID).setName(name);
}

public String getStatus(long taskID)
{
  return getTaskDetails(taskID).getStatus();
}

public void setStatus(String status, long taskID) throws IllegalArgumentException
{
  getTaskDetails(taskID).setStatus(status);
}

public void setMessage(String msg, long taskID)
{
  getTaskDetails(taskID).setMessage(msg);
}

public String getMessage(long taskID)
{
  return getTaskDetails(taskID).getMessage();
}

public boolean isFinished(long taskID)
{
  return getTaskDetails(taskID).isFinished();
}

private TaskDetails getTaskDetails(long taskID)
{
  //lazy - for now just have one task per job
  //this is being done for running buildPyramid after the MapAlgebraJob is completed
  //so for now we only need to track one task for the job
  //if in the future we need to track multiple tasks, we can create a Map of tasks
  if (taskDetails == null)
  {
    taskDetails = new TaskDetails(taskID);
  }
  return taskDetails;
}
}
