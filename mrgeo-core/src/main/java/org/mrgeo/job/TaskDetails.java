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

public class TaskDetails
{
public final static String COMPLETE = "Complete";
public final static String FAILED = "Failed";
public final static String PENDING = "Pending";
public final static String RUNNING = "Running";
public final static String CANCELLED = "Cancelled";

long _jobId;
String _name;
String _status = null;
long _started = -1;
long _duration = -1; //time in ms that job ran
String _message;

public TaskDetails(long taskId)
{
  _jobId = taskId;
}

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
  if (_status != null && (_status.equals(COMPLETE) ||
      _status.equals(FAILED) ||
      _status.equals(CANCELLED)))
  {
    return true;
  }
  return false;
}
}
