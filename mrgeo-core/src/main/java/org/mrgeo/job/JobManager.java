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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class JobManager
{
private static final Logger _log = LoggerFactory.getLogger(JobManager.class);
static JobManager theInstance;
private ExecutorService threadPool = Executors.newCachedThreadPool();

protected JobManager()
{
}

synchronized public static JobManager getInstance()
{
  if (theInstance == null)
  {
    theInstance = new JobManager();
  }
  return theInstance;
}

@SuppressFBWarnings(value = "RV_RETURN_VALUE_IGNORED_BAD_PRACTICE",
    justification = "For now, there is no monitoring.  Eventually there should be.")
public long submitJob(String name, RunnableJob job)
{
  // For now, we aren't going to worry about monitoring job status. We can add that
  // later as needed. To do this, we would need the applicationId which can be gotten
  // from SparkListener.onApplicationStart. We would actually need the applicationId
  // for both the map algebra job and the build pyramid job that follows it. However,
  // we can't configure a SparkListener directly from here because the SparkContext is
  // created on the driver side (which runs on a worker node). Instead, we would have
  // to define a REST endpoint like ".../job/register/<jobId>" and pass that URI to
  // the remote side to call back from the MrGeoListener it sets up (or something
  // similar to that).
  threadPool.submit(job);
  return -1L;
}
}
