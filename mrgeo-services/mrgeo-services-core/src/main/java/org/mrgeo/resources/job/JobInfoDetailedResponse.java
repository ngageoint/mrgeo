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

package org.mrgeo.resources.job;

import org.apache.hadoop.mapreduce.Job;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.List;

@XmlRootElement
public class JobInfoDetailedResponse extends JobInfoResponse
{
  //this class will contain the hadoop jobs list in the future
  String _instructions;
  List<Long> _hadoopJobs = new ArrayList<Long>();
  public String getInstructions() {
    return _instructions;
  }
  public void setInstructions(String instructions) {
    _instructions = instructions;
  }
  @XmlElement(name="tasks")
  public List<Long> getHadoopJobs() {
    return _hadoopJobs;
  }
  public void setHadoopJobs(List<Long> jobs) {
    _hadoopJobs = jobs;
  }

  public void addHadoopJob(Job job) {
    _hadoopJobs.add(new Long(job.getJobID().getId()));
  }
  
}
