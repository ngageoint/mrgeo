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

package org.mrgeo.resources.job;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class JobInfoResponse
{
  private JobStateResponse _state = null;
  private TaskStateResponse _buildPyramid = null;
  
  long _jobId;
  String _name;
  String _type;
  String _statusUrl;
  public long getJobId() {
    return _jobId;
  }  
  public void setJobId(long jobId) {
    _jobId = jobId;
  }

  public String getName() {
    return _name;
  }
  public void setName(String name) {
    _name = name;
  }
  
  public String getStatusUrl() {
    return _statusUrl;
  }
  public void setStatusUrl(String url) {
    _statusUrl = url;
  }
  
  public String getType() {
    return _type;
  }
  public void setType(String type) {
    _type = type;
  }
  
  public JobStateResponse getJobState() {
    return _state;
  }
  public void setJobState(JobStateResponse state) {
    _state = state;
  }    
  
  @XmlElement(name="buildPyramid")
  public TaskStateResponse getBuildPyramidTaskState() {
    return _buildPyramid;
  }
  public void setBuildPyramidTaskState(TaskStateResponse state) {
    _buildPyramid = state;
  }    
}

