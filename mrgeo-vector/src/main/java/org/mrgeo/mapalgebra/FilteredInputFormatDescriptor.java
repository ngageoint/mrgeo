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

package org.mrgeo.mapalgebra;

import org.apache.hadoop.mapreduce.Job;
import org.mrgeo.featurefilter.FeatureFilter;
import org.mrgeo.featurefilter.FeatureFilterChain;
import org.mrgeo.format.FilteredFeatureInputFormat;

import java.io.IOException;

public class FilteredInputFormatDescriptor implements InputFormatDescriptor 
{
  InputFormatDescriptor _descriptor;
  public InputFormatDescriptor getDescriptor() { return _descriptor; }
  
  FeatureFilterChain _chain = new FeatureFilterChain();
  
  FilteredInputFormatDescriptor(InputFormatDescriptor d, FeatureFilter f)
  {
    _descriptor = d;
    _chain.add(_descriptor.getFilter());
    _chain.add(f);
  }
  
  @Override
  public FeatureFilter getFilter()
  {
    return _chain;
  }
  
  @Override
  public void populateJobParameters(Job job) throws IOException
  {
    _descriptor.populateJobParameters(job);
    FilteredFeatureInputFormat.setFeatureFilter(job.getConfiguration(), _chain);
  }
}
