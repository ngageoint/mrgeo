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

package org.mrgeo.mapalgebra.old;

import org.apache.hadoop.mapreduce.Job;
import org.mrgeo.featurefilter.FeatureFilter;
import org.mrgeo.featurefilter.FeatureFilterChain;
import org.mrgeo.format.FilteredFeatureInputFormat;
import org.mrgeo.format.InlineCsvInputFormat;

import java.io.IOException;

public class InlineCsvInputFormatDescriptor implements InputFormatDescriptor
{
  FeatureFilterChain _chain = new FeatureFilterChain();
  String _columns;
  String _values;
  
  public InlineCsvInputFormatDescriptor(String columns, String values)
  {
    _columns = columns;
    _values = values;
  }

  public String getValues()
  {
    return _values;
  }

  public String getColumns()
  {
    return _columns;
  }

  @Override
  public void populateJobParameters(Job job) throws IOException
  {
    job.setInputFormatClass(FilteredFeatureInputFormat.class);
    FilteredFeatureInputFormat.setInputFormat(job.getConfiguration(), InlineCsvInputFormat.class);
    FilteredFeatureInputFormat.setFeatureFilter(job.getConfiguration(), _chain);

    InlineCsvInputFormat.setColumns(job.getConfiguration(), _columns);
    InlineCsvInputFormat.setValues(job.getConfiguration(), _values);
  }

  @Override
  public FeatureFilter getFilter()
  {
    return _chain;
  }
}
