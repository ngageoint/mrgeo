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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.vector.VectorDataProvider;
import org.mrgeo.data.vector.VectorInputFormat;
import org.mrgeo.data.vector.VectorInputFormatContext;
import org.mrgeo.data.vector.VectorInputFormatProvider;
import org.mrgeo.featurefilter.FeatureFilter;
import org.mrgeo.featurefilter.FeatureFilterChain;
import org.mrgeo.format.AutoFeatureInputFormat;
import org.mrgeo.format.FilteredFeatureInputFormat;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class BasicInputFormatDescriptor implements InputFormatDescriptor
{
  private FeatureFilterChain _chain = new FeatureFilterChain();
  private String _path = null;
  private VectorDataProvider dp;
  private ProviderProperties providerProperties;
  
  public BasicInputFormatDescriptor(String path)
  {
    _path = path;
  }

  public BasicInputFormatDescriptor(VectorDataProvider dp, ProviderProperties providerProperties)
  {
    this.dp = dp;
    this.providerProperties = providerProperties;
  }

  public VectorDataProvider getVectorDataProvider()
  {
    return dp;
  }

  @Override
  public void populateJobParameters(Job job) throws IOException
  {
    job.setInputFormatClass(FilteredFeatureInputFormat.class);
    if (dp != null)
    {
      Set<String> inputs = new HashSet<String>();
      inputs.add(dp.getPrefixedResourceName());
      VectorInputFormatContext context = new VectorInputFormatContext(inputs, providerProperties);
      VectorInputFormatProvider ifp = dp.getVectorInputFormatProvider(context);
      ifp.setupJob(job, providerProperties);
      FilteredFeatureInputFormat.setInputFormat(job.getConfiguration(), VectorInputFormat.class);
      FilteredFeatureInputFormat.setFeatureFilter(job.getConfiguration(), _chain);
    }
    else
    {
      // No vector provider, so fall-back to old HDFS-specific code
      FilteredFeatureInputFormat.setInputFormat(job.getConfiguration(), AutoFeatureInputFormat.class);
      FilteredFeatureInputFormat.setFeatureFilter(job.getConfiguration(), _chain);
  
      FileInputFormat.setInputPaths(job, new Path(_path));
    }
  }

  /**
   * TODO If we ever support variables in map algebra this trick won't work.
   * 
   * @param f
   */
  @Override
  public FeatureFilter getFilter()
  {
    return _chain;
  }
  
  public String getPath()
  {
    return _path;
  }
}
