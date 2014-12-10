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
package org.mrgeo.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.mrgeo.featurefilter.FeatureFilter;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.rasterops.OpImageRegistrar;
import org.mrgeo.utils.Base64Utils;

import java.io.IOException;

public class FeatureFilterMapper extends
    Mapper<LongWritable, Geometry, LongWritable, Geometry>
{
  public static String FEATURE_FILTER = FeatureFilterMapper.class.getName() + ".featureFilter";
  private FeatureFilter filter = null;
  private LongWritable outputKey = new LongWritable();

  @Override
  public void setup(Context context)
  {
    OpImageRegistrar.registerMrGeoOps();
    try
    {
      Configuration conf = context.getConfiguration();
      if (conf.get(FEATURE_FILTER) != null)
      {
        filter = (FeatureFilter) Base64Utils.decodeToObject(conf.get(FEATURE_FILTER));
      }
    }
    catch (Exception e)
    {
      throw new IllegalArgumentException("Error parsing configuration", e);
    }
  }

  @Override
  public void map(LongWritable key, Geometry value, Context context) throws IOException,
      InterruptedException
  {
    if (filter != null)
    {
      value = filter.filterInPlace(value);
      if (value == null)
      {
        return;
      }
      System.out.printf("Post-filter %s\n", value.toString());
    }
    
    context.write(key, value);
  }
}
