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

package org.mrgeo.hdfs.vector;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.mrgeo.data.DataProviderException;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.vector.FeatureIdWritable;
import org.mrgeo.data.vector.VectorDataProvider;
import org.mrgeo.data.vector.VectorInputFormatContext;
import org.mrgeo.data.vector.VectorInputFormatProvider;
import org.mrgeo.geometry.Geometry;

import java.io.IOException;

public class DelimitedVectorInputFormatProvider extends VectorInputFormatProvider
{
  public DelimitedVectorInputFormatProvider(VectorInputFormatContext context)
  {
    super(context);
  }

  @Override
  public InputFormat<FeatureIdWritable, Geometry> getInputFormat(String input)
  {
    return new DelimitedVectorInputFormat();
  }

  @Override
  public void setupJob(Job job, ProviderProperties providerProperties) throws DataProviderException
  {
    super.setupJob(job, providerProperties);
    long featureCount = getContext().getFeatureCount();
    int minFeaturesPerSplit = getContext().getMinFeaturesPerSplit();
    boolean calcFeatureCount = (minFeaturesPerSplit > 0 && featureCount < 0);
    if (calcFeatureCount)
    {
      featureCount = 0L;
    }
    for (String input: getContext().getInputs())
    {
      try
      {
        // Set up native input format
        TextInputFormat.addInputPath(job, new Path(input));

        // Compute the number of features across all inputs if we don't already
        // have it in the context.
        if (calcFeatureCount)
        {
          VectorDataProvider dp = DataProviderFactory.getVectorDataProvider(input,
              AccessMode.READ, providerProperties);
          if (dp != null)
          {
            featureCount += dp.getVectorReader().count();
          }
        }
      }
      catch (IOException e)
      {
        throw new DataProviderException(e);
      }
    }
    DelimitedVectorInputFormat.setupJob(job, getContext().getMinFeaturesPerSplit(),
                                        featureCount);
  }
}
