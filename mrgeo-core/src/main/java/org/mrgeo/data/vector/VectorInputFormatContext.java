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

package org.mrgeo.data.vector;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class VectorInputFormatContext
{
  private static final String className = VectorInputFormatContext.class.getSimpleName();
  private static final String INPUTS_COUNT = className + ".inputsCount";
  private static final String INPUTS_PREFIX = className + ".inputs.";
  private static final String FEATURE_COUNT_KEY = className + ".featureCount";
  private static final String MIN_FEATURES_PER_SPLIT_KEY = className + ".minFeaturesPerSplit";
  private static final String PROVIDER_PROPERTY_COUNT = className + ".provPropCount";
  private static final String PROVIDER_PROPERTY_KEY = className + ".provPropKey.";
  private static final String PROVIDER_PROPERTY_VALUE = className + ".provPropValue.";

  // TODO: Might need to include properties for spatial filtering
  // here - like a geometry collection. We could also add a flag for
  // inclusive/exclusive. Or we could make this extensive to support
  // lots of different spatial filtering, like intersects, colinear,
  // touches, disjoint, overlaps, contains, etc... Searching wikipedia
  // for "Spatial query" gives a pretty good list.
  //
  // TODO: Also should consider properties for attribute filtering.

  private Set<String> inputs;
  private Properties inputProviderProperties = new Properties();
  private long featureCount = -1L;
  private int minFeaturesPerSplit = -1;

  public VectorInputFormatContext(final Set<String> inputs,
      final Properties inputProviderProperties)
  {
    this.inputs = inputs;
    if (inputProviderProperties != null)
    {
      this.inputProviderProperties.putAll(inputProviderProperties);
    }
  }

  public VectorInputFormatContext(final Set<String> inputs,
      final Properties inputProviderProperties, long featureCount,
      int minFeaturesPerSplit)
  {
    this(inputs, inputProviderProperties);
    this.featureCount = featureCount;
    this.minFeaturesPerSplit = minFeaturesPerSplit;
  }
  protected VectorInputFormatContext()
  {
  }

  public Set<String> getInputs()
  {
    return inputs;
  }

  public Properties getProviderProperties()
  {
    return inputProviderProperties;
  }

  public long getFeatureCount()
  {
    return featureCount;
  }

  public int getMinFeaturesPerSplit()
  {
    return minFeaturesPerSplit;
  }

  public void save(final Configuration conf)
  {
    conf.setInt(INPUTS_COUNT, inputs.size());
    int inputIndex = 0;
    for (String input: inputs)
    {
      conf.set(INPUTS_PREFIX + inputIndex, input);
      inputIndex++;
    }
    conf.setLong(FEATURE_COUNT_KEY, featureCount);
    conf.setInt(MIN_FEATURES_PER_SPLIT_KEY, minFeaturesPerSplit);
    conf.setInt(PROVIDER_PROPERTY_COUNT,
        ((inputProviderProperties == null) ? 0 : inputProviderProperties.size()));
    if (inputProviderProperties != null)
    {
      Set<String> keySet = inputProviderProperties.stringPropertyNames();
      String[] keys = new String[keySet.size()];
      keySet.toArray(keys);
      for (int i=0; i < keys.length; i++)
      {
        conf.set(PROVIDER_PROPERTY_KEY + i, keys[i]);
        String v = inputProviderProperties.getProperty(keys[i]);
        if (v != null)
        {
          conf.set(PROVIDER_PROPERTY_VALUE + i, v);
        }
      }
    }
  }

  public static VectorInputFormatContext load(final Configuration conf)
  {
    VectorInputFormatContext context = new VectorInputFormatContext();
    context.inputs = new HashSet<String>();
    context.featureCount = conf.getLong(FEATURE_COUNT_KEY, -1L);
    context.minFeaturesPerSplit = conf.getInt(MIN_FEATURES_PER_SPLIT_KEY, -1);
    int inputsCount = conf.getInt(INPUTS_COUNT, 0);
    for (int inputIndex=0; inputIndex < inputsCount; inputIndex++)
    {
      String input = conf.get(INPUTS_PREFIX + inputIndex);
      context.inputs.add(input);
    }
    int providerPropertyCount = conf.getInt(PROVIDER_PROPERTY_COUNT, 0);
    if (providerPropertyCount > 0)
    {
      context.inputProviderProperties = new Properties();
      for (int i=0; i < providerPropertyCount; i++)
      {
        String key = conf.get(PROVIDER_PROPERTY_KEY + i);
        String value = conf.get(PROVIDER_PROPERTY_VALUE + i);
        context.inputProviderProperties.setProperty(key, value);
      }
    }
    return context;
  }
}
