/*
 * Copyright 2009-2017. DigitalGlobe, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.mrgeo.data.vector;

import org.apache.hadoop.conf.Configuration;
import org.mrgeo.data.ProviderProperties;

import java.util.HashSet;
import java.util.Set;

public class VectorInputFormatContext
{
private static final String className = VectorInputFormatContext.class.getSimpleName();
private static final String INPUTS_COUNT = className + ".inputsCount";
private static final String INPUTS_PREFIX = className + ".inputs.";
private static final String FEATURE_COUNT_KEY = className + ".featureCount";
private static final String MIN_FEATURES_PER_SPLIT_KEY = className + ".minFeaturesPerSplit";
private static final String PROVIDER_PROPERTY_KEY = className + ".provProps";

// TODO: Might need to include properties for spatial filtering
// here - like a geometry collection. We could also add a flag for
// inclusive/exclusive. Or we could make this extensive to support
// lots of different spatial filtering, like intersects, colinear,
// touches, disjoint, overlaps, contains, etc... Searching wikipedia
// for "Spatial query" gives a pretty good list.
//
// TODO: Also should consider properties for attribute filtering.

private Set<String> inputs;
private ProviderProperties inputProviderProperties = new ProviderProperties();
private long featureCount = -1L;
private int minFeaturesPerSplit = -1;

public VectorInputFormatContext(final Set<String> inputs,
    final ProviderProperties inputProviderProperties)
{
  this.inputs = inputs;
  this.inputProviderProperties = inputProviderProperties;
}

public VectorInputFormatContext(final Set<String> inputs,
    final ProviderProperties inputProviderProperties, long featureCount,
    int minFeaturesPerSplit)
{
  this(inputs, inputProviderProperties);
  this.featureCount = featureCount;
  this.minFeaturesPerSplit = minFeaturesPerSplit;
}

protected VectorInputFormatContext()
{
}

public static VectorInputFormatContext load(final Configuration conf)
{
  VectorInputFormatContext context = new VectorInputFormatContext();
  context.inputs = new HashSet<String>();
  context.featureCount = conf.getLong(FEATURE_COUNT_KEY, -1L);
  context.minFeaturesPerSplit = conf.getInt(MIN_FEATURES_PER_SPLIT_KEY, -1);
  int inputsCount = conf.getInt(INPUTS_COUNT, 0);
  for (int inputIndex = 0; inputIndex < inputsCount; inputIndex++)
  {
    String input = conf.get(INPUTS_PREFIX + inputIndex);
    context.inputs.add(input);
  }
  String strProviderProperties = conf.get(PROVIDER_PROPERTY_KEY);
  if (strProviderProperties != null)
  {
    context.inputProviderProperties = ProviderProperties.fromDelimitedString(strProviderProperties);
  }
  return context;
}

public Set<String> getInputs()
{
  return inputs;
}

public ProviderProperties getProviderProperties()
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
  for (String input : inputs)
  {
    conf.set(INPUTS_PREFIX + inputIndex, input);
    inputIndex++;
  }
  conf.setLong(FEATURE_COUNT_KEY, featureCount);
  conf.setInt(MIN_FEATURES_PER_SPLIT_KEY, minFeaturesPerSplit);
  conf.set(PROVIDER_PROPERTY_KEY, ProviderProperties.toDelimitedString(inputProviderProperties));
}
}
