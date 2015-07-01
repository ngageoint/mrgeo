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

/**
 * 
 */
package org.mrgeo.aggregators;

import com.google.common.collect.ImmutableBiMap;

/**
 * A registry class containing a bi-directional map
 * that can be used to lookup the Aggregator class
 * for a resampling method key and lookup the String
 * key for an Aggregator class for storing in
 * JSON metadata.
 */
public class AggregatorRegistry
{
  static public final ImmutableBiMap<String, Class<? extends Aggregator>> aggregatorRegistry =
      new ImmutableBiMap.Builder<String, Class<? extends Aggregator>>()
          .put("MEAN", MeanAggregator.class)
          .put("SUM", SumAggregator.class)
          .put("MODE", ModeAggregator.class)
          .put("NEAREST", NearestAggregator.class)
          .put("MIN", MinAggregator.class)
          .put("MAX", MaxAggregator.class)
          .put("MINAVGPAIR", MinAvgPairAggregator.class)
          .build();

}
