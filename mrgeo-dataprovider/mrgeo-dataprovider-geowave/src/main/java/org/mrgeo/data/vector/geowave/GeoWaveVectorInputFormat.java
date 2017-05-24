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

package org.mrgeo.data.vector.geowave;

import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputFormat;
import org.apache.hadoop.mapreduce.*;
import org.mrgeo.data.vector.FeatureIdWritable;
import org.mrgeo.geometry.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class GeoWaveVectorInputFormat extends InputFormat<FeatureIdWritable, Geometry>
{
static Logger log = LoggerFactory.getLogger(GeoWaveVectorInputFormat.class);

private GeoWaveInputFormat delegate = new GeoWaveInputFormat();

public GeoWaveVectorInputFormat()
{
}

@Override
public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException
{
  try
  {
    return delegate.getSplits(context);
  }
  catch (OutOfMemoryError e)
  {
    // This can happen for example if the date range used in the temporal query
    // spans too much time. I'm not sure what else might trigger this.
    throw new IOException("Unable to query GeoWave data due to memory constraints." +
        " If you queried by a time range, the range may be too large.", e);
  }
}

@Override
public RecordReader<FeatureIdWritable, Geometry> createRecordReader(InputSplit split,
                                                                    TaskAttemptContext context) throws IOException, InterruptedException
{
  RecordReader delegateReader = delegate.createRecordReader(split, context);
  RecordReader<FeatureIdWritable, Geometry> result = new GeoWaveVectorRecordReader(delegateReader);
  result.initialize(split, context);
  return result;
}
}
