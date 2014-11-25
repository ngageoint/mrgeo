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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.mrgeo.geometry.Geometry;

import java.io.IOException;

public class GeometryWritableToGeometryReducer extends
    Reducer<LongWritable, GeometryWritable, LongWritable, Geometry>
{

  @Override
  public void reduce(LongWritable key, Iterable<GeometryWritable> values, Context context)
      throws IOException, InterruptedException
  {
    for (GeometryWritable f : values)
    {
      context.write(key, f.getGeometry());
    }
  }
}
