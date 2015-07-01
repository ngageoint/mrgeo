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

package org.mrgeo.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.mrgeo.geometry.Geometry;

import java.io.IOException;
import java.util.Random;

public class RandomizeVectorMapper extends Mapper<LongWritable, Geometry, LongWritable, GeometryWritable>
{
  private Random randomGenerator;
  private LongWritable outKey = new LongWritable();

  @Override
  public void setup(Context context)
  {
    randomGenerator = new Random();
  }

  @Override
  public void map(LongWritable key, Geometry value, Context context) throws IOException,
      InterruptedException
  {
    outKey.set(randomGenerator.nextLong());
    context.write(outKey, new GeometryWritable(value));
  }
}
