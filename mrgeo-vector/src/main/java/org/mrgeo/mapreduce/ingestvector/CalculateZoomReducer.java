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

package org.mrgeo.mapreduce.ingestvector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.mrgeo.tile.TileIdZoomWritable;

import java.io.IOException;
import java.util.Arrays;

public class CalculateZoomReducer extends Reducer<TileIdZoomWritable, LongWritable, IntWritable, LongWritable>
{
  int maxzoom = 20;
  
  long[] counts;

  public CalculateZoomReducer()
  {
  }
  
  @Override
  public void cleanup(final Context context) throws IOException, InterruptedException
  {
    for (int i = 1; i <= maxzoom; i++)
    {
//      System.out.println("zoom: " + i + " max per tile: " + counts[i]);
      context.write(new IntWritable(i), new LongWritable(counts[i]));
    }
    
  }
  
  @Override
  public void reduce(final TileIdZoomWritable key, final Iterable<LongWritable> it,
      final Context context) 
  {
    long count = 0;
    
    int zoom = key.getZoom();
    for (LongWritable l: it)
    {
      count += l.get();
    }
    
    counts[zoom] = Math.max(counts[zoom], count);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public void setup(final Reducer.Context context)
  {
    Configuration conf =  context.getConfiguration();
    
    maxzoom = conf.getInt(IngestVectorDriver.MAX_ZOOM, 20);
    
    // need to include the max zoom level...
    counts = new long[maxzoom + 1];
    Arrays.fill(counts, 0);
  }
}
