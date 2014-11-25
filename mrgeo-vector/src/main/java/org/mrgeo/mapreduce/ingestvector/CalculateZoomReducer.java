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
