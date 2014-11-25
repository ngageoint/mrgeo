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
