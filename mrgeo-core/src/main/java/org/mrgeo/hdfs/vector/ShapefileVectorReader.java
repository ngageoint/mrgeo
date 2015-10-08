package org.mrgeo.hdfs.vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.mrgeo.data.CloseableKVIterator;
import org.mrgeo.data.vector.VectorDataProvider;
import org.mrgeo.data.vector.VectorReader;
import org.mrgeo.data.vector.VectorReaderContext;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.utils.Bounds;

import java.io.IOException;

public class ShapefileVectorReader implements VectorReader
{
  public ShapefileVectorReader(VectorDataProvider provider, VectorReaderContext context, Configuration conf)
  {
  }

  @Override
  public void close()
  {
  }

  @Override
  public CloseableKVIterator<LongWritable, Geometry> get() throws IOException
  {
    return null;
  }

  @Override
  public boolean exists(LongWritable featureId) throws IOException
  {
    return false;
  }

  @Override
  public Geometry get(LongWritable featureId) throws IOException
  {
    return null;
  }

  @Override
  public CloseableKVIterator<LongWritable, Geometry> get(Bounds bounds) throws IOException
  {
    return null;
  }

  @Override
  public long count() throws IOException
  {
    return 0;
  }
}
