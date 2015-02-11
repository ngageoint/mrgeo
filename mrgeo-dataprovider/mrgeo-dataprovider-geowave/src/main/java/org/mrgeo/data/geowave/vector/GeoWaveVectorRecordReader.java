package org.mrgeo.data.geowave.vector;

import java.io.IOException;

import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveRecordReader;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.mrgeo.data.vector.VectorInputSplit;
import org.mrgeo.geometry.Geometry;
import org.opengis.feature.simple.SimpleFeature;

public class GeoWaveVectorRecordReader extends RecordReader<LongWritable, Geometry>
{
  private GeoWaveRecordReader<Object> delegateReader;
  private LongWritable currKey = new LongWritable();
  private Geometry currValue;

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException,
      InterruptedException
  {
    if (!(split instanceof VectorInputSplit))
    {
      throw new IOException("Expected split to be of type VectorInputSplit, but got: " + split.getClass().getName());
    }
    delegateReader = new GeoWaveRecordReader<Object>();
    // Pass the native split wrapped by VectorInputSplit back into the native reader.
    delegateReader.initialize(((VectorInputSplit)split).getWrappedInputSplit(), context);
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException
  {
    boolean result = delegateReader.nextKeyValue();
    if (result)
    {
      Object value = delegateReader.getCurrentValue();
      if (value instanceof SimpleFeature)
      {
        SimpleFeature feature = (SimpleFeature)value;
        GeoWaveVectorIterator.setKeyFromFeature(currKey, feature);
        currValue = GeoWaveVectorIterator.convertToGeometry(feature);
      }
      else
      {
        throw new IOException("Expected value of type SimpleFeature, but got " + value.getClass().getName());
      }
    }
    else
    {
      currKey = null;
      currValue = null;
    }
    return result;
  }

  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException
  {
    return currKey;
  }

  @Override
  public Geometry getCurrentValue() throws IOException, InterruptedException
  {
    return currValue;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException
  {
    return delegateReader.getProgress();
  }

  @Override
  public void close() throws IOException
  {
    delegateReader.close();
  }
}
