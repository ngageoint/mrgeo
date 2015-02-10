package org.mrgeo.data.geowave.vector;

import java.io.IOException;
import java.util.List;

import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveInputFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.mrgeo.geometry.Geometry;

public class GeoWaveVectorInputFormat extends InputFormat<LongWritable, Geometry>
{
  private GeoWaveInputFormat delegate = new GeoWaveInputFormat();

  public GeoWaveVectorInputFormat()
  {
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException
  {
    return delegate.getSplits(context);
  }

  @Override
  public RecordReader<LongWritable, Geometry> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException
  {
    RecordReader<LongWritable, Geometry> result = new GeoWaveVectorRecordReader();
    result.initialize(split, context);
    return result;
  }
}
