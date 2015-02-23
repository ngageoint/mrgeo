package org.mrgeo.hdfs.vector;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.mrgeo.data.DataProviderException;
import org.mrgeo.data.vector.VectorInputFormatContext;
import org.mrgeo.data.vector.VectorInputFormatProvider;
import org.mrgeo.geometry.Geometry;

public class HdfsVectorInputFormatProvider extends VectorInputFormatProvider
{
  public HdfsVectorInputFormatProvider(VectorInputFormatContext context)
  {
    super(context);
  }

  @Override
  public InputFormat<LongWritable, Geometry> getInputFormat(String input)
  {
    return new HdfsVectorInputFormat();
  }

  @Override
  public void setupJob(Job job, Properties providerProperties) throws DataProviderException
  {
    super.setupJob(job, providerProperties);
    HdfsVectorInputFormat.setupJob(job.getConfiguration());
    // Make sure the native input format is configured
    for (String input: getContext().getInputs())
    {
      try
      {
        TextInputFormat.addInputPath(job, new Path(input));
      }
      catch (IOException e)
      {
        throw new DataProviderException(e);
      }
    }
  }
}
