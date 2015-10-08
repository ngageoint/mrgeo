package org.mrgeo.hdfs.vector;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.mrgeo.data.DataProviderException;
import org.mrgeo.data.vector.VectorDataProvider;
import org.mrgeo.data.vector.VectorOutputFormatContext;
import org.mrgeo.data.vector.VectorOutputFormatProvider;
import org.mrgeo.geometry.Geometry;

import java.io.IOException;

public class DelimitedVectorOutputFormatProvider implements VectorOutputFormatProvider
{
  private HdfsVectorDataProvider provider;
  private VectorOutputFormatContext context;

  public DelimitedVectorOutputFormatProvider(HdfsVectorDataProvider provider,
                                             VectorOutputFormatContext context)
  {
    this.provider = provider;
    this.context = context;
  }

  @Override
  public OutputFormat<LongWritable, Geometry> getOutputFormat(String input)
  {
    return new CsvOutputFormat();
  }

  @Override
  public void setupJob(Job job) throws DataProviderException, IOException
  {
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Geometry.class);
    job.setOutputFormatClass(CsvOutputFormat.class);
    CsvOutputFormat.setup(new Path(provider.getResolvedResourceName(false)), job);
  }

  @Override
  public void teardown(Job job) throws DataProviderException
  {
  }
}
