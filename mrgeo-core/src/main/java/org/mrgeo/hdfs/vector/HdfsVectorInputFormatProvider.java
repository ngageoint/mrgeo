package org.mrgeo.hdfs.vector;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.mrgeo.data.DataProviderException;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.vector.VectorDataProvider;
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
    long featureCount = getContext().getFeatureCount();
    int minFeaturesPerSplit = getContext().getMinFeaturesPerSplit();
    boolean calcFeatureCount = (minFeaturesPerSplit > 0 && featureCount < 0);
    if (calcFeatureCount)
    {
      featureCount = 0L;
    }
    for (String input: getContext().getInputs())
    {
      try
      {
        // Set up native input format
        TextInputFormat.addInputPath(job, new Path(input));

        // Compute the number of features across all inputs if we don't already
        // have it in the context.
        if (calcFeatureCount)
        {
          VectorDataProvider dp = DataProviderFactory.getVectorDataProvider(input,
              AccessMode.READ, providerProperties);
          if (dp != null)
          {
            featureCount += dp.getVectorReader().count();
          }
        }
      }
      catch (IOException e)
      {
        throw new DataProviderException(e);
      }
    }
    HdfsVectorInputFormat.setupJob(job, getContext().getMinFeaturesPerSplit(),
        featureCount);
  }
}
