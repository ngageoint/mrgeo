package org.mrgeo.mapalgebra;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.mrgeo.featurefilter.FeatureFilter;
import org.mrgeo.featurefilter.FeatureFilterChain;
import org.mrgeo.format.AutoFeatureInputFormat;
import org.mrgeo.format.FilteredFeatureInputFormat;

import java.io.IOException;

public class BasicInputFormatDescriptor implements InputFormatDescriptor
{
  FeatureFilterChain _chain = new FeatureFilterChain();
  String _path = null;
  
  public BasicInputFormatDescriptor(String path)
  {
    _path = path;
  }

  @Override
  public void populateJobParameters(Job job) throws IOException
  {
    job.setInputFormatClass(FilteredFeatureInputFormat.class);
    FilteredFeatureInputFormat.setInputFormat(job.getConfiguration(), AutoFeatureInputFormat.class);
    FilteredFeatureInputFormat.setFeatureFilter(job.getConfiguration(), _chain);

    FileInputFormat.setInputPaths(job, new Path(_path));
  }

  /**
   * TODO If we ever support variables in map algebra this trick won't work.
   * 
   * @param f
   */
  @Override
  public FeatureFilter getFilter()
  {
    return _chain;
  }
  
  public String getPath()
  {
    return _path;
  }
}
