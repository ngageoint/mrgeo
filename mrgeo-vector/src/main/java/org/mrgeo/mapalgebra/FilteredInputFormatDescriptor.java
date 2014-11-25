package org.mrgeo.mapalgebra;

import org.apache.hadoop.mapreduce.Job;
import org.mrgeo.featurefilter.FeatureFilter;
import org.mrgeo.featurefilter.FeatureFilterChain;
import org.mrgeo.format.FilteredFeatureInputFormat;

import java.io.IOException;

public class FilteredInputFormatDescriptor implements InputFormatDescriptor 
{
  InputFormatDescriptor _descriptor;
  public InputFormatDescriptor getDescriptor() { return _descriptor; }
  
  FeatureFilterChain _chain = new FeatureFilterChain();
  
  FilteredInputFormatDescriptor(InputFormatDescriptor d, FeatureFilter f)
  {
    _descriptor = d;
    _chain.add(_descriptor.getFilter());
    _chain.add(f);
  }
  
  @Override
  public FeatureFilter getFilter()
  {
    return _chain;
  }
  
  @Override
  public void populateJobParameters(Job job) throws IOException
  {
    _descriptor.populateJobParameters(job);
    FilteredFeatureInputFormat.setFeatureFilter(job.getConfiguration(), _chain);
  }
}
