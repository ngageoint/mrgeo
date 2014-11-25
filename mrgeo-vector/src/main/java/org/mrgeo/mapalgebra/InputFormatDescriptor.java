package org.mrgeo.mapalgebra;

import org.apache.hadoop.mapreduce.Job;
import org.mrgeo.featurefilter.FeatureFilter;

import java.io.IOException;

public interface InputFormatDescriptor
{
  public FeatureFilter getFilter();
  
  public void populateJobParameters(Job job) throws IOException;
}
