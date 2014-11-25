package org.mrgeo.mapalgebra;

import org.apache.hadoop.mapreduce.Job;
import org.mrgeo.featurefilter.FeatureFilter;
import org.mrgeo.featurefilter.FeatureFilterChain;
import org.mrgeo.format.FilteredFeatureInputFormat;
import org.mrgeo.format.InlineCsvInputFormat;

import java.io.IOException;

public class InlineCsvInputFormatDescriptor implements InputFormatDescriptor
{
  FeatureFilterChain _chain = new FeatureFilterChain();
  String _columns;
  String _values;
  
  public InlineCsvInputFormatDescriptor(String columns, String values)
  {
    _columns = columns;
    _values = values;
  }

  public String getValues()
  {
    return _values;
  }

  public String getColumns()
  {
    return _columns;
  }

  @Override
  public void populateJobParameters(Job job) throws IOException
  {
    job.setInputFormatClass(FilteredFeatureInputFormat.class);
    FilteredFeatureInputFormat.setInputFormat(job.getConfiguration(), InlineCsvInputFormat.class);
    FilteredFeatureInputFormat.setFeatureFilter(job.getConfiguration(), _chain);

    InlineCsvInputFormat.setColumns(job.getConfiguration(), _columns);
    InlineCsvInputFormat.setValues(job.getConfiguration(), _values);
  }

  @Override
  public FeatureFilter getFilter()
  {
    return _chain;
  }
}
