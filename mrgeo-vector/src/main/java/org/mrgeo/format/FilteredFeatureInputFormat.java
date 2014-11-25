package org.mrgeo.format;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.*;
import org.mrgeo.featurefilter.FeatureFilter;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.utils.Base64Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class FilteredFeatureInputFormat extends InputFormat<LongWritable, Geometry> implements
    Serializable
{
  static final Logger log = LoggerFactory.getLogger(FilteredFeatureInputFormat.class);

  private static final long serialVersionUID = 1L;

  public static FeatureFilter getFeatureFilter(Configuration conf) throws IOException,
      ClassNotFoundException
  {
    return (FeatureFilter) Base64Utils.decodeToObject(conf.get(FilteredFeatureInputFormat.class
        .getName()
        + "filter"));
  }

  public static void setFeatureFilter(Configuration conf, FeatureFilter filter) throws IOException
  {
    conf.set(FilteredFeatureInputFormat.class.getName() + "filter", Base64Utils.encodeObject(filter));
  }

  public static Class<? extends InputFormat<LongWritable, Geometry>> getInputFormat(
      Configuration conf)
  {
    if (conf.get(FilteredFeatureInputFormat.class.getName() + "input.format") == null)
    {
      return AutoFeatureInputFormat.class;
    }
    return (Class<? extends InputFormat<LongWritable, Geometry>>) conf.getClass(
        FilteredFeatureInputFormat.class.getName() + "input.format", null);
  }

  /**
   * Sets the input format that this feature filter will use.
   * 
   * @param conf
   * @param theClass
   */
  public static void setInputFormat(Configuration conf,
      Class<? extends InputFormat<LongWritable, Geometry>> theClass)
  {
    conf.setClass(FilteredFeatureInputFormat.class.getName() + "input.format", theClass,
        InputFormat.class);
  }

  @Override
  public RecordReader<LongWritable, Geometry> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException
  {
    try
    {
      InputFormat<LongWritable, Geometry> srcFormat = getInputFormat(context.getConfiguration())
          .newInstance();
      RecordReader<LongWritable, Geometry> srcReader = srcFormat.createRecordReader(split, context);
      return new FilteredRecordReader(srcReader, getFeatureFilter(context.getConfiguration()));
    }
    catch (Exception e)
    {
      throw new IOException(e);
    }
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException
  {
    try
    {
      InputFormat<LongWritable, Geometry> srcFormat = getInputFormat(context.getConfiguration())
          .newInstance();
      return srcFormat.getSplits(context);
    }
    catch (InstantiationException e)
    {
      throw new IOException(e);
    }
    catch (IllegalAccessException e)
    {
      throw new IOException(e);
    }
  }

  static public class FilteredRecordReader extends RecordReader<LongWritable, Geometry>
  {
    private RecordReader<LongWritable, Geometry> _source;
    private Geometry _current;
    private FeatureFilter _filter;

    FilteredRecordReader(RecordReader<LongWritable, Geometry> source, FeatureFilter filter)
    {
      _source = source;
      _filter = filter;
    }

    @Override
    public void close() throws IOException
    {
      _source.close();
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException
    {
      return _source.getCurrentKey();
    }

    @Override
    public Geometry getCurrentValue() throws IOException, InterruptedException
    {
      return _current;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException
    {
      return _source.getProgress();
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException,
        InterruptedException
    {
      _source.initialize(split, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException
    {
      do
      {
        if (_source.nextKeyValue() == false)
        {
          return false;
        }
        try
        {
          _current = _filter.filterInPlace(_source.getCurrentValue());
        }
        catch (IllegalArgumentException e)
        {
          log.warn("Error filtering features", e);
          log.warn("Input feature: {}", _source.getCurrentValue());
          log.warn(_source.toString());
          throw e;
        }

      } while (_current == null);

      return true;
    }
  }
}
