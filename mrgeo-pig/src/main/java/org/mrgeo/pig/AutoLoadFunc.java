package org.mrgeo.pig;

import org.apache.commons.lang.time.FastDateFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.*;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.mrgeo.format.AutoFeatureInputFormat;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.utils.HadoopUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AutoLoadFunc extends LoadFunc implements LoadMetadata
{
  @SuppressWarnings("rawtypes")
  private RecordReader _reader;
  private TupleFactory _tupleFactory = TupleFactory.getInstance();

  @Override
  public InputFormat getInputFormat() throws IOException
  {
    return new AutoFeatureInputFormat();
  }

  @Override
  public Tuple getNext() throws IOException
  {
    Tuple result = null;
    Geometry f;
    boolean gotFeature;
    try
    {
      gotFeature = _reader.nextKeyValue();
      f = (Geometry) _reader.getCurrentValue();
    }
    catch (InterruptedException e)
    {
      throw new IOException(e);
    }

    if (gotFeature)
    {
      Map<String, String> attrs = f.getAllAttributesSorted();
      ArrayList<Object> entries = new ArrayList<Object>(attrs.size() + 1);

      // skip 0
      entries.add(null);

      for (String attr: attrs.values())
      {
          entries.add(attr);
      }

      result = _tupleFactory.newTupleNoCopy(entries);
    }

    return result;
  }

  @Override
  public void prepareToRead(RecordReader reader, PigSplit split) throws IOException
  {
    _reader = reader;
  }

  @Override
  public void setLocation(String location, Job job) throws IOException
  {
    FileInputFormat.setInputPaths(job, location);
  }

  @Override
  public String[] getPartitionKeys(String arg0, Job arg1) throws IOException
  {
    // not needed.
    return null;
  }

  @Override
  public ResourceSchema getSchema(String path, Job job) throws IOException
  {
    try
    {
      ResourceSchema result = null;
      AutoFeatureInputFormat input = new AutoFeatureInputFormat();
      job.getConfiguration().set("mapred.input.dir", path);
      List<InputSplit> splits = input.getSplits(job);
      RecordReader<LongWritable, Geometry> reader = input.createRecordReader(splits.get(0),
        HadoopUtils.createTaskAttemptContext(job.getConfiguration(), new TaskAttemptID()));

      if (reader.nextKeyValue())
      {
        result = new ResourceSchema();
        Geometry f = reader.getCurrentValue();

        Map<String, String> attrs = f.getAllAttributesSorted();

        ResourceFieldSchema[] fields = new ResourceFieldSchema[attrs.size()];

        int ndx = 0;
        for (Map.Entry<String, String> attr: attrs.entrySet())
        {
          fields[ndx] = new ResourceFieldSchema();
          fields[ndx].setName(attr.getKey());

          String val = attr.getValue();

          try
          {
            Double.parseDouble(val);
            fields[ndx].setType(DataType.DOUBLE);
          }
          catch (NumberFormatException e)
          {
            try
            {
              Integer.parseInt(val);
              fields[ndx].setType(DataType.INTEGER);
            }
            catch (NumberFormatException e2)
            {
              fields[ndx].setType(DataType.CHARARRAY);
            }
          }

          ndx++;
        }

        result.setFields(fields);
      }

      return result;
    }
    catch (InterruptedException e)
    {
      throw new IOException(e);
    }
  }

  @Override
  public ResourceStatistics getStatistics(String arg0, Job arg1) throws IOException
  {
    // not needed.
    return null;
  }

  @Override
  public void setPartitionFilter(Expression arg0) throws IOException
  {
    // not needed.
  }
}
