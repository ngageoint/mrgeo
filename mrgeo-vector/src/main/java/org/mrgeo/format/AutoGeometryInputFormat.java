/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.format;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.mrgeo.mapreduce.GeometryWritable;
import org.mrgeo.hdfs.utils.HadoopFileUtils;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

/**
 * Automatically determines a file type based on the extension and opens an
 * appropriate input format.
 */
public class AutoGeometryInputFormat extends InputFormat<LongWritable, GeometryWritable> implements
    Serializable
{
  private static final long serialVersionUID = 1L;

  @Override
  public RecordReader<LongWritable, GeometryWritable> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException
  {
    RecordReader<LongWritable, GeometryWritable> result = new AutoRecordReader();
    result.initialize(split, context);
    return result;
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException
  {
    Path[] paths = FileInputFormat.getInputPaths(context);
    List<InputSplit> result = new LinkedList<InputSplit>();

    Configuration conf = context.getConfiguration();
    long totalSize = 0;
    for (Path p : paths)
    {
      totalSize += HadoopFileUtils.getPathSize(conf, p);
    }

    String inputs = conf.get("mapred.input.dir");
    int totalSplits = conf.getInt("mapred.map.tasks", 2);
    for (Path p : paths)
    {
      double portion = HadoopFileUtils.getPathSize(conf, p) / totalSize;
      int splits = (int) Math.max(1, Math.round(portion * totalSplits));
      conf.setInt("mapred.map.tasks", splits);
      conf.set("mapred.input.dir", p.toString());
      InputFormat<LongWritable, GeometryWritable> f = GeometryInputFormatFactory.getInstance()
          .createReader(p);
      for (InputSplit s : f.getSplits(context))
      {
        result.add(new AutoInputSplit(s, f));
      }
    }
    conf.setInt("mapred.map.tasks", totalSplits);
    conf.set("mapred.input.dir", inputs);

    return result;
  }

  static public class AutoRecordReader extends RecordReader<LongWritable, GeometryWritable>
  {
    AutoInputSplit split;
    RecordReader<LongWritable, GeometryWritable> reader = null;

    AutoRecordReader()
    {
    }

    @Override
    public void close() throws IOException
    {
      reader.close();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException
    {
      return reader.getProgress();
    }

    @SuppressWarnings("hiding")
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException
    {
      this.split = (AutoInputSplit) split;
      reader = this.split.format.createRecordReader(this.split.getSplit(), context);
      reader.initialize(this.split.getSplit(), context);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException
    {
      return reader.nextKeyValue();
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException
    {
      return reader.getCurrentKey();
    }

    @Override
    public GeometryWritable getCurrentValue() throws IOException, InterruptedException
    {
      return reader.getCurrentValue();
    }

  }

  public class AutoInputSplit extends InputSplit implements Serializable
  {
    private static final long serialVersionUID = 1L;
    private transient InputSplit split;
    transient InputFormat<LongWritable, GeometryWritable> format;

    AutoInputSplit(InputSplit split, InputFormat<LongWritable, GeometryWritable> format)
    {
      if (split instanceof Serializable == false && split instanceof FileSplit == false)
      {
        throw new IllegalArgumentException("Only serializable splits are supported. ("
            + format.getClass().getName() + ")");
      }
      this.split = split;
      this.format = format;
    }

    public InputFormat<LongWritable, GeometryWritable> getFormat()
    {
      return format;
    }

    @Override
    public long getLength() throws IOException, InterruptedException
    {
      return split.getLength();
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException
    {
      return split.getLocations();
    }

    public InputSplit getSplit()
    {
      return split;
    }

    private void readObject(ObjectInputStream stream) throws ClassNotFoundException, IOException
    {
      stream.defaultReadObject();
      Class<?> c = (Class<?>) stream.readObject();
      try
      {
        format = (InputFormat<LongWritable, GeometryWritable>) c.newInstance();
      }
      catch (Exception e)
      {
        throw new IOException(e);
      }
      int obj = stream.readInt();
      if (obj == 1)
      {
        split = (InputSplit) stream.readObject();
      }
      else
      {
        Path p = new Path((String) stream.readObject());
        long length = stream.readLong();
        long start = stream.readLong();
        String[] hosts = (String[]) stream.readObject();
        split = new FileSplit(p, start, length, hosts);
      }
    }

    private void writeObject(ObjectOutputStream stream) throws IOException
    {
      stream.defaultWriteObject();
      stream.writeObject(format.getClass());
      if (split instanceof Serializable)
      {
        stream.writeInt(1);
        stream.writeObject(split);
      }
      else
      {
        stream.writeInt(0);
        FileSplit fs = (FileSplit) split;
        stream.writeObject(fs.getPath().toString());
        stream.writeLong(fs.getLength());
        stream.writeLong(fs.getStart());
        stream.writeObject(fs.getLocations());
      }
    }
  }
}
