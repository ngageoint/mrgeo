/*
 * Copyright 2009-2014 DigitalGlobe, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
package org.mrgeo.format;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.ReflectionUtils;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.utils.Base64Utils;
import org.mrgeo.utils.HadoopUtils;
import org.mrgeo.utils.LeakChecker;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;

/**
 * Automatically determines a file type based on the extension and opens an
 * appropriate input format.
 */
public class AutoFeatureInputFormat extends InputFormat<LongWritable, Geometry> implements
    Writable
{
  @Override
  public RecordReader<LongWritable, Geometry> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException
  {
    RecordReader<LongWritable, Geometry> result = new AutoRecordReader();
    result.initialize(split, context);
    return result;
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException
  {
    Path[] paths = FileInputFormat.getInputPaths(context);

    List<InputSplit> result = new LinkedList<InputSplit>();
    Configuration conf = context.getConfiguration();

    // expand the wild cards and add up the total size by recursing.
    Vector<Path> expanded = new Vector<Path>();
    long totalSize = 0;
    for (Path p : paths)
    {
      FileSystem fs = p.getFileSystem(conf);
      FileStatus[] status = fs.globStatus(p);
      for (FileStatus s : status)
      {
        totalSize += HadoopFileUtils.getPathSize(conf, s.getPath());
        if (s.getPath() != null)
        {
          expanded.add(s.getPath());
        }
      }
    }
    paths = expanded.toArray(paths);

    // create the individual splits
    String inputs = conf.get("mapred.input.dir", "");
    int totalSplits = conf.getInt("mapred.map.tasks", 2);
    
    for (Path p : paths)
    {
      double portion = HadoopFileUtils.getPathSize(conf, p) / totalSize;
      int splits = (int) Math.max(1, Math.round(portion * totalSplits));
      conf.setInt("mapred.map.tasks", splits);
      conf.set("mapred.input.dir", p.toString());
      InputFormat<LongWritable, Geometry> f = 
        FeatureInputFormatFactory.getInstance().createInputFormat(p.toString());
      for (InputSplit s : f.getSplits(context))
      {
        AutoInputSplit ais = new AutoInputSplit(conf, s, f);
        result.add(ais);
      }
    }
    conf.setInt("mapred.map.tasks", totalSplits);
    conf.set("mapred.input.dir", inputs);

    return result;
  }

  static public class AutoRecordReader extends RecordReader<LongWritable, Geometry>
  {
    AutoInputSplit split;
    RecordReader<LongWritable, Geometry> reader = null;

    final private boolean profile;
    
    AutoRecordReader()
    {
      if (System.getProperty("mrgeo.profile", "false").compareToIgnoreCase("true") == 0)
      {
        profile = true;
        LeakChecker.instance().add(this, ExceptionUtils.getFullStackTrace(new Throwable("AutoRecordReader creation stack(ignore the Throwable...)")));
      }
      else
      {
        profile = false;
      }

    }

    @Override
    public void close() throws IOException
    {
      if (profile)
      {
        LeakChecker.instance().remove(this);
      }
      reader.close();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException
    {
      return reader.getProgress();
    }

    @Override
    public void initialize(InputSplit splt, TaskAttemptContext context) throws IOException, InterruptedException
    {
      this.split = (AutoInputSplit) splt;
      reader = this.split.format.createRecordReader(this.split.getSplit(), context);
      //FIXME: This seems to be called from createRecordReader()
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
    public Geometry getCurrentValue() throws IOException, InterruptedException
    {
      return reader.getCurrentValue();
    }

  }

  public static class AutoInputSplit extends InputSplit implements Writable
  {
    @SuppressWarnings("unused")
    private static final long serialVersionUID = 1L;
    private transient InputSplit split;
    transient InputFormat<LongWritable, Geometry> format;
    private transient Configuration conf;

    public AutoInputSplit()
    {
    }

    public AutoInputSplit(Configuration conf, InputSplit split, InputFormat<LongWritable, Geometry> format)
    {
      if (split instanceof Serializable == false && split instanceof FileSplit == false)
      {
        throw new IllegalArgumentException("Only serializable splits are supported. ("
            + format.getClass().getName() + ")");
      }
      this.conf = conf;
      this.split = split;
      this.format = format;
    }

    public InputFormat<LongWritable, Geometry> getFormat()
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

    @Override
    public void readFields(DataInput in) throws IOException
    {
      try
      {
        if (conf == null)
        {
          conf = HadoopUtils.createConfiguration();
          System.out.println("Creating new configuration in AutoINputSplit.readFields()");
        }
        Class<?> c = conf.getClassByName(in.readUTF());
        format = (InputFormat<LongWritable, Geometry>) c.newInstance();
        int obj = in.readInt();
        if (obj == 0)
        {
          split = (InputSplit) Base64Utils.decodeToObject(in.readUTF());
        }
        else if (obj == 1)
        {
          c = conf.getClassByName(in.readUTF());
          split = (InputSplit) ReflectionUtils.newInstance(c, conf);
          Writable ws = (Writable) split;
          ws.readFields(in);
        }
        else
        {
          Path p = new Path(in.readUTF());
          long length = in.readLong();
          long start = in.readLong();
          String[] hosts = (String[]) Base64Utils.decodeToObject(in.readUTF());
          split = new FileSplit(p, start, length, hosts);
        }
      }
      catch (Exception e)
      {
        throw new IOException(e);
      }
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
      out.writeUTF(format.getClass().getName());
      if (split instanceof Serializable)
      {
        out.writeInt(0);
        out.writeUTF(Base64Utils.encodeObject(split));
      }
      else if (split instanceof Writable)
      {
        out.writeInt(1);
        out.writeUTF(split.getClass().getName());
        Writable ws = (Writable) split;
        ws.write(out);
      }
      else
      {
        out.writeInt(2);
        FileSplit fs = (FileSplit) split;
        out.writeUTF(fs.getPath().toString());
        out.writeLong(fs.getLength());
        out.writeLong(fs.getStart());
        out.writeUTF(Base64Utils.encodeObject(fs.getLocations()));
      }
    }
  }

  @Override
  public void readFields(DataInput arg0) throws IOException
  {
  }

  @Override
  public void write(DataOutput arg0) throws IOException
  {
  }
}
