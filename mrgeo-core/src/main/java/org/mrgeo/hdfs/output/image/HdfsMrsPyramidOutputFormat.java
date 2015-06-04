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

package org.mrgeo.hdfs.output.image;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.hdfs.output.MapFileOutputFormat;

import java.io.IOException;

public class HdfsMrsPyramidOutputFormat extends MapFileOutputFormat
{
  public static void setInfo(final Job job)
  {
    // index every entry
    job.getConfiguration().set("io.map.index.interval", "1");

    // compress at a record level
    SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.RECORD);
  }

  public static void setOutputInfo(final Job job, final String output)
  {
    setInfo(job);
    FileOutputFormat.setOutputPath(job, new Path(output));
  }

  @Override
  public RecordWriter<WritableComparable<?>, Writable> getRecordWriter(TaskAttemptContext context) throws IOException
  {
    // this RecordWriter is copied from the MapFileOutputFormat, with the addition of
    // the write() method checking for TileIdWritable.  We needed to copy the code
    // because the RecordWriter is actually inline.  yuck!
    Configuration conf = context.getConfiguration();
    CompressionCodec codec = null;
    SequenceFile.CompressionType compressionType = SequenceFile.CompressionType.NONE;
    if (getCompressOutput(context)) {
      // find the kind of compression to do
      compressionType = SequenceFileOutputFormat.getOutputCompressionType(context);

      // find the right codec
      Class<?> codecClass = getOutputCompressorClass(context,
          DefaultCodec.class);
      codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
    }

    Path file = getDefaultWorkFile(context, "");

    final MapFile.Writer out =  new MapFile.Writer(conf, file,
        MapFile.Writer.keyClass(context.getOutputKeyClass().asSubclass(WritableComparable.class)),
        MapFile.Writer.valueClass(context.getOutputValueClass().asSubclass(Writable.class)),
        MapFile.Writer.compression(compressionType, codec),
        MapFile.Writer.progressable(context));

    return new Writer(out);
  }

  private static class Writer extends RecordWriter<WritableComparable<?>, Writable>
  {
    private final MapFile.Writer out;
    private TileIdWritable tileid;

    public Writer(MapFile.Writer out)
    {
      this.out = out;
      tileid = new TileIdWritable();
    }

    @Override
    public void write(WritableComparable<?> key, Writable value)
        throws IOException
    {

      // there may ba a case or two where an extended TileIdWritable is written as the key
      // (buildpyramid does it).  So we strip out any of that information when we write the
      // actual key.
      if (key instanceof TileIdWritable)
      {
        tileid.set(((TileIdWritable)key).get());
        out.append(tileid, value);
      }
      else
      {
        out.append(key, value);
      }
    }

    @Override
    public void close(TaskAttemptContext contxt) throws IOException {
      out.close();
    }
  }
}
