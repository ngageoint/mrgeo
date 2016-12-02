/*
 * Copyright 2009-2016 DigitalGlobe, Inc.
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
 *
 */

package org.mrgeo.hdfs.output.image;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.mrgeo.data.tile.TileIdWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HdfsMrsPyramidOutputFormat extends FileOutputFormat<WritableComparable<?>, Writable>
{
private static final Logger log = LoggerFactory.getLogger(HdfsMrsPyramidOutputFormat.class);

@Override
public RecordWriter<WritableComparable<?>, Writable> getRecordWriter(TaskAttemptContext context) throws IOException
{
  CompressionCodec codec = null;
  SequenceFile.CompressionType compressionType = SequenceFile.CompressionType.NONE;
  if (getCompressOutput(context))
  {
    // find the kind of compression to do
    compressionType = SequenceFileOutputFormat.getOutputCompressionType(context);

    // find the right codec
    codec = getCompressionCodec(context);
  }

  Path file = getDefaultWorkFile(context, "");

  final MapFile.Writer out = createMapFileWriter(context, codec, compressionType, file);

  return new Writer(out);
}

protected MapFile.Writer createMapFileWriter(TaskAttemptContext context, CompressionCodec codec,
    SequenceFile.CompressionType compressionType, Path file) throws IOException
{
  return new MapFile.Writer(context.getConfiguration(), file,
      MapFile.Writer.keyClass(context.getOutputKeyClass().asSubclass(WritableComparable.class)),
      MapFile.Writer.valueClass(context.getOutputValueClass().asSubclass(Writable.class)),
      MapFile.Writer.compression(compressionType, codec),
      MapFile.Writer.progressable(context));
}

protected CompressionCodec getCompressionCodec(TaskAttemptContext context)
{
  CompressionCodec codec;
  Class<?> codecClass = getOutputCompressorClass(context,
      DefaultCodec.class);
  codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, context.getConfiguration());
  return codec;
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
      tileid.set(((TileIdWritable) key).get());
      out.append(tileid, value);
    }
    else
    {
      out.append(key, value);
    }
  }

  @Override
  public void close(TaskAttemptContext contxt) throws IOException
  {
    out.close();
  }
}
}
