/*
 * Copyright 2009-2017. DigitalGlobe, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.mrgeo.hdfs.input.image;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.mapreduce.splitters.TiledInputSplit;

import java.io.IOException;

public class HdfsMrsPyramidRecordReader extends RecordReader<TileIdWritable, RasterWritable>
{
private Reader reader;
private TileIdWritable key;
private RasterWritable value;
private long startTileId;
private long endTileId;
private long recordCount;
private boolean more = true;

// Factory for creating instances of SequenceFile.Reader
private ReaderFactory readerFactory;

// Default constructor injects Default Reader Factory
public HdfsMrsPyramidRecordReader()
{
  readerFactory = new ReaderFactory();
}

// Constructor for injecting a ReaderFactory implementation
public HdfsMrsPyramidRecordReader(ReaderFactory readerFactory)
{
  this.readerFactory = readerFactory;
}

@Override
public TileIdWritable getCurrentKey()
{
  return key;
}

@Override
public RasterWritable getCurrentValue()
{
  return value;
}

@Override
// TODO eaw this should probably throw an UnsupportedOperationException since it does not meet the contract on getProgress because recordCount is never updated
public float getProgress() throws IOException, InterruptedException
{
  if (startTileId == endTileId)
  {
    return 0.0f;
  }
  else
  {
    return Math.min(1.0f, (recordCount - startTileId) / (float) (endTileId - startTileId));
  }
}

@Override
public void close() throws IOException
{
  reader.close();
}

@Override
public void initialize(InputSplit split, TaskAttemptContext context) throws IOException,
    InterruptedException
{
  // TODO eaw - Better to use isAssignableFrom so it doesn't break if TiledInputSplit is ever subclassed
  if (split instanceof TiledInputSplit)
  {
    TiledInputSplit tiledInputSplit = (TiledInputSplit) split;
    startTileId = tiledInputSplit.getStartTileId();
    endTileId = tiledInputSplit.getEndTileId();
    // TODO, can use tiledInputSplit instead of casting split again
    FileSplit fileSplit = (FileSplit) ((TiledInputSplit) split).getWrappedSplit();
    Configuration conf = context.getConfiguration();
    Path path = fileSplit.getPath();
    FileSystem fs = path.getFileSystem(conf);

    // Use a factory to create the reader reader to make this class easier to test and support decoupling the reader
    // lifecycle from this object's lifecycle.
    reader = readerFactory.createReader(fs, path, conf);

    try
    {
      key = (TileIdWritable) reader.getKeyClass().newInstance();
      value = (RasterWritable) reader.getValueClass().newInstance();
    }
    catch (InstantiationException | IllegalAccessException e)
    {
      throw new IOException(e);
    }
  }
  else
  {
    // TODO eaw - IllegalArgumentException would be more appropriate here
    throw new IOException("Expected a TiledInputSplit but received " + split.getClass().getName());
  }
}

@Override
public boolean nextKeyValue() throws IOException, InterruptedException
{
  // TODO eaw evaluate whether it is needed to store more as an instance member.  If not, use a local variable instead
  if (more)
  {
    more = reader.next(key, value);
    if (!more)
    {
      key = null;
      value = null;
    }
    else {
      recordCount++;
    }
  }
  return more;
}

// Default ReaderFactory
static class ReaderFactory
{
  public Reader createReader(FileSystem fs, Path path, Configuration config) throws IOException
  {
    return new Reader(fs, path, config);
  }
}
}
