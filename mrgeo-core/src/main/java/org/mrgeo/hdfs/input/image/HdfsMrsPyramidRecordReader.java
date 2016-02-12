/*
 * Copyright 2009-2015 DigitalGlobe, Inc.
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

package org.mrgeo.hdfs.input.image;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
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
  private SequenceFile.Reader reader;
  private TileIdWritable key;
  private RasterWritable value;
  private long startTileId;
  private long endTileId;
  private long recordCount = 0;
  private boolean more = true;

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
    if (split instanceof TiledInputSplit)
    {
      TiledInputSplit tiledInputSplit = (TiledInputSplit)split;
      startTileId = tiledInputSplit.getStartTileId();
      endTileId = tiledInputSplit.getEndTileId();
      FileSplit fileSplit = (FileSplit)((TiledInputSplit)split).getWrappedSplit();
      Configuration conf = context.getConfiguration();
      Path path = fileSplit.getPath();
      FileSystem fs = path.getFileSystem(conf);
      this.reader = new SequenceFile.Reader(fs, path, conf);
      try
      {
        this.key = (TileIdWritable)reader.getKeyClass().newInstance();
        this.value = (RasterWritable)reader.getValueClass().newInstance();
      }
      catch (InstantiationException e)
      {
        e.printStackTrace();
      }
      catch (IllegalAccessException e)
      {
        e.printStackTrace();
      }
    }
    else
    {
      throw new IOException("Expected a TiledInputSplit but received " + split.getClass().getName());
    }
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException
  {
    if (more)
    {
      more = reader.next(key, value);
      if (!more) {
        key = null;
        value = null;
      }
    }
    return more;
  }
}
