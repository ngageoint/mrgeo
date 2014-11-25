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

package org.mrgeo.hdfs.input.image;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;
import org.mrgeo.mapreduce.splitters.TiledInputSplit;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;

import java.io.IOException;

public class HDFSMrsImagePyramidRecordReader extends SequenceFileRecordReader<TileIdWritable, RasterWritable>
{

  @Override
  public TileIdWritable getCurrentKey()
  {
    // TODO Auto-generated method stub
    return super.getCurrentKey();
  }

  @Override
  public RasterWritable getCurrentValue()
  {
    // TODO Auto-generated method stub
    return super.getCurrentValue();
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException,
      InterruptedException
  {
    if (split instanceof TiledInputSplit)
    {
      super.initialize(((TiledInputSplit)split).getWrappedSplit(), context);
    }
    else
    {
      // Should never happen
      super.initialize(split, context);
    }
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException
  {
    // TODO Auto-generated method stub
    return super.nextKeyValue();
  }
}
