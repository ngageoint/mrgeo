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
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.KVIterator;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.data.image.MrsImagePyramidReaderContext;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.MrsTileReader;
import org.mrgeo.data.tile.TileIdWritable;

import java.awt.image.Raster;
import java.io.IOException;
import java.util.Properties;

public class HdfsMrsImagePyramidSimpleRecordReader extends RecordReader<TileIdWritable, RasterWritable>
{
  private MrsTileReader<Raster> reader = null;
  private KVIterator<TileIdWritable, Raster> iter = null;

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException
  {
    if (split instanceof SimplePyramidInputSplit)
    {
      SimplePyramidInputSplit tiledSplit = (SimplePyramidInputSplit)split;

      MrsImageDataProvider dp = DataProviderFactory.getMrsImageDataProvider(tiledSplit.pyramid,
          DataProviderFactory.AccessMode.READ, new Properties());
      MrsImagePyramidReaderContext readerContext = new MrsImagePyramidReaderContext();
      readerContext.setZoomlevel(tiledSplit.zoom);

      reader = dp.getMrsTileReader(readerContext);

      iter = reader.get(new TileIdWritable(tiledSplit.startId), new TileIdWritable(tiledSplit.endId));
    }
    else
    {
      throw new IOException("Bad split type");
    }
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException
  {
    if (iter.hasNext())
    {
      iter.next();
      return true;
    }

    return false;
  }

  @Override
  public TileIdWritable getCurrentKey() throws IOException, InterruptedException
  {
    return iter.currentKey();
  }

  @Override
  public RasterWritable getCurrentValue() throws IOException, InterruptedException
  {
    return RasterWritable.toWritable(iter.currentValue());
  }

  @Override
  public float getProgress() throws IOException, InterruptedException
  {
    return 0;
  }

  @Override
  public void close() throws IOException
  {
    if (reader != null)
    {
      reader.close();
    }
  }
}
