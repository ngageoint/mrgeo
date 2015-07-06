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

package org.mrgeo.data.image;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderNotFound;
import org.mrgeo.data.MrsPyramidSimpleRecordReader;
import org.mrgeo.data.raster.RasterUtils;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.MrsTileReader;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.data.tile.TiledInputFormatContext;
import org.mrgeo.image.MrsImage;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.mrgeo.mapreduce.splitters.MrsPyramidInputSplit;
import org.mrgeo.pyramid.MrsPyramidMetadata;
import org.mrgeo.utils.HadoopUtils;
import org.mrgeo.utils.TMSUtils;

import java.awt.image.Raster;
import java.awt.image.WritableRaster;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MrsImagePyramidSimpleRecordReader extends MrsPyramidSimpleRecordReader<Raster, RasterWritable>
{
  private MrsImage image;

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException,
          InterruptedException
  {
    super.initialize(split, context);
    if (split instanceof MrsPyramidInputSplit)
    {
      TiledInputFormatContext ifContext = TiledInputFormatContext.load(context.getConfiguration());
      String imageName = ((MrsPyramidInputSplit)split).getName();
      MrsImageDataProvider dp = DataProviderFactory.getMrsImageDataProvider(imageName, DataProviderFactory.AccessMode.READ,
              context.getConfiguration());
      image = MrsImage.open(dp, ifContext.getZoomLevel());
    }
    else
    {
      throw new IOException("Got a split of type " + split.getClass().getCanonicalName() +
              " but expected one of type " + MrsPyramidInputSplit.class.getCanonicalName());
    }
  }

  @Override
  public void close() throws IOException
  {
    image.close();
    super.close();
  }

  @Override
  protected Raster toNonWritableTile(RasterWritable tileValue) throws IOException
  {
    return RasterWritable.toRaster(tileValue);
  }

  @Override
  protected Map<String, MrsPyramidMetadata> readMetadata(Configuration conf)
          throws ClassNotFoundException, IOException
  {
    Map<String, MrsImagePyramidMetadata> m = HadoopUtils.getMetadata(conf);
    Map<String, MrsPyramidMetadata> results = new HashMap<String, MrsPyramidMetadata>(m.size());
    for (String key : m.keySet())
    {
      results.put(key, (MrsPyramidMetadata)m.get(key));
    }
    return results;
  }

  @Override
  protected RecordReader<TileIdWritable, RasterWritable> getRecordReader(
          final String name, final Configuration conf) throws DataProviderNotFound
  {
    MrsImageDataProvider dp = DataProviderFactory.getMrsImageDataProvider(name,
            DataProviderFactory.AccessMode.READ, conf);
    return dp.getRecordReader();
  }

  @Override
  protected Raster createBlankTile(final double fill)
  {
    Raster anyTile = image.getAnyTile();

    if (anyTile != null)
    {
      WritableRaster wr = anyTile.createCompatibleWritableRaster();

      for (int y = wr.getMinY(); y < wr.getMinY() + wr.getHeight(); y++)
      {
        for (int x = wr.getMinX(); x < wr.getMinX() + wr.getWidth(); x++)
        {
          for (int b = 0; b < wr.getNumBands(); b++)
          {
            wr.setSample(x, y, b, fill);
          }
        }
      }
      return wr.createTranslatedChild(0, 0);
    }
    return null;
  }

  @Override
  protected RasterWritable toWritable(Raster val) throws IOException
  {
    return RasterWritable.toWritable(val);
  }

  @Override
  protected RasterWritable copyWritable(RasterWritable val)
  {
    return new RasterWritable(val);
  }
}
