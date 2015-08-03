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
import org.mrgeo.image.MrsImage;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.mrgeo.mapreduce.splitters.MrsPyramidInputSplit;
import org.mrgeo.pyramid.MrsPyramidMetadata;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.DataProviderNotFound;
import org.mrgeo.data.MrsPyramidRecordReader;
import org.mrgeo.data.raster.RasterUtils;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.MrsTileReader;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.data.tile.TiledInputFormatContext;
import org.mrgeo.utils.HadoopUtils;
import org.mrgeo.utils.TMSUtils;

import java.awt.image.Raster;
import java.awt.image.WritableRaster;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * This class is the Hadoop RecordReader returned from the InputFormat configured
 * into map/reduce jobs that get their input from image pyramids. It uses the data
 * access layer to get access to the underlying image.
 */
public class MrsImagePyramidRecordReader extends MrsPyramidRecordReader<Raster, RasterWritable>
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
      MrsImageDataProvider dp = DataProviderFactory.getMrsImageDataProvider(imageName, AccessMode.READ,
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
  protected Raster splitTile(Raster tile, long id, int zoom, long childTileid, int childZoomLevel,
      int tilesize)
  {
    // get the lat/lon of the child tile
    final TMSUtils.Tile ct = TMSUtils.tileid(childTileid, childZoomLevel);
    final TMSUtils.Bounds childBounds = TMSUtils.tileBounds(ct.tx, ct.ty, childZoomLevel, tilesize);

    // calculate the offset in the raster for the child lat/lon
    final TMSUtils.Tile pt = TMSUtils.tileid(id, zoom);
    final TMSUtils.Pixel swOffset = TMSUtils.latLonToTilePixelUL(childBounds.s, childBounds.w,
        pt.tx, pt.ty, zoom, tilesize);

    // calculate the scale factor (1 pixel = <scale> child pixels)
    final int scale = (int) Math.pow(2.0, (childZoomLevel - zoom));

    // calculate the size of the child tile within the parent area
    final int cropsize = tilesize / scale;

    final Raster cropped = tile.createChild((int) swOffset.px, (int) (swOffset.py + 1) - cropsize,
        cropsize, cropsize, 0, 0, null);

    return RasterUtils.scaleRasterNearest(cropped, tilesize, tilesize);
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
  protected MrsTileReader<Raster> getMrsTileReader(final String name, int zoomlevel,
      final Configuration conf) throws DataProviderNotFound, IOException
  {
    MrsImageDataProvider dp = DataProviderFactory.getMrsImageDataProvider(name,
        AccessMode.READ, conf);
    
    MrsImagePyramidReaderContext context = new MrsImagePyramidReaderContext();
    context.setZoomlevel(zoomlevel);
    return dp.getMrsTileReader(context);
  }

  @Override
  protected RecordReader<TileIdWritable, RasterWritable> getRecordReader(
      final String name, final Configuration conf) throws DataProviderNotFound
  {
    MrsImageDataProvider dp = DataProviderFactory.getMrsImageDataProvider(name,
        AccessMode.READ, conf);
    return dp.getRecordReader();
  }

  @Override
  protected Raster createBlankTile(final double fill)
  {
    try
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
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }

    return null;
  }

  @Override
  protected RasterWritable toWritable(Raster val) throws IOException
  {
    return RasterWritable.toWritable(val);
  }
}
