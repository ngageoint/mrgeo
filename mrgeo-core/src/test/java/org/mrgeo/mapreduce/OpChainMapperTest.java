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

package org.mrgeo.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.image.ImageStats;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.mrgeo.mapreduce.formats.EmptyTileInputFormat;
import org.mrgeo.mapreduce.formats.TileCollection;
import org.mrgeo.opimage.OpChainMapperTestDescriptor;
import org.mrgeo.rasterops.OpImageRegistrar;
import org.mrgeo.data.raster.RasterUtils;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.utils.HadoopUtils;
import org.mrgeo.utils.TMSUtils;
import org.mrgeo.utils.TMSUtils.Tile;

import java.awt.*;
import java.awt.image.*;
import java.io.IOException;

@SuppressWarnings("static-method")
public class OpChainMapperTest
{
  private static final double EPSILON = 1e-8;

  private int size;
  private int zoom;
  private double lat, lon;
  
  private static int width;
  private static int height;
  private static SampleModel sm;
  private static WritableRaster r;

  @Before
  public void init() throws Exception
  {
    width = 4;
    height = 4;
    zoom = 5;
    size = 4;
    lat = -17.5;
    lon = 142.3;
    sm = new BandedSampleModel(DataBuffer.TYPE_DOUBLE, width, height, 1);
    r = Raster.createWritableRaster(sm, new Point(0, 0));
    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        r.setSample(x, y, 0, 0.0);
      }
    }
  }

  @Before
  public void setUp()
  {
    OpImageRegistrar.registerMrGeoOps();
  }

  @Test
  @Category(UnitTest.class)
  public void mapTest() throws IOException
  {
    Configuration config = new Configuration();
    config.setInt(OpChainMapper.ZOOM_LEVEL, zoom);
    config.setInt(OpChainMapper.TILE_SIZE, size);

    MrsImagePyramidMetadata metadata = new MrsImagePyramidMetadata();
    metadata.setTilesize(size);
    metadata.setBands(1);
    metadata.setMaxZoomLevel(zoom);
    metadata.setDefaultValues(new double[] {Double.NaN});
    metadata.setPyramid(EmptyTileInputFormat.EMPTY_IMAGE);

    try
    {
      HadoopUtils.setMetadata(config, metadata);
    }
    catch (IOException e)
    {
      e.printStackTrace();
      Assert.fail("Failed to set metadata to Hadoop configuration.");
    }

    ColorModel cm = RasterUtils.createColorModel(r);
    BufferedImage bi = new BufferedImage(cm, r, false, null);
    RenderedImage op = OpChainMapperTestDescriptor.create(bi);

    OpChainMapper.setOperation(config, op);
    // Tile Id for child
    Tile tile = TMSUtils.latLonToTile(lat, lon, zoom, size);
    long tileId = TMSUtils.tileid(tile.tx, tile.ty, zoom);
    TileIdWritable key = new TileIdWritable(tileId);

    TileCollection<Raster> value = new TileCollection<Raster>();
    value.setTileid(key.get());
    value.set(EmptyTileInputFormat.EMPTY_IMAGE, key.get(), r);

    MapDriver<TileIdWritable, TileCollection<Raster>, TileIdWritable, RasterWritable> driver = 
        new MapDriver<TileIdWritable, TileCollection<Raster>, TileIdWritable, RasterWritable>()
        .withConfiguration(config)
        .withMapper(new OpChainMapper())
        .withInputKey(key)
        .withInputValue(value);

    java.util.List<Pair<TileIdWritable, RasterWritable>> results = driver.run();

    // One of the results is our tile, and the other is statistics
    Assert.assertEquals(2, results.size());
    for (Pair<TileIdWritable, RasterWritable> entry: results)
    {
      if (entry.getFirst().get() == tileId)
      {
        Raster outputRaster = RasterWritable.toRaster(entry.getSecond());
        Assert.assertEquals(width, outputRaster.getWidth());
        Assert.assertEquals(height, outputRaster.getHeight());
        // All pixel values for the image should be the worldwide tile id.
        for (int x=0; x < outputRaster.getWidth(); x++)
        {
          for (int y=0; y < outputRaster.getHeight(); y++)
          {
            double v = outputRaster.getSampleDouble(x, y, 0);
            Assert.assertEquals(tileId, v, EPSILON);
          }
        }
      }
      else if (entry.getFirst().get() != ImageStats.STATS_TILE_ID)
      {
        Assert.fail("Got bad output: " + entry.getFirst().get());
      }
    }
  }
}
