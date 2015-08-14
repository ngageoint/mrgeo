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

package org.mrgeo.test;

import org.mrgeo.rasterops.OpImageRegistrar;
import org.mrgeo.rasterops.OpImageUtils;
import org.mrgeo.utils.TMSUtils;

import javax.media.jai.TiledImage;
import java.awt.*;
import java.awt.image.BandedSampleModel;
import java.awt.image.DataBuffer;
import java.awt.image.SampleModel;
import java.io.IOException;

/**
 * This class contains helper methods for unit testing OpImage classes.
 * Currently, it creates in memory multiple 30x30 pixel images composed
 * of nine 10x10 tiles. This makes it possible to perform unit tests on
 * OpImage classes that require a neighborhood of pixels in order to
 * perform their computations. The computation is only executed on the
 * center tile with starting point 0, 0 and width and height both 10.
 * 
 * There are six different images available in two sets of three. The first
 * set of three are single-band images containing double float values set
 * to 2.0. The second set of three images is the same size/type of image,
 * but the pixel values are set to a pixel id multiplied by 100000.0. The
 * pixel id's are number from left -> right and top -> bottom starting
 * with 1. So the end result is the first pixel is 100000.0, the second is
 * 200000.0, etc...
 * 
 * In each grouping of three, the first image is as described above. The
 * second image sets the value of every seventh pixel to the noData value
 * assigned to NON_NAN_NODATA_VALUE. The third image of each group is the
 * same as the second except that the noData value used is Double.NaN.
 */
public class OpImageTestUtils extends TestUtils
{
  public static final int noDataModValue = 7;
  public static final double NON_NAN_NODATA_VALUE = -32767.0;
  private SampleModel sm;
  public int width;
  public int height;
  public int tileWidth;
  public int tileHeight;
  public int minX;
  public int minY;
  public int minXForTile;
  public int minYForTile;
  public TiledImage twos;
  public TiledImage twosWithNoData;
  public TiledImage twosWithNanNoData;
  public TiledImage numbered;
  public TiledImage numberedWithNoData;
  public TiledImage numberedWithNanNoData;
  public Rectangle destRect;
  public int zoom;
  public long tx;
  public long ty;
public TestUtils.ValueTranslator nanTranslatorToMinus9999;
  public TestUtils.ValueTranslator nanTranslatorToMinus32767;

  public OpImageTestUtils(final Class<?> testClass) throws IOException
  {
    super(testClass);

    OpImageRegistrar.registerMrGeoOps();
    init();
  }

  public static class NaNTranslator implements TestUtils.ValueTranslator
  {

    private float translateTo;

    public NaNTranslator(float translateTo)
    {
      this.translateTo = translateTo;
    }

    @Override
    public float translate(float value)
    {
      float result = value;
      if (Float.isNaN(value))
      {
        result = translateTo;
      }
      return result;
    }
  }

  public int getPixelId(int x, int y)
  {
    // Shift the x/y by a tileWidth/Height to get rid of negative x/y values (since
    // x and y start at -tileWidth and -tileheight).
    return (tileWidth + x) + ((tileHeight + y) * width);
  }

  private void init()
  {
    // The baseline (golden) images have nodata values saved as -9999.0. We use the
    // following translators during image comparisons to convert nodata values in the
    // generated images to -9999.0 so they match up.
    nanTranslatorToMinus9999 = new NaNTranslator(-9999.0f);
    nanTranslatorToMinus32767 = new NaNTranslator((float)NON_NAN_NODATA_VALUE);
    tileWidth = 10;
    tileHeight = 10;
    width = 3 * tileWidth;
    height = 3 * tileHeight;
    minX = -10;
    minY = -10;
    minXForTile = 0;
    minYForTile = 0;
    zoom = 4;
    TMSUtils.Tile tile = TMSUtils.latLonToTile(0.0, 0.0, zoom, tileWidth);
    tx = tile.tx;
    ty = tile.ty;
    sm = new BandedSampleModel(DataBuffer.TYPE_DOUBLE, width, height, 1);
    numbered = new TiledImage(new Point(minX, minY), sm, tileWidth, tileHeight);
    numberedWithNoData = new TiledImage(new Point(minX, minY), sm, tileWidth, tileHeight);
    numberedWithNoData.setProperty(OpImageUtils.NODATA_PROPERTY, NON_NAN_NODATA_VALUE);
    numberedWithNanNoData = new TiledImage(new Point(minX, minY), sm, tileWidth, tileHeight);
    twos = new TiledImage(new Point(minX, minY), sm, tileWidth, tileHeight);
    twosWithNoData = new TiledImage(new Point(minX, minY), sm, tileWidth, tileHeight);
    twosWithNoData.setProperty(OpImageUtils.NODATA_PROPERTY, NON_NAN_NODATA_VALUE);
    twosWithNanNoData = new TiledImage(new Point(minX, minY), sm, tileWidth, tileHeight);
    destRect = new Rectangle(minXForTile, minYForTile, tileWidth, tileHeight);
    for (int x=minX; x < minX + width; x++)
    {
      for (int y=minY; y < minY + height; y++)
      {
        int pixelId = getPixelId(x, y);
        numbered.setSample(x, y, 0, (double)pixelId * 100000.0);
        numberedWithNoData.setSample(x, y, 0,
            ((pixelId % noDataModValue) == 0) ? NON_NAN_NODATA_VALUE : ((double)pixelId) * 100000.0);
        numberedWithNanNoData.setSample(x, y, 0,
            ((pixelId % noDataModValue) == 0) ? Double.NaN : ((double)pixelId) * 100000.0);
        twos.setSample(x, y, 0, 2.0);
        twosWithNoData.setSample(x, y, 0,
            ((pixelId % noDataModValue) == 0) ? NON_NAN_NODATA_VALUE : 2.0);
        twosWithNanNoData.setSample(x, y, 0,
            ((pixelId % noDataModValue) == 0) ? Double.NaN : 2.0);
      }
    }
  }
}
