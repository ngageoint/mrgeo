/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */

package org.mrgeo.opimage;

import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mrgeo.rasterops.OpImageRegistrar;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.rasterops.OpImageUtils;
import org.mrgeo.test.LocalRunnerTest;
import org.mrgeo.test.MapOpTestUtils;
import org.mrgeo.utils.TMSUtils;
import org.opengis.referencing.FactoryException;

import javax.media.jai.PlanarImage;
import javax.media.jai.RenderedOp;
import javax.media.jai.TiledImage;
import java.awt.*;
import java.awt.image.*;
import java.io.IOException;
import java.util.*;

/**
 * @author jason.surratt
 *
 */
@SuppressWarnings("static-method")
public class AspectOpImageTest extends LocalRunnerTest
{
  @Rule
  public TestName testname = new TestName();

  private static MapOpTestUtils testUtils;

  private static TiledImage twos;
  private static TiledImage twosWithNoData;
  private static TiledImage twosWithNanNoData;
  private static TiledImage numbered;
  private static TiledImage numberedWithNoData;
  private static TiledImage numberedWithNanNoData;

  private static boolean GEN_BASELINE_DATA_ONLY = false;

  private static Rectangle destRect;

  private static int width;

  private final static double NODATA = -32767.0;

  @BeforeClass
  public static void init() throws IOException
  {
    testUtils = new MapOpTestUtils(AspectOpImageTest.class);

    width = 10;

    int minX = -10;
    int minY = -10;


    SampleModel sm = new BandedSampleModel(DataBuffer.TYPE_DOUBLE, width * 3, width * 3, 1);
    numbered = new TiledImage(new Point(minX, minY), sm, width, width);
    numberedWithNoData = new TiledImage(new Point(minX, minY), sm, width, width);
    numberedWithNanNoData = new TiledImage(new Point(minX, minY), sm, width, width);
    twos = new TiledImage(new Point(minX, minY), sm, width, width);
    twosWithNoData = new TiledImage(new Point(minX, minY), sm, width, width);
    twosWithNoData.setProperty(OpImageUtils.NODATA_PROPERTY, NODATA);
    twosWithNanNoData = new TiledImage(new Point(minX, minY), sm, width, width);
    destRect = new Rectangle(0, 0, width, width);

    int noDataModValue = 7;

    for (int x = minX; x < minX + sm.getHeight(); x++)
    {
      for (int y=minY; y < minY + sm.getHeight(); y++)
      {
        int pixelId = getPixelId(x, y, width, sm.getWidth(), sm.getHeight());

        numbered.setSample(x, y, 0, (double)pixelId * 100000.0);

        numberedWithNoData.setSample(x, y, 0,
            ((pixelId % noDataModValue) == 0) ? NODATA : ((double)pixelId) * 100000.0);

        numberedWithNanNoData.setSample(x, y, 0,
            ((pixelId % noDataModValue) == 0) ? Double.NaN : ((double)pixelId) * 100000.0);

        twos.setSample(x, y, 0, 2.0);
        twosWithNoData.setSample(x, y, 0,
            ((pixelId % noDataModValue) == 0) ? NODATA : 2.0);
        twosWithNanNoData.setSample(x, y, 0,
            ((pixelId % noDataModValue) == 0) ? Double.NaN : 2.0);
      }
    }

  }
  @Before public void setUp()
  {
    OpImageRegistrar.registerMrGeoOps();
  }

  private static int getPixelId(int x, int y, int w, int tileWidth, int tileHeight)
  {
    // Shift the x/y by a tileWidth/Height to get rid of negative x/y values (since
    // x and y start at -tileWidth and -tileheight).
    return (tileWidth + x) + ((tileHeight + y) * w);
  }

  private static RenderedImage runAspect(TiledImage arg, double nodata) throws IOException, FactoryException
  {

    int zoom = 4;
    TMSUtils.Tile tile = TMSUtils.latLonToTile(0.0, 0.0, zoom, width);
    long tx = tile.tx;
    long ty = tile.ty;


    RenderedImage op = AspectDescriptor.create(arg, null);

    // Force the OpImage to be created - invokes create method on the descriptor for the op
    op.getMinX();

    PlanarImage image = ((RenderedOp)op).getCurrentRendering();
    OpImageUtils.setNoData(image, nodata);

    java.util.List<TileLocator> tiles = new LinkedList<>();
    walkOpTree(op, tiles, nodata);

    for (TileLocator tl: tiles)
    {
      tl.setTileInfo(tx, ty, zoom, width);
    }

    return op;
  }

  private static void walkOpTree(RenderedImage op, java.util.List<TileLocator> tl, double nodata)
  {
    OpImageUtils.setNoData((PlanarImage)op, nodata);

    if (op.getSources() != null)
    {
      for (Object obj: op.getSources())
      {
        if (obj instanceof RenderedImage)
        {
          walkOpTree((RenderedImage)obj, tl, nodata);
        }
      }
    }
    if (op instanceof RenderedOp)
    {
      RenderedOp ro = (RenderedOp)op;
      PlanarImage image = ro.getCurrentRendering();

      if (image instanceof TileLocator)
      {
        tl.add((TileLocator)image);
      }
      walkOpTree(image, tl, nodata);
    }
  }



  @Test
  @Category(UnitTest.class)
  public void aspect() throws Exception
  {
    RenderedImage aspect = runAspect(numbered, Double.NaN);

    Raster r = aspect.getData(destRect);
    if (GEN_BASELINE_DATA_ONLY)
    {
      testUtils.generateBaselineTif(conf, testname.getMethodName(), r);
    }
    else
    {
      testUtils.compareRasters(testname.getMethodName(), r);
    }
  }

  @Test
  @Category(UnitTest.class)
  public void aspectNaN() throws Exception
  {
    RenderedImage aspect = runAspect(numberedWithNanNoData, Double.NaN);

    Raster r = aspect.getData(destRect);
    if (GEN_BASELINE_DATA_ONLY)
    {
      testUtils.generateBaselineTif(conf, testname.getMethodName(), r);
    }
    else
    {
      testUtils.compareRasters(testname.getMethodName(), r);
    }
  }

  @Test
  @Category(UnitTest.class)
  public void aspectNodata() throws Exception
  {
    RenderedImage aspect = runAspect(numberedWithNoData, NODATA);

    Raster r = aspect.getData(destRect);
    if (GEN_BASELINE_DATA_ONLY)
    {
      testUtils.generateBaselineTif(conf, testname.getMethodName(), r);
    }
    else
    {
      testUtils.compareRasters(testname.getMethodName(), r);
    }
  }

  @Test
  @Category(UnitTest.class)
  public void aspectflat() throws Exception
  {
    RenderedImage aspect = runAspect(twos, Double.NaN);

    Raster r = aspect.getData(destRect);
    if (GEN_BASELINE_DATA_ONLY)
    {
      testUtils.generateBaselineTif(conf, testname.getMethodName(), r);
    }
    else
    {
      testUtils.compareRasters(testname.getMethodName(), r);
    }
  }
  @Test
  @Category(UnitTest.class)
  public void aspectflatNaN() throws Exception
  {
    RenderedImage aspect = runAspect(twosWithNanNoData, Double.NaN);

    Raster r = aspect.getData(destRect);
    if (GEN_BASELINE_DATA_ONLY)
    {
      testUtils.generateBaselineTif(conf, testname.getMethodName(), r);
    }
    else
    {
      testUtils.compareRasters(testname.getMethodName(), r);
    }
  }
  @Test
  @Category(UnitTest.class)
  public void aspectflatNodata() throws Exception
  {
    RenderedImage aspect = runAspect(twosWithNoData, NODATA);

    Raster r = aspect.getData(destRect);
    if (GEN_BASELINE_DATA_ONLY)
    {
      testUtils.generateBaselineTif(conf, testname.getMethodName(), r);
    }
    else
    {
      testUtils.compareRasters(testname.getMethodName(), r);
    }
  }
}
