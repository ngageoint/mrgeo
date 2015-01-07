/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */

package org.mrgeo.opimage;

import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mrgeo.rasterops.OpImageRegistrar;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.test.OpImageTestUtils;
import org.opengis.referencing.FactoryException;

import javax.media.jai.PlanarImage;
import javax.media.jai.RenderedOp;
import javax.media.jai.TiledImage;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.io.IOException;
import java.util.LinkedList;

/**
 * @author jason.surratt
 * 
 */
@Ignore
@SuppressWarnings("static-method")
public class SlopeOpImageTest
{
  @Rule
  public TestName testname = new TestName();

  private static OpImageTestUtils testUtils;

  private static boolean GEN_BASELINE_DATA_ONLY = false;

  @BeforeClass
  public static void init() throws IOException
  {
    testUtils = new OpImageTestUtils(SlopeOpImageTest.class);
  }

  @Before public void setUp()
  {
    OpImageRegistrar.registerMrGeoOps();
  }

  private static RenderedImage runSlope(TiledImage arg) throws IOException, FactoryException
  {
    return runSlope(arg, "gradient");
  }

  private static RenderedImage runSlope(TiledImage arg, String units) throws IOException, FactoryException
  {
    RenderedImage op = SlopeDescriptor.create(arg, units, null);

    // Force the OpImage to be created - invokes create method on the descriptor for the op
    op.getMinX();

    java.util.List<TileLocator> tiles = new LinkedList<>();
    walkOpTree(op, tiles);

    for (TileLocator tl: tiles)
    {
      tl.setTileInfo(testUtils.tx, testUtils.ty, testUtils.zoom, testUtils.tileWidth);
    }

    return op;
  }

  private static void walkOpTree(RenderedImage op, java.util.List<TileLocator> tl)
  {
    if (op.getSources() != null)
    {
      for (Object obj: op.getSources())
      {
        if (obj instanceof RenderedImage)
        {
          walkOpTree((RenderedImage)obj, tl);
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
      walkOpTree(image, tl);
    }
  }



  @Test
  @Category(UnitTest.class)
  public void slope() throws Exception
  {
    RenderedImage slope = runSlope(testUtils.numbered);

    Raster r = slope.getData(testUtils.destRect);
    if (GEN_BASELINE_DATA_ONLY)
    {
      testUtils.generateBaselineTif(testname.getMethodName(), r);
    }
    else
    {
      testUtils.compareRasters(testname.getMethodName(), r);
    }
  }

  @Test
  @Category(UnitTest.class)
  public void slopeRad() throws Exception
  {
    RenderedImage slope = runSlope(testUtils.numbered, "rad");

    Raster r = slope.getData(testUtils.destRect);
    if (GEN_BASELINE_DATA_ONLY)
    {
      testUtils.generateBaselineTif(testname.getMethodName(), r);
    }
    else
    {
      testUtils.compareRasters(testname.getMethodName(), r);
    }
  }

  @Test
  @Category(UnitTest.class)
  public void slopeDeg() throws Exception
  {
    RenderedImage slope = runSlope(testUtils.numbered, "deg");

    Raster r = slope.getData(testUtils.destRect);
    if (GEN_BASELINE_DATA_ONLY)
    {
      testUtils.generateBaselineTif(testname.getMethodName(), r);
    }
    else
    {
      testUtils.compareRasters(testname.getMethodName(), r);
    }
  }
  @Test
  @Category(UnitTest.class)
  public void slopePct() throws Exception
  {
    RenderedImage slope = runSlope(testUtils.numbered, "percent");

    Raster r = slope.getData(testUtils.destRect);
    if (GEN_BASELINE_DATA_ONLY)
    {
      testUtils.generateBaselineTif(testname.getMethodName(), r);
    }
    else
    {
      testUtils.compareRasters(testname.getMethodName(), r);
    }
  }

  @Test
  @Category(UnitTest.class)
  public void slopeNaN() throws Exception
  {
    RenderedImage slope = runSlope(testUtils.numberedWithNanNoData);

    Raster r = slope.getData(testUtils.destRect);
    if (GEN_BASELINE_DATA_ONLY)
    {
      testUtils.generateBaselineTif(testname.getMethodName(), r);
    }
    else
    {
      testUtils.compareRasters(testname.getMethodName(), r);
    }
  }

  @Test
  @Category(UnitTest.class)
  public void slopeNodata() throws Exception
  {
    RenderedImage slope = runSlope(testUtils.numberedWithNoData);

    Raster r = slope.getData(testUtils.destRect);
    if (GEN_BASELINE_DATA_ONLY)
    {
      testUtils.generateBaselineTif(testname.getMethodName(), r);
    }
    else
    {
      testUtils.compareRasters(testname.getMethodName(), r);
    }
  }

  @Test
  @Category(UnitTest.class)
  public void slopeflat() throws Exception
  {
    RenderedImage slope = runSlope(testUtils.twos);

    Raster r = slope.getData(testUtils.destRect);
    if (GEN_BASELINE_DATA_ONLY)
    {
      testUtils.generateBaselineTif(testname.getMethodName(), r);
    }
    else
    {
      testUtils.compareRasters(testname.getMethodName(), r);
    }
  }
  @Test
  @Category(UnitTest.class)
  public void slopeflatNaN() throws Exception
  {
    RenderedImage slope = runSlope(testUtils.twosWithNanNoData);

    Raster r = slope.getData(testUtils.destRect);
    if (GEN_BASELINE_DATA_ONLY)
    {
      testUtils.generateBaselineTif(testname.getMethodName(), r);
    }
    else
    {
      testUtils.compareRasters(testname.getMethodName(), r);
    }
  }
  @Test
  @Category(UnitTest.class)
  public void slopeflatNodata() throws Exception
  {
    RenderedImage slope = runSlope(testUtils.twosWithNoData);

    Raster r = slope.getData(testUtils.destRect);
    if (GEN_BASELINE_DATA_ONLY)
    {
      testUtils.generateBaselineTif(testname.getMethodName(), r);
    }
    else
    {
      testUtils.compareRasters(testname.getMethodName(), r);
    }
  }
}
