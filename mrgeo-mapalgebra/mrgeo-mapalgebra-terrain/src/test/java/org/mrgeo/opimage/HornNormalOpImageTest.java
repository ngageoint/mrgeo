package org.mrgeo.opimage;

import java.awt.Rectangle;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.io.IOException;

import javax.media.jai.PlanarImage;
import javax.media.jai.RenderedOp;
import javax.media.jai.TiledImage;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.mapalgebra.parser.ParserException;
import org.mrgeo.mapreduce.job.JobCancelledException;
import org.mrgeo.mapreduce.job.JobFailedException;
import org.mrgeo.rasterops.OpImageRegistrar;
import org.mrgeo.rasterops.OpImageUtils;
import org.mrgeo.test.OpImageTestUtils;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;

public class HornNormalOpImageTest
{
  @Rule public TestName testname = new TestName();

  private static final boolean GENERATE_BASELINE_DATA = false;
  private static final double EPSILON = 1e-7;

//  private static String input;

  private static OpImageTestUtils testUtils;

  @BeforeClass
  public static void init() throws IOException
  {
    testUtils = new OpImageTestUtils(HornNormalOpImageTest.class);

//    input = OpImageTestUtils.composeInputDir(HornNormalOpImageTest.class);
  }

  @Before
  public void setUp()
  {
    OpImageRegistrar.registerMrGeoOps();
  }

  @Test
  @Category(UnitTest.class)
  public void testFlatSurfaceNoNan() throws IOException, NoSuchAuthorityCodeException, FactoryException
  {
    double noData = Double.NaN;
    RenderedImage ri = runHornNormal(testUtils.twos, noData, testUtils.destRect,
        testUtils.tx, testUtils.ty, testUtils.zoom,
        testUtils.tileWidth, testUtils.tileHeight);
    Raster r = ri.getData();
    for (int x=testUtils.minXForTile; x < testUtils.minXForTile + testUtils.tileWidth; x++)
    {
      for (int y=testUtils.minYForTile; y < testUtils.minYForTile + testUtils.tileHeight; y++)
      {
        // Because all the elevations are the same, the horn normal should be 1.0
        Assert.assertEquals("Bad value at x = " + x + ", y = " + y, 1.0, r.getSampleFloat(x, y, 2), EPSILON);
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testFlatSurfaceWithNanNoData() throws IOException, NoSuchAuthorityCodeException, FactoryException
  {
    double noData = Double.NaN;
    RenderedImage ri = runHornNormal(testUtils.twosWithNanNoData, noData,
        testUtils.destRect, testUtils.tx, testUtils.ty, testUtils.zoom,
        testUtils.tileWidth, testUtils.tileHeight);
    Raster r = ri.getData();
    for (int x=testUtils.minXForTile; x < testUtils.minXForTile + testUtils.tileWidth; x++)
    {
      for (int y=testUtils.minYForTile; y < testUtils.minYForTile + testUtils.tileHeight; y++)
      {
        int pixelId = testUtils.getPixelId(x, y);
        if (pixelId % testUtils.noDataModValue == 0)
        {
          Assert.assertTrue("Bad value at x = " + x + ", y = " + y, Float.isNaN(r.getSampleFloat(x, y, 2)));
        }
        else
        {
          // Because all the elevations are the same, the horn normal should be 1.0
          Assert.assertEquals("Bad value at x = " + x + ", y = " + y, 1.0, r.getSampleFloat(x, y, 2), EPSILON);
        }
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testFlatSurfaceWithNoData() throws IOException, FactoryException
  {
    double noData = OpImageTestUtils.NON_NAN_NODATA_VALUE;
    RenderedImage ri = runHornNormal(testUtils.twosWithNoData, noData,
        testUtils.destRect, testUtils.tx, testUtils.ty, testUtils.zoom,
        testUtils.tileWidth, testUtils.tileHeight);
    Raster r = ri.getData();
    for (int x=testUtils.minXForTile; x < testUtils.minXForTile + testUtils.tileWidth; x++)
    {
      for (int y=testUtils.minYForTile; y < testUtils.minYForTile + testUtils.tileHeight; y++)
      {
        int pixelId = testUtils.getPixelId(x, y);
        if (pixelId % testUtils.noDataModValue == 0)
        {
          Assert.assertTrue("Bad value at x = " + x + ", y = " + y, Float.isNaN(r.getSampleFloat(x, y, 2)));
        }
        else
        {
          // Because all the elevations are the same, the horn normal should be 1.0
          Assert.assertEquals("Bad value at x = " + x + ", y = " + y, 1.0, r.getSampleFloat(x, y, 2), EPSILON);
        }
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testNoNan()
      throws IOException, FactoryException, JobCancelledException, ParserException,
      JobFailedException
  {
    double noData = Double.NaN;
    RenderedImage ri = runHornNormal(testUtils.numbered, noData, testUtils.destRect,
        testUtils.tx, testUtils.ty, testUtils.zoom,
        testUtils.tileWidth, testUtils.tileHeight);
//    String resultOutputFile = input + testname.getMethodName();
//    if (GENERATE_BASELINE_DATA)
//    {
//      GeotoolsRasterUtils.saveLocalGeotiff(resultOutputFile, ri.getData(destRect),
//          tx, ty, zoom, tileWidth, noData);
//    }
//    else
//    {
//      TestUtils.compareRenderedImages(ImageIO.read(new File(resultOutputFile + ".tif")), ri);
//    }

    Raster r = ri.getData(testUtils.destRect);
    if (GENERATE_BASELINE_DATA)
    {
      testUtils.generateBaselineTif( testname.getMethodName(), r);
    }
    else
    {
      testUtils.compareRasters(testname.getMethodName(), r);
    }

  }

  @Test
  @Category(UnitTest.class)
  public void testWithNanNoData()
      throws IOException,  FactoryException, JobCancelledException, ParserException,
      JobFailedException
  {
    double noData = Double.NaN;
    RenderedImage ri = runHornNormal(testUtils.numberedWithNanNoData, noData,
        testUtils.destRect, testUtils.tx, testUtils.ty, testUtils.zoom,
        testUtils.tileWidth, testUtils.tileHeight);
//    String resultOutputFile = input + testname.getMethodName();
//    if (GENERATE_BASELINE_DATA)
//    {
//      GeotoolsRasterUtils.saveLocalGeotiff(resultOutputFile, ri.getData(destRect),
//          tx, ty, zoom, tileWidth, noData);
//    }
//    else
//    {
//      // Since tif's cannot store NaN values, the baseline image stores NaN as
//      // -9999 instead.
//      TestUtils.compareRenderedImages(ImageIO.read(new File(resultOutputFile + ".tif")),
//          null,
//          ri, nanTranslatorToMinus9999);
//    }
    Raster r = ri.getData(testUtils.destRect);
    if (GENERATE_BASELINE_DATA)
    {
      testUtils.generateBaselineTif( testname.getMethodName(), r);
    }
    else
    {
      testUtils.compareRasters(testname.getMethodName(), null, r,
          testUtils.nanTranslatorToMinus9999);
    }

  }

  @Test
  @Category(UnitTest.class)
  public void testWithNoData()
      throws IOException, NoSuchAuthorityCodeException, FactoryException, JobCancelledException, ParserException,
      JobFailedException
  {
    double noData = OpImageTestUtils.NON_NAN_NODATA_VALUE;
    RenderedImage ri = runHornNormal(testUtils.numberedWithNoData, noData,
        testUtils.destRect, testUtils.tx, testUtils.ty, testUtils.zoom,
        testUtils.tileWidth, testUtils.tileHeight);
//    String resultOutputFile = input + testname.getMethodName();
//    if (GENERATE_BASELINE_DATA)
//    {
//      GeotoolsRasterUtils.saveLocalGeotiff(resultOutputFile, ri.getData(destRect),
//          tx, ty, zoom, tileWidth, noData);
//    }
//    else
//    {
//      TestUtils.compareRenderedImages(ImageIO.read(new File(resultOutputFile + ".tif")),
//          null, ri, nanTranslatorToMinus32767 /*nonNanTranslator*/);
//    }
    Raster r = ri.getData(testUtils.destRect);
    if (GENERATE_BASELINE_DATA)
    {
      testUtils.generateBaselineTif( testname.getMethodName(), r);
    }
    else
    {
      testUtils.compareRasters(testname.getMethodName(), null, r,
          testUtils.nanTranslatorToMinus32767);
    }

  }

  private static RenderedImage runHornNormal(TiledImage arg, double argNoData,
      Rectangle destRect, long tx, long ty, int zoom, int tileWidth, int tileHeight) throws IOException, NoSuchAuthorityCodeException, FactoryException
  {
    RenderedImage op = HornNormalDescriptor.create(arg, null);
    
    // Force the OpImage to be created - invokes create method on the descriptor for the op
    op.getMinX();
    // Need to tell horn normal where the tile is in the world so it can perform correct computations
    Assert.assertTrue(op instanceof RenderedOp);
    RenderedOp ro = (RenderedOp)op;
    PlanarImage image = ro.getCurrentRendering();
    OpImageUtils.setNoData((HornNormalOpImage)image, argNoData);
    Assert.assertTrue("Looks like HornNormalOpImage no longer implements TileLocator, but it should", (image instanceof TileLocator));
    ((TileLocator)image).setTileInfo(tx, ty, zoom, tileWidth);
    return op;
  }
}
