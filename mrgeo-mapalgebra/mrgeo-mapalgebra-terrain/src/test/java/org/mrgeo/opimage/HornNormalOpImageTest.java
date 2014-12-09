package org.mrgeo.opimage;

import java.awt.Point;
import java.awt.Rectangle;
import java.awt.image.BandedSampleModel;
import java.awt.image.ColorModel;
import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;
import java.io.File;
import java.io.IOException;

import javax.imageio.ImageIO;
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
import org.mrgeo.image.geotools.GeotoolsRasterUtils;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.mapalgebra.parser.ParserException;
import org.mrgeo.mapreduce.job.JobCancelledException;
import org.mrgeo.mapreduce.job.JobFailedException;
import org.mrgeo.rasterops.OpImageRegistrar;
import org.mrgeo.rasterops.OpImageUtils;
import org.mrgeo.test.TestUtils;
import org.mrgeo.utils.TMSUtils;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;

public class HornNormalOpImageTest
{
  @Rule public TestName testname = new TestName();

  private static final boolean GENERATE_BASELINE_DATA = false;
  private static final double EPSILON = 1e-7;

  private static String input;
  private static int noDataModValue = 7;
  private static double NON_NAN_NODATA_VALUE = -32767.0;
  private static SampleModel sm;
  private static int width;
  private static int height;
  private static int tileWidth;
  private static int tileHeight;
  private static int minX;
  private static int minY;
  private static int minXForTile;
  private static int minYForTile;
  private static TiledImage twos;
  private static TiledImage twosWithNoData;
  private static TiledImage twosWithNanNoData;
  private static TiledImage numbered;
  private static TiledImage numberedWithNoData;
  private static TiledImage numberedWithNanNoData;
  private static Rectangle destRect;
  private static int zoom;
  private static long tx;
  private static long ty;
  private static TestUtils.ValueTranslator nanTranslatorToMinus9999;
  private static TestUtils.ValueTranslator nanTranslatorToMinus32767;

  private static TestUtils testUtils;

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

  @BeforeClass
  public static void init() throws IOException
  {
    testUtils = new TestUtils(HornNormalOpImageTest.class);

    input = TestUtils.composeInputDir(HornNormalOpImageTest.class);
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
        int pixelId = getPixelId(x, y, width, tileWidth, tileHeight);
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
    RenderedImage ri = runHornNormal(twos, noData, destRect, tx, ty, zoom, tileWidth, tileHeight);
    Raster r = ri.getData();
    for (int x=minXForTile; x < minXForTile + tileWidth; x++)
    {
      for (int y=minYForTile; y < minYForTile + tileHeight; y++)
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
    RenderedImage ri = runHornNormal(twosWithNanNoData, noData, destRect, tx, ty, zoom, tileWidth, tileHeight);
    Raster r = ri.getData();
    for (int x=minXForTile; x < minXForTile + tileWidth; x++)
    {
      for (int y=minYForTile; y < minYForTile + tileHeight; y++)
      {
        int pixelId = getPixelId(x, y, width, tileWidth, tileHeight);
        if (pixelId % noDataModValue == 0)
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
    double noData = NON_NAN_NODATA_VALUE;
    RenderedImage ri = runHornNormal(twosWithNoData, noData, destRect, tx, ty, zoom, tileWidth, tileHeight);
    Raster r = ri.getData();
    for (int x=minXForTile; x < minXForTile + tileWidth; x++)
    {
      for (int y=minYForTile; y < minYForTile + tileHeight; y++)
      {
        int pixelId = getPixelId(x, y, width, tileWidth, tileHeight);
        if (pixelId % noDataModValue == 0)
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
    RenderedImage ri = runHornNormal(numbered, noData, destRect, tx, ty, zoom, tileWidth, tileHeight);
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

    Raster r = ri.getData(destRect);
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
    RenderedImage ri = runHornNormal(numberedWithNanNoData, noData, destRect, tx, ty, zoom, tileWidth, tileHeight);
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
    Raster r = ri.getData(destRect);
    if (GENERATE_BASELINE_DATA)
    {
      testUtils.generateBaselineTif( testname.getMethodName(), r);
    }
    else
    {
      testUtils.compareRasters(testname.getMethodName(), null, r, nanTranslatorToMinus9999);
    }

  }

  @Test
  @Category(UnitTest.class)
  public void testWithNoData()
      throws IOException, NoSuchAuthorityCodeException, FactoryException, JobCancelledException, ParserException,
      JobFailedException
  {
    double noData = NON_NAN_NODATA_VALUE;
    RenderedImage ri = runHornNormal(numberedWithNoData, noData, destRect, tx, ty, zoom, tileWidth, tileHeight);
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
    Raster r = ri.getData(destRect);
    if (GENERATE_BASELINE_DATA)
    {
      testUtils.generateBaselineTif( testname.getMethodName(), r);
    }
    else
    {
      testUtils.compareRasters(testname.getMethodName(), null, r, nanTranslatorToMinus32767);
    }

  }

  private static int getPixelId(int x, int y, int w, int tileWidth, int tileHeight)
  {
    // Shift the x/y by a tileWidth/Height to get rid of negative x/y values (since
    // x and y start at -tileWidth and -tileheight).
    return (tileWidth + x) + ((tileHeight + y) * w);
  }

  private static RenderedImage runHornNormal(TiledImage arg, double argNoData,
      Rectangle destRect, long tx, long ty, int zoom, int tileWidth, int tileHeight) throws IOException, NoSuchAuthorityCodeException, FactoryException
  {
    ColorModel cm = arg.getColorModel();
    SampleModel smOutput = new BandedSampleModel(DataBuffer.TYPE_DOUBLE, tileWidth, tileHeight, 3);
    WritableRaster rasterOutput = Raster.createWritableRaster(smOutput, new Point(minXForTile, minYForTile));
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
