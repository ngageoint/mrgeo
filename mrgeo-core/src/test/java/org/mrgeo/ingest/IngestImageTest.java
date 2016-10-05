package org.mrgeo.ingest;

import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;
import org.gdal.gdal.Dataset;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.raster.MrGeoRaster;
import org.mrgeo.image.MrsImage;
import org.mrgeo.image.MrsPyramid;
import org.mrgeo.image.MrsPyramidMetadata;
import org.mrgeo.junit.IntegrationTest;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.test.LocalRunnerTest;
import org.mrgeo.test.TestUtils;
import org.mrgeo.utils.GDALJavaUtils;
import org.mrgeo.utils.GDALUtils;
import org.mrgeo.utils.LongRectangle;
import org.mrgeo.utils.tms.Bounds;
import org.mrgeo.utils.tms.TMSUtils;

import java.awt.image.DataBuffer;
import java.io.IOException;
import java.util.HashMap;

public class IngestImageTest extends LocalRunnerTest
{
@Rule
public TestName testname = new TestName();

public final static boolean GEN_BASELINE_DATA_ONLY = false;

private static TestUtils testUtils;

private String onetile = "onetile.tif";
private String bigtile = "bigtile.tif";
private String shiftedtile = "shiftedtile.tif";
private String[] fourtiles = {
    "fourtiles-1-1.tif",
    "fourtiles-1-2.tif",
    "fourtiles-2-1.tif",
    "fourtiles-2-2.tif"
};


@BeforeClass
public static void init() throws IOException
{
  testUtils = new TestUtils(IngestImageTest.class);
}

@Before
public void setup() throws IOException
{
}

@Test
@Category(UnitTest.class)
public void ingestOneTile() throws IOException
{

//  def localIngest(inputs: Array[String], output: String,
//    categorical: Boolean, skipCategoryLoad: Boolean, config: Configuration, bounds: Bounds,
//    zoomlevel: Int, tilesize: Int, nodata: Array[Double], bands: Int, tiletype: Int,
//    tags: java.util.Map[String, String], protectionLevel: String,
//    providerProperties: ProviderProperties): Boolean = {

  String input = testUtils.getInputLocalFor(onetile);
  String output = testUtils.getOutputHdfsFor(testname.getMethodName()).toString();

  int tilesize = 512;
  int zoom = GDALUtils.calculateZoom(input, tilesize);
  Bounds bounds = GDALUtils.getBounds(GDALUtils.open(input));

  IngestImage.localIngest(new String[]{input}, output, false, false, getConfiguration(),
      bounds, zoom, tilesize, new double[]{-9999}, 1, DataBuffer.TYPE_FLOAT,
      new HashMap<String, String>(), "", new ProviderProperties());

  MrsPyramid ingested = MrsPyramid.open(output,  getConfiguration());
  MrsPyramidMetadata meta = ingested.getMetadata();

  Assert.assertEquals("Bad zoom", 3, meta.getMaxZoomLevel());

  LongRectangle tb = meta.getTileBounds(zoom);
  Assert.assertEquals("Wrong number of tiles", 1, tb.getWidth() * tb.getHeight());

  try (MrsImage image = ingested.getImage(zoom))
  {
    MrGeoRaster raster = image.getRaster();

    TestUtils.compareNumberedRaster(raster);
  }
}

@Test
@Category(UnitTest.class)
public void ingestFourTiles() throws IOException
{

//  def localIngest(inputs: Array[String], output: String,
//    categorical: Boolean, skipCategoryLoad: Boolean, config: Configuration, bounds: Bounds,
//    zoomlevel: Int, tilesize: Int, nodata: Array[Double], bands: Int, tiletype: Int,
//    tags: java.util.Map[String, String], protectionLevel: String,
//    providerProperties: ProviderProperties): Boolean = {

  String[] inputs = new String[fourtiles.length];
  for (int i = 0; i < fourtiles.length; i++)
  {
    inputs[i] = testUtils.getInputLocalFor(fourtiles[i]);
  }

  String output = testUtils.getOutputHdfsFor(testname.getMethodName()).toString();

  int zoom = 3;
  int tilesize = 512;

  Bounds bounds = null;
  for (int x = 1; x < 3; x++)
  {
    for (int y = 1; y < 3; y++)
    {
      if (bounds == null)
      {
        bounds = TMSUtils .tileBounds(x, y, zoom, tilesize);
      }
      else
      {
        bounds = bounds.expand(TMSUtils .tileBounds(x, y, zoom, tilesize));
      }
    }
  }

  IngestImage.localIngest(inputs, output, false, false, getConfiguration(),
      bounds, zoom, tilesize, new double[]{-9999}, 1, DataBuffer.TYPE_FLOAT,
      new HashMap<String, String>(), "", new ProviderProperties());

  MrsPyramid ingested = MrsPyramid.open(output,  getConfiguration());
  MrsPyramidMetadata meta = ingested.getMetadata();

  Assert.assertEquals("Bad zoom", zoom, meta.getMaxZoomLevel());

  LongRectangle tb = meta.getTileBounds(zoom);
  Assert.assertEquals("Wrong number of tiles", 4, tb.getWidth() * tb.getHeight());

  try (MrsImage image = ingested.getImage(zoom))
  {
    MrGeoRaster raster = image.getRaster();

    for (int y = 0; y < raster.height(); y++)
    {
      for (int x = 0; x < raster.width(); x++)
      {
        int pixelId =  (x % tilesize) + ((y % tilesize) * tilesize);

        Assert.assertEquals("Bad pixel px: " + x + " py: " + y +  " expected: " + pixelId + " actual: " +  raster.getPixelDouble(x, y, 0),
            pixelId, raster.getPixelDouble(x, y, 0), 1e-8);
      }
    }
  }
}


@Test
@Category(UnitTest.class)
public void ingestTileShifted() throws IOException
{

//  def localIngest(inputs: Array[String], output: String,
//    categorical: Boolean, skipCategoryLoad: Boolean, config: Configuration, bounds: Bounds,
//    zoomlevel: Int, tilesize: Int, nodata: Array[Double], bands: Int, tiletype: Int,
//    tags: java.util.Map[String, String], protectionLevel: String,
//    providerProperties: ProviderProperties): Boolean = {

  String input = testUtils.getInputLocalFor(shiftedtile);
  String output = testUtils.getOutputHdfsFor(testname.getMethodName()).toString();

  int tilesize = 512;
  int zoom = GDALUtils.calculateZoom(input, tilesize);
  Bounds bounds = GDALUtils.getBounds(GDALUtils.open(input));

  IngestImage.localIngest(new String[]{input}, output, false, false, getConfiguration(),
      bounds, zoom, tilesize, new double[]{-9999}, 1, DataBuffer.TYPE_FLOAT,
      new HashMap<String, String>(), "", new ProviderProperties());

  MrsPyramid ingested = MrsPyramid.open(output,  getConfiguration());
  MrsPyramidMetadata meta = ingested.getMetadata();

  Assert.assertEquals("Bad zoom", 3, meta.getMaxZoomLevel());

  LongRectangle tb = meta.getTileBounds(zoom);
  Assert.assertEquals("Wrong number of tiles", 4, tb.getWidth() * tb.getHeight());


  try (MrsImage image = ingested.getImage(zoom))
  {
    MrGeoRaster raster = image.getRaster();

    int pixelId = 0;
    for (int y = 0; y < raster.height(); y++)
    {
      for (int x = 0; x < raster.width(); x++)
      {
        int px = raster.getPixelInt(x, y, 0);
        if (px != -9999)
        {
          Assert.assertEquals("Bad pixel px: " + x + " py: " + y + " expected: " + pixelId + " actual: " + raster.getPixelInt(x, y, 0),
              pixelId, raster.getPixelInt(x, y, 0));

          pixelId++;
        }
//        else
//        {
//          System.out.println("px: " + x + " py: " + y);
//        }
      }
    }
  }
}

@Test
@Category(IntegrationTest.class)
public void ingestBigtile() throws IOException
{

//  def localIngest(inputs: Array[String], output: String,
//    categorical: Boolean, skipCategoryLoad: Boolean, config: Configuration, bounds: Bounds,
//    zoomlevel: Int, tilesize: Int, nodata: Array[Double], bands: Int, tiletype: Int,
//    tags: java.util.Map[String, String], protectionLevel: String,
//    providerProperties: ProviderProperties): Boolean = {

  String input = testUtils.getInputLocalFor(bigtile);
  String output = testUtils.getOutputHdfsFor(testname.getMethodName()).toString();

  int tilesize = 512;
  int zoom = 9; // GDALUtils.calculateZoom(input, tilesize);

  Bounds bounds = GDALUtils.getBounds(GDALUtils.open(input));
  IngestImage.localIngest(new String[]{input}, output, false, false, getConfiguration(),
      bounds, zoom, tilesize, new double[]{-9999}, 1, DataBuffer.TYPE_FLOAT,
      new HashMap<String, String>(), "", new ProviderProperties());

  MrsPyramid ingested = MrsPyramid.open(output,  getConfiguration());
  MrsPyramidMetadata meta = ingested.getMetadata();

  Assert.assertEquals("Bad zoom", zoom, meta.getMaxZoomLevel());

  LongRectangle tb = meta.getTileBounds(zoom);
  Assert.assertEquals("Wrong number of tiles", 6, tb.getWidth() * tb.getHeight());


  try (MrsImage image = ingested.getImage(zoom))
  {
    MrGeoRaster raster = image.getRaster();

    if (GEN_BASELINE_DATA_ONLY)
    {
      testUtils.saveBaselineTif(testname.getMethodName(), raster, TMSUtils.tileBounds(bounds, zoom, tilesize), -9999);
    }
    else
    {
      testUtils.compareRasters(testname.getMethodName(), raster);
    }

  }
}

}
