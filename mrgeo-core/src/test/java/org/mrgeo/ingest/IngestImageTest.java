package org.mrgeo.ingest;

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

  // Create rasters to ingest
//  MrGeoRaster raster = TestUtils.createNumberedRaster(512, 512, DataBuffer.TYPE_FLOAT);
//  for (int y = 1; y < 3; y++)
//  {
//    for (int x = 1; x < 3; x++)
//    {
//       Bounds b = TMSUtils.tileBounds(x, y, 3, 512);
//       GDALJavaUtils.saveRaster(raster.toDataset(b, new double[]{-9999}),
//           "/data/export/tiles/fourtiles-" + x + "-" + y + ".tif", b, -9999);
//    }
//  }
//
//   Bounds b = TMSUtils.tileBounds(1, 1, 3, 512);
//  double hw = b.width() / 2.0;
//  double hh = b.height() / 2.0;
//
//  b = new Bounds(b.w + hw, b.s + hh, b.e + hw, b.n + hh);
//   GDALJavaUtils.saveRaster(raster.toDataset(b, new double[]{-9999}),
//       "/data/export/tiles/shiftedtile.tif", b, -9999);
//
  Dataset file = GDALUtils.open("/home/tim.tisler/projects/mrgeo/mrgeo-opensource/mrgeo-cmd/mrgeo-cmd-ingest/testFiles/org.mrgeo.cmd.ingest/IngestImageTest/AsterSample/ASTGTM2_N33E069_dem.tif");
  MrGeoRaster raster = MrGeoRaster.createEmptyRaster(file.GetRasterXSize(), file.GetRasterYSize(), 1, DataBuffer.TYPE_FLOAT);

  for (int b = 0; b < raster.bands(); b++)
  {
    for (int y = 0; y < raster.height(); y++)
    {
      for (int x = 0; x < raster.width(); x++)
      {
        int pixelId =  x + (y * raster.width());
        raster.setPixel(x, y, b, pixelId);
      }
    }
  }
  Bounds b = TMSUtils.tileBounds(GDALUtils.getBounds(file), 12, 512);
  GDALJavaUtils.saveRaster(raster.toDataset(b, new double[]{-32767}),
      "/data/export/tiles/bigtile.tif", b, -32767);

}

@Before
public void setup() throws IOException
{
}

@Test
@Category(IntegrationTest.class)
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
@Category(IntegrationTest.class)
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
@Category(IntegrationTest.class)
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
    testUtils.saveBaselineTif(testname.getMethodName(), raster, TMSUtils.tileBounds(bounds, zoom, tilesize), -9999);

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
  int zoom = GDALUtils.calculateZoom(input, tilesize);

  Bounds bounds = GDALUtils.getBounds(GDALUtils.open(input));
  IngestImage.localIngest(new String[]{input}, output, true, false, getConfiguration(),
      bounds, zoom, tilesize, new double[]{-9999}, 1, DataBuffer.TYPE_FLOAT,
      new HashMap<String, String>(), "", new ProviderProperties());

  MrsPyramid ingested = MrsPyramid.open(output,  getConfiguration());
  MrsPyramidMetadata meta = ingested.getMetadata();

  Assert.assertEquals("Bad zoom", zoom, meta.getMaxZoomLevel());

  LongRectangle tb = meta.getTileBounds(zoom);
  Assert.assertEquals("Wrong number of tiles", 144, tb.getWidth() * tb.getHeight());


  try (MrsImage image = ingested.getImage(zoom))
  {
    MrGeoRaster raster = image.getRaster();


    testUtils.saveBaselineTif(testname.getMethodName(), raster, TMSUtils.tileBounds(bounds, zoom, tilesize), -9999);

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

}
