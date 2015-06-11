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

package org.mrgeo.utils.geotools;

import java.awt.image.Raster;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.io.AbstractGridCoverage2DReader;
import org.geotools.coverage.grid.io.AbstractGridFormat;
import org.geotools.coverage.processing.CoverageProcessingException;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.core.Defs;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.mrgeo.image.MrsImagePyramidMetadata.ImageMetadata;
import org.mrgeo.image.geotools.GeotoolsRasterUtils;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.test.TestUtils;
import org.mrgeo.utils.Bounds;
import org.mrgeo.utils.HadoopUtils;
import org.mrgeo.utils.LongRectangle;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.operation.TransformException;

@SuppressWarnings("static-method")
public class GeotoolsRasterUtilsTest
{
  private static Configuration conf;
  private static Path hdfsDir;

  private static String greeceTif = "greece.tif";
  private static String greecePng = "greece.png";

  private static String hdfsGreeceTif;
  private static String hdfsGreecePng;

  private static String localGreeceTif;
  private static String localGreecePng;

  private static String chessTif = "chess.tif";
  private static String hdfsChessTif;

  private static String epsg2100Tif = "projections/epsg-2100.tif";
  private static String epsg3857Tif = "projections/epsg-3785.tif";
  private static String epsg4314Tif = "projections/epsg-4314.tif";
  private static String epsg4326Tif = "projections/epsg-4326.tif";
  private static String epsg32634Tif = "projections/epsg-32634.tif";
  private static String epsg102031Tif = "projections/epsg-102031.tif";

  private static String epsg2100;
  private static String epsg3857;
  private static String epsg4314;
  private static String epsg4326;
  private static String epsg32634;
  private static String epsg102031;

  private static String highLat = "high-lat-elevation/ASTGTM2_N00E006_dem.tif";
  private static String hdfsHighLatTif;

  @BeforeClass
  public static void init() throws Exception
  {
    hdfsDir = TestUtils.composeInputHdfs(GeotoolsRasterUtilsTest.class);

    File file = new File(Defs.INPUT + chessTif);
    // chessTif = "file://" + file.getCanonicalPath();
    HadoopFileUtils.copyToHdfs(file.getCanonicalPath(), hdfsDir.toString());
    hdfsChessTif = new Path(hdfsDir, chessTif).toString();

    localGreeceTif = "file://" + new File(Defs.INPUT + greeceTif).getCanonicalPath();
    HadoopFileUtils.copyToHdfs(Defs.INPUT + greeceTif, hdfsDir.toString());
    hdfsGreeceTif = new Path(hdfsDir, greeceTif).toString();

    localGreecePng = new File(Defs.INPUT + greecePng).getCanonicalPath();
    HadoopFileUtils.copyToHdfs(Defs.INPUT + greecePng, hdfsDir.toString());
    hdfsGreecePng = new Path(hdfsDir, greecePng).toString();

    file = new File(Defs.INPUT + highLat);
    HadoopFileUtils.copyToHdfs(file.getCanonicalPath(), hdfsDir.toString());
    hdfsHighLatTif = new Path(hdfsDir, file.getName()).toString();

    conf = HadoopUtils.createConfiguration();

    epsg2100 = "file://" + new File(Defs.INPUT + epsg2100Tif).getCanonicalPath();
    epsg3857 = "file://" + new File(Defs.INPUT + epsg3857Tif).getCanonicalPath();
    epsg4314 ="file://" +  new File(Defs.INPUT + epsg4314Tif).getCanonicalPath();
    epsg4326 ="file://" +  new File(Defs.INPUT + epsg4326Tif).getCanonicalPath();
    epsg32634 = "file://" + new File(Defs.INPUT + epsg32634Tif).getCanonicalPath();
    epsg102031 = "file://" + new File(Defs.INPUT + epsg102031Tif).getCanonicalPath();

    // this is a little sloppy, but in order to run these tests in parallel,
    // we need to invoke the lazy-load of the formats early. This is the easiest way
    GeotoolsRasterUtils.fastAccepts(null);
  }

  @AfterClass
  public static void tearDown() throws Exception
  {
    // free up statics
    conf = null;
    hdfsDir = null;

    greeceTif = null;
    greecePng = null;

    hdfsGreeceTif = null;
    hdfsGreecePng = null;

    localGreeceTif = null;
    localGreecePng = null;

    chessTif = null;
    hdfsChessTif = null;

    epsg2100Tif = null;
    epsg3857Tif = null;
    epsg4314Tif = null;
    epsg4326Tif = null;
    epsg32634Tif = null;
    epsg102031Tif = null;

    epsg2100 = null;
    epsg3857 = null;
    epsg4314 = null;
    epsg4326 = null;
    epsg32634 = null;
    epsg102031 = null;

  }

  @Test
  @Category(UnitTest.class)
  public void calculateMetaData() throws Exception
  {
    String protectionLevel = "abc";
    final MrsImagePyramidMetadata metadata = GeotoolsRasterUtils.calculateMetaData(localGreeceTif, protectionLevel, 0.0);

    // haven't ingested the image, so the pyramid name should be ""
    compareMetadata(metadata, "", false, 0.00001, protectionLevel);
  }

  @Test
  @Category(UnitTest.class)
  public void calculateMetaData2() throws Exception
  {
    String protectionLevel = "abc";
    final MrsImagePyramidMetadata metadata = GeotoolsRasterUtils.calculateMetaData(localGreeceTif,
        localGreeceTif, protectionLevel);

    compareMetadata(metadata, localGreeceTif, false, 0.00001, protectionLevel);
  }

  @Test
  @Category(UnitTest.class)
  public void calculateMetaData3() throws Exception
  {
    String protectionLevel = "abc";
    final String[] inputs = { localGreeceTif };
    final MrsImagePyramidMetadata metadata = GeotoolsRasterUtils.calculateMetaData(inputs, localGreeceTif,
        true, protectionLevel, false, false);

    compareMetadata(metadata, localGreeceTif, false, 0.00001, protectionLevel);

    // Check the json serialized metadata
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    metadata.save(baos);
    final String s = (baos.toString("UTF-8"));
    final JSONObject m = new JSONObject(s);
    baos.close();
    Assert.assertNotNull(m.get("stats"));
    Assert.assertNotNull(m.getJSONArray("imageMetadata").getJSONObject(0).get("stats"));
    Assert.assertEquals(-5d, m.getJSONArray("stats").getJSONObject(0).getDouble("min"), 0.0000001);
    Assert.assertEquals(2193d, m.getJSONArray("stats").getJSONObject(0).getDouble("max"), 0.0000001);
    Assert.assertEquals(521.14518385749d, m.getJSONArray("stats").getJSONObject(0).getDouble("mean"),
        0.0000001);
    Assert.assertEquals(protectionLevel,  m.get("protectionLevel"));
  }

  @Test(expected = IOException.class)
  @Category(UnitTest.class)
  public void calculateMetaDataEPSG102031() throws Exception
  {
    // can't handle epsg:10231
    GeotoolsRasterUtils.calculateMetaData(epsg102031, "abc", 0.0);
  }

  @Test
  @Category(UnitTest.class)
  public void calculateMetaDataEPSG2100() throws Exception
  {
    String protectionLevel = "abc";
    final MrsImagePyramidMetadata metadata = GeotoolsRasterUtils.calculateMetaData(epsg2100,
        protectionLevel, 0.0);

    // haven't ingested the image, so the pyramid name should be ""
    compareMetadata(metadata, "", true, 0.1, protectionLevel);
  }

  @Test
  @Category(UnitTest.class)
  public void calculateMetaDataEPSG32634() throws Exception
  {
    String protectionLevel = "abc";
    final MrsImagePyramidMetadata metadata = GeotoolsRasterUtils.calculateMetaData(epsg32634,
        protectionLevel, 0.0);

    // haven't ingested the image, so the pyramid name should be ""
    compareMetadata(metadata, "", true, 0.1, protectionLevel);
  }

  @Test
  @Category(UnitTest.class)
  public void calculateMetaDataEPSG3785() throws Exception
  {
    String protectionLevel = "abc";
    final MrsImagePyramidMetadata metadata = GeotoolsRasterUtils.calculateMetaData(epsg3857,
        protectionLevel, 0.0);

    // haven't ingested the image, so the pyramid name should be ""
    compareMetadataEPSG3785(metadata, "", 0.1, protectionLevel);
  }

  @Test
  @Category(UnitTest.class)
  public void calculateMetaDataEPSG4314() throws Exception
  {
    String protectionLevel = "abc";
    final MrsImagePyramidMetadata metadata = GeotoolsRasterUtils.calculateMetaData(epsg4314,
        protectionLevel, 0.0);

    // haven't ingested the image, so the pyramid name should be ""
    compareMetadata(metadata, "", true, 0.1, protectionLevel);
  }

  @Test
  @Category(UnitTest.class)
  public void calculateMetaDataEPSG4326() throws Exception
  {
    String protectionLevel = "abc";
    final MrsImagePyramidMetadata metadata = GeotoolsRasterUtils.calculateMetaData(epsg4326,
        protectionLevel, 0.0);

    // haven't ingested the image, so the pyramid name should be ""
    compareMetadata(metadata, "", true, 0.1, protectionLevel);
  }

  @Ignore
  @Test
  @Category(UnitTest.class)
  public void calculateMetaDataMultiband() throws Exception
  {
    final String filename = Defs.INPUT + "multiband.png";

    String protectionLevel = "abc";
    final String[] inputs = { filename };
    final MrsImagePyramidMetadata metadata = GeotoolsRasterUtils.calculateMetaData(inputs, filename,
        true, protectionLevel, false, false);

    Assert.assertEquals("bad band count", 3, metadata.getBands());
    Assert.assertEquals("Bad stats min, band 1", 76, metadata.getStats(0).min, 0.0001);
    Assert.assertEquals("Bad stats max, band 1", 219, metadata.getStats(0).max, 0.0001);
    Assert.assertEquals("Bad stats min, band 2", 38, metadata.getStats(1).min, 0.0001);
    Assert.assertEquals("Bad stats max, band 3", 208, metadata.getStats(1).max, 0.0001);
    Assert.assertEquals("Bad stats min, band 3", 13, metadata.getStats(2).min, 0.0001);
    Assert.assertEquals("Bad stats max, band 3", 184, metadata.getStats(2).max, 0.0001);
    Assert.assertEquals(protectionLevel, metadata.getProtectionLevel());
  }

  @Test
  @Category(UnitTest.class)
  public void calculateZoomlevel() throws Exception
  {
    final AbstractGridCoverage2DReader reader = GeotoolsRasterUtils.openImage(hdfsGreeceTif);

    final int zoom = GeotoolsRasterUtils.calculateZoomlevel(reader, 512);

    Assert.assertEquals("Bad zoomlevel", 9, zoom);

    GeotoolsRasterUtils.closeStreamFromReader(reader);
  }

  @Test
  @Category(UnitTest.class)
  public void closeStreamFromReader() throws Exception
  {
    final AbstractGridCoverage2DReader reader = GeotoolsRasterUtils.openImage(hdfsGreeceTif);
    GeotoolsRasterUtils.closeStreamFromReader(reader);
  }

  @Test
  @Category(UnitTest.class)
  public void cutTileCategoricalExterior() throws Exception, IOException, IllegalArgumentException,
      NoSuchAuthorityCodeException, CoverageProcessingException, FactoryException,
      TransformException
  {
    final AbstractGridCoverage2DReader reader = GeotoolsRasterUtils.openImage(hdfsChessTif);

    final int zoom = GeotoolsRasterUtils.calculateZoomlevel(reader, 512);

    String protectionLevel = "abc";
    final MrsImagePyramidMetadata metadata = GeotoolsRasterUtils.calculateMetaData(hdfsChessTif.toString(),
        protectionLevel, 0.0);
    final GridCoverage2D image = GeotoolsRasterUtils.getImageFromReader(reader, "EPSG:4326");

    final double noData = metadata.getDefaultValue(0);

    final LongRectangle rect = metadata.getTileBounds(zoom);

    // pass categorical parameter as true, so the resample will use nearest neighbor type.
    final Raster raster = GeotoolsRasterUtils.cutTile(image, rect.getMinX(), rect.getMinY(), zoom,
        metadata.getTilesize(), metadata.getDefaultValues(), true);

    for (int y = raster.getMinY(); y < raster.getMinY() + raster.getHeight(); y++)
    {
      for (int x = raster.getMinX(); x < raster.getMinX() + raster.getWidth(); x++)
      {
        final double value = raster.getSampleDouble(x, y, 0);
        Assert.assertTrue("Invalid value", value == noData || value == 0 || value == 1);
      }
    }

    GeotoolsRasterUtils.closeStreamFromReader(reader);
  }

  @Test
  @Category(UnitTest.class)
  public void cutTileCategoricalInterior() throws Exception, IOException, IllegalArgumentException,
      NoSuchAuthorityCodeException, CoverageProcessingException, FactoryException,
      TransformException
  {
    final AbstractGridCoverage2DReader reader = GeotoolsRasterUtils.openImage(hdfsChessTif);

    final int zoom = GeotoolsRasterUtils.calculateZoomlevel(reader, 512);

    String protectionLevel = "abc";
    final MrsImagePyramidMetadata metadata = GeotoolsRasterUtils.calculateMetaData(hdfsChessTif.toString(),
        protectionLevel, 0.0);
    final GridCoverage2D image = GeotoolsRasterUtils.getImageFromReader(reader, "EPSG:4326");

    final LongRectangle rect = metadata.getTileBounds(zoom);

    // pass categorical parameter as true, so the resample will use nearest neighbor type.
    final Raster raster = GeotoolsRasterUtils.cutTile(image, rect.getCenterX(), rect.getCenterY(), zoom,
        metadata.getTilesize(), metadata.getDefaultValues(), true);

    for (int y = raster.getMinY(); y < raster.getMinY() + raster.getHeight(); y++)
    {
      for (int x = raster.getMinX(); x < raster.getMinX() + raster.getWidth(); x++)
      {
        final double value = raster.getSampleDouble(x, y, 0);
        Assert.assertTrue("Invalid value", value == 0 || value == 1);
      }
    }

    GeotoolsRasterUtils.closeStreamFromReader(reader);
  }

  @Test
  @Category(UnitTest.class)
  public void cutTileExterior() throws Exception, IOException, IllegalArgumentException,
      NoSuchAuthorityCodeException, CoverageProcessingException, FactoryException,
      TransformException
  {
    final AbstractGridCoverage2DReader reader = GeotoolsRasterUtils.openImage(hdfsChessTif);

    final int zoom = GeotoolsRasterUtils.calculateZoomlevel(reader, 512);

    String protectionLevel = "abc";
    final MrsImagePyramidMetadata metadata = GeotoolsRasterUtils.calculateMetaData(hdfsChessTif.toString(),
        protectionLevel, 0.0);
    final GridCoverage2D image = GeotoolsRasterUtils.getImageFromReader(reader, "EPSG:4326");

    final double noData = metadata.getDefaultValue(0);

    final LongRectangle rect = metadata.getTileBounds(zoom);
    final Raster raster = GeotoolsRasterUtils.cutTile(image, rect.getMinX(), rect.getMinY(), zoom,
        metadata.getTilesize(), metadata.getDefaultValues(), false);

    // exterior tile, can have nodata
    for (int y = raster.getMinY(); y < raster.getMinY() + raster.getHeight(); y++)
    {
      for (int x = raster.getMinX(); x < raster.getMinX() + raster.getWidth(); x++)
      {
        final double value = raster.getSampleDouble(x, y, 0);
        Assert.assertTrue("Invalid value " + value, value == noData || value > 0 || value < 1);
      }
    }

    GeotoolsRasterUtils.closeStreamFromReader(reader);
  }

  @Test
  @Category(UnitTest.class)
  public void cutTileHighLat() throws Exception, IOException, IllegalArgumentException,
      NoSuchAuthorityCodeException, CoverageProcessingException, FactoryException,
      TransformException
  {
    final AbstractGridCoverage2DReader reader = GeotoolsRasterUtils.openImage(hdfsHighLatTif);

    final int zoom = GeotoolsRasterUtils.calculateZoomlevel(reader, 512);

    String protectionLevel = "abc";
    final MrsImagePyramidMetadata metadata = GeotoolsRasterUtils.calculateMetaData(hdfsHighLatTif.toString(),
        protectionLevel, 0.0);
    final GridCoverage2D image = GeotoolsRasterUtils.getImageFromReader(reader, "EPSG:4326");

    final LongRectangle rect = metadata.getTileBounds(zoom);

    // pass categorical parameter as true, so the resample will use nearest neighbor type.
    final Raster raster = GeotoolsRasterUtils.cutTile(image, rect.getMinX(), rect.getMinY(), zoom,
        metadata.getTilesize(), metadata.getDefaultValues(), true);

    final int width = raster.getWidth();
    final int height = raster.getHeight();

    for (int y = 0; y < height; y++)
    {
      for (int x = 0; x < width; x++)
      {
        final double value = raster.getSampleDouble(x, y, 0);
        // System.out.println(value);
        Assert.assertEquals("Invalid value", 0.0, value, 0.0000001);
      }
    }

    GeotoolsRasterUtils.closeStreamFromReader(reader);
  }

  @Test
  @Category(UnitTest.class)
  public void cutTileInterior() throws Exception, IOException, IllegalArgumentException,
      NoSuchAuthorityCodeException, CoverageProcessingException, FactoryException,
      TransformException
  {
    final AbstractGridCoverage2DReader reader = GeotoolsRasterUtils.openImage(hdfsChessTif);

    final int zoom = GeotoolsRasterUtils.calculateZoomlevel(reader, 512);

    String protectionLevel = "abc";
    final MrsImagePyramidMetadata metadata = GeotoolsRasterUtils.calculateMetaData(hdfsChessTif.toString(),
        protectionLevel, 0.0);
    final GridCoverage2D image = GeotoolsRasterUtils.getImageFromReader(reader, "EPSG:4326");

    final LongRectangle rect = metadata.getTileBounds(zoom);
    final Raster raster = GeotoolsRasterUtils.cutTile(image, rect.getCenterX(), rect.getCenterY(), zoom,
        metadata.getTilesize(), metadata.getDefaultValues(), false);

    // interior tile, no nodata
    for (int y = raster.getMinY(); y < raster.getMinY() + raster.getHeight(); y++)
    {
      for (int x = raster.getMinX(); x < raster.getMinX() + raster.getWidth(); x++)
      {
        final double value = raster.getSampleDouble(x, y, 0);
        Assert.assertTrue("Invalid value " + value, value > 0 || value < 1);
      }
    }

    GeotoolsRasterUtils.closeStreamFromReader(reader);
  }

  @Test
  @Category(UnitTest.class)
  public void fastAccepts() throws MalformedURLException
  {
    java.net.URL url = new java.net.URL(localGreeceTif);
    Assert.assertEquals("Format not found", true, GeotoolsRasterUtils.fastAccepts(url));
  }

  @Test
  @Category(UnitTest.class)
  public void fastAcceptsUnknownAccepts()
  {
    Assert.assertEquals("Format not found", false, GeotoolsRasterUtils.fastAccepts(localGreecePng));
  }

  @Test
  @Category(UnitTest.class)
  public void fastFormatFinder() throws MalformedURLException
  {
    AbstractGridFormat format;

    format = GeotoolsRasterUtils.fastFormatFinder(new java.net.URL(localGreeceTif));

    Assert.assertEquals("Format not found", "GeoTiffFormat", format.getClass().getSimpleName());
  }

  @Test
  @Category(UnitTest.class)
  public void fastFormatFinderUnknownFormat()
  {
    AbstractGridFormat format;

    format = GeotoolsRasterUtils.fastFormatFinder(localGreecePng);

    // Geotools doesn't know about pngs (no geospatial info)
    Assert.assertEquals("Format not found", null, format);
  }

  @Test
  @Category(UnitTest.class)
  public void getImageFromReader() throws Exception
  {
    final AbstractGridCoverage2DReader reader = GeotoolsRasterUtils.openImage(hdfsGreeceTif);

    final GridCoverage2D image = GeotoolsRasterUtils.getImageFromReader(reader, "EPSG:4326");

    Assert.assertNotSame("Couldn't open reader", null, image);
    Assert.assertEquals("Could not read image", "geotiff_coverage", image.getName().toString());

    GeotoolsRasterUtils.closeStreamFromReader(reader);

  }

  @Test
  @Category(UnitTest.class)
  public void openHdfsImage() throws Exception
  {
    final AbstractGridCoverage2DReader reader = GeotoolsRasterUtils.openImage(hdfsGreeceTif);

    Assert.assertNotSame("Couldn't open reader", null, reader);

    Assert.assertEquals("Format not found", "GeoTiffReader", reader.getClass().getSimpleName());

    GeotoolsRasterUtils.closeStreamFromReader(reader);
  }
  

  @Test(expected = IOException.class)
  @Category(UnitTest.class)
  public void openHdfsImageBadImage() throws IOException
  {
    GeotoolsRasterUtils.openImage(hdfsGreecePng);
  }

  @Before
  public void setUp()
  {
  }

  private void compareMetadata(final MrsImagePyramidMetadata metadata, final String pyramidname,
      final boolean skipPixelTest, final double boundsEpsilon, String protectionLevel)
  {
    Assert.assertNotNull("Bad metadata", metadata);

    Assert.assertEquals("bad pyramid name", pyramidname, metadata.getPyramid());

    Assert.assertEquals("bad band count", 1, metadata.getBands());

    final Bounds bounds = metadata.getBounds();

    Assert.assertEquals("Bad bounds - min x", 24.0, bounds.getMinX(), boundsEpsilon);
    Assert.assertEquals("Bad bounds - min y", 40.5, bounds.getMinY(), boundsEpsilon);
    Assert.assertEquals("Bad bounds - max x", 25.0, bounds.getMaxX(), boundsEpsilon);
    Assert.assertEquals("Bad bounds - max y", 41.5, bounds.getMaxY(), boundsEpsilon);

    final double[] defaults = metadata.getDefaultValues();
    Assert.assertEquals("Bad default values length", 1, defaults.length);
    Assert.assertEquals("Bad default values ", -32768.0, defaults[0], 0);

    final int max = metadata.getMaxZoomLevel();
    Assert.assertEquals("Bad max zoom", 9, max);

    Assert.assertEquals("Bad image name", "" + max, metadata.getName(max));

    if (!skipPixelTest)
    {
      final LongRectangle pb = metadata.getPixelBounds(max);
      Assert.assertEquals("Bad pixel bounds - min x", 0, pb.getMinX());
      Assert.assertEquals("Bad pixel bounds - min y", 0, pb.getMinY());
      Assert.assertEquals("Bad pixel bounds - max x", 728, pb.getMaxX());
      Assert.assertEquals("Bad pixel bounds - max y", 728, pb.getMaxY());
    }

    metadata.getTileBounds(max);

    Assert.assertEquals(protectionLevel, metadata.getProtectionLevel());
    final LongRectangle tb = metadata.getTileBounds(max);
    Assert.assertEquals("Bad tile bounds - min x", 290, tb.getMinX());
    Assert.assertEquals("Bad tile bounds - min y", 185, tb.getMinY());
    Assert.assertEquals("Bad tile bounds - max x", 291, tb.getMaxX());
    Assert.assertEquals("Bad tile bounds - max y", 187, tb.getMaxY());

    final ImageMetadata[] im = metadata.getImageMetadata();

    Assert.assertEquals("Bad image metadata length", max + 1, im.length);

    Assert.assertEquals("Bad tilesize", 512, metadata.getTilesize());

    Assert.assertEquals("Bad tiletype", 2, metadata.getTileType());

  }

  private void compareMetadataEPSG3785(final MrsImagePyramidMetadata metadata, final String pyramidname,
      final double boundsEpsilon, final String protectionLevel)
  {
    Assert.assertNotNull("Bad metadata", metadata);

    Assert.assertEquals("bad pyramid name", pyramidname, metadata.getPyramid());

    Assert.assertEquals("bad band count", 1, metadata.getBands());

    final Bounds bounds = metadata.getBounds();

    Assert.assertEquals("Bad bounds - min x", 24.0, bounds.getMinX(), boundsEpsilon);
    Assert.assertEquals("Bad bounds - min y", 40.689, bounds.getMinY(), boundsEpsilon);
    Assert.assertEquals("Bad bounds - max x", 25.0, bounds.getMaxX(), boundsEpsilon);
    Assert.assertEquals("Bad bounds - max y", 41.691, bounds.getMaxY(), boundsEpsilon);

    final double[] defaults = metadata.getDefaultValues();
    Assert.assertEquals("Bad default values length", 1, defaults.length);
    Assert.assertEquals("Bad default values ", -32768.0, defaults[0], 0);

    final int max = metadata.getMaxZoomLevel();
    Assert.assertEquals("Bad max zoom", 9, max);

    Assert.assertEquals("Bad image name", "" + max, metadata.getName(max));

    final LongRectangle pb = metadata.getPixelBounds(max);
    Assert.assertEquals("Bad pixel bounds - min x", 0, pb.getMinX());
    Assert.assertEquals("Bad pixel bounds - min y", 0, pb.getMinY());
    Assert.assertEquals("Bad pixel bounds - max x", 729, pb.getMaxX());
    Assert.assertEquals("Bad pixel bounds - max y", 730, pb.getMaxY());

    metadata.getTileBounds(max);
    Assert.assertEquals(protectionLevel, metadata.getProtectionLevel());

    final LongRectangle tb = metadata.getTileBounds(max);
    Assert.assertEquals("Bad tile bounds - min x", 290, tb.getMinX());
    Assert.assertEquals("Bad tile bounds - min y", 185, tb.getMinY());
    Assert.assertEquals("Bad tile bounds - max x", 291, tb.getMaxX());
    Assert.assertEquals("Bad tile bounds - max y", 187, tb.getMaxY());

    final ImageMetadata[] im = metadata.getImageMetadata();

    Assert.assertEquals("Bad image metadata length", max + 1, im.length);

    Assert.assertEquals("Bad tilesize", 512, metadata.getTilesize());

    Assert.assertEquals("Bad tiletype", 2, metadata.getTileType());

  }

}
