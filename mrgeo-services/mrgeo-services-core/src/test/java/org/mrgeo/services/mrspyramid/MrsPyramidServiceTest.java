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
package org.mrgeo.services.mrspyramid;

import junit.framework.Assert;
import org.apache.commons.io.IOUtils;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.data.DataProviderNotFound;
import org.mrgeo.data.raster.RasterUtils;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.rasterops.ColorScale;
import org.mrgeo.rasterops.ColorScale.ColorScaleException;
import org.mrgeo.resources.mrspyramid.ColorScaleResourceTest;
import org.mrgeo.resources.mrspyramid.RasterResourceTest;
import org.mrgeo.services.mrspyramid.rendering.ImageRenderer;
import org.mrgeo.test.TestUtils;
import org.mrgeo.utils.Bounds;
import org.mrgeo.utils.ImageUtils;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;
import java.awt.image.BufferedImage;
import java.awt.image.Raster;
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

/**
 * @author Steve Ingram
 *         Date: 10/26/13
 */
@SuppressWarnings("static-method")
public class MrsPyramidServiceTest {

  // only set this to true to generate new baseline images after correcting tests; image comparison
  // tests won't be run when is set to true
  public final static boolean GEN_BASELINE_DATA_ONLY = false;

  private static String islandsElevation = "IslandsElevation-v2-2";
  private static String islandsElevationColorScale = "IslandsElevation-v2-2-color-scale";
  private static String islandsElevationNoPyramid = "IslandsElevation-v2-no-pyramid";
  private static String islandsElevation_unqualified = islandsElevation;
  private static String islandsElevationColorScale_unqualified = islandsElevationColorScale;
  private static String islandsElevationNoPyramid_unqualified = islandsElevationNoPyramid;
  private static String islandsElevationNonExistant = "IslandsElevation-Non-Existent";
  // bounds is completely outside of the image bounds
  public final static String ISLANDS_ELEVATION_V2_OUT_OF_BOUNDS = "160.312500,-12.656250,161.718750,-11.250000";
  // bounds is within the image bounds and results in a single source tile being
  // accessed;
  // zoom level = 8
  public final static String ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_TILE = "160.312500,-11.250000,163.125000,-8.437500";
  // bounds is within the image bounds and results in multiple source tiles
  // being accessed;
  // zoom level = 9
  public final static String ISLANDS_ELEVATION_V2_IN_BOUNDS_MULTIPLE_TILES = "160.312500,-11.250000,161.718750,-9.843750";

  public MrsPyramidServiceTest() {
  }

  @Test
  @Category(UnitTest.class)
  public void testFormatElapsedTime()
  {
    MrsPyramidService testInstance = new MrsPyramidService(new Properties());

    assertEquals(testInstance.formatElapsedTime(7200d), "2h");
    assertEquals(testInstance.formatElapsedTime(60d), "1m");
    assertEquals(testInstance.formatElapsedTime(1d), "1s");
    assertEquals(testInstance.formatElapsedTime(86400d), "1d");
    assertEquals(testInstance.formatElapsedTime(90000d), "1d:1h");
    assertEquals(testInstance.formatElapsedTime(90060d), "1d:1h:1m");
    assertEquals(testInstance.formatElapsedTime(90061d), "1d:1h:1m:1s");
    assertEquals(testInstance.formatElapsedTime(14000d), "3h:53m:20s");
    assertEquals(testInstance.formatElapsedTime(0d), "0s");
    assertEquals(testInstance.formatElapsedTime(null), "0s");
  }

  @Test
  @Category(UnitTest.class)
  public void testFormatValue()
  {
    MrsPyramidService testInstance = new MrsPyramidService(new Properties());
    assertEquals(testInstance.formatValue(7200d, "seconds"), "2h");
    assertEquals(testInstance.formatValue(7200d, ""), "7200.0");
    assertEquals(testInstance.formatValue(75d, "degrees"), "75deg");
    assertEquals(testInstance.formatValue(1000d, "meters"), "1000m");
    assertEquals(testInstance.formatValue(0.56d, "percent"), "56%");
    assertEquals(testInstance.formatValue(0.046d, "percent"), "5%");
  }

  @Test
  @Category(UnitTest.class)
  public void testCreateColorSwatch() throws Exception
  {
    String input = TestUtils.composeInputDir(ColorScaleResourceTest.class);
    MrsPyramidService testInstance = new MrsPyramidService(new Properties());
    int width = 100;
    int height = 10;

    Raster ri = testInstance.createColorScaleSwatch(createRainbowColorScale(), "png", width, height);
    assertEquals(ri.getWidth(), width);
    assertEquals(ri.getHeight(), height);
    TestUtils.compareRasters(new File(input + "colorswatch.png"), ri);

    width = 20;
    height = 200;

    ri = testInstance.createColorScaleSwatch(createRainbowColorScale(), "png", width, height);
    assertEquals(ri.getWidth(), width);
    assertEquals(ri.getHeight(), height);
    TestUtils.compareRasters(new File(input + "colorswatchvertical.png"), ri);
  }

  private ColorScale createRainbowColorScale() throws ColorScaleException {
    String colorScaleXml = "<ColorMap name=\"Rainbow\">\n" +
        "  <Scaling>MinMax</Scaling> <!-- Could also be Absolute -->\n" +
        "  <ReliefShading>0</ReliefShading>\n" +
        "  <Interpolate>1</Interpolate>\n" +
        "  <NullColor color=\"0,0,0\" opacity=\"0\"/>\n" +
        "  <Color value=\"0.0\" color=\"0,0,127\" opacity=\"255\"/>\n" +
        "  <Color value=\"0.2\" color=\"0,0,255\"/> <!-- if not specified an opacity defaults to 255 -->\n" +
        "  <Color value=\"0.4\" color=\"0,255,255\"/>\n" +
        "  <Color value=\"0.6\" color=\"0,255,0\"/>\n" +
        "  <Color value=\"0.8\" color=\"255,255,0\"/>\n" +
        "  <Color value=\"1.0\" color=\"255,0,0\"/>\n" +
        "</ColorMap>";
    return ColorScale.loadFromXML(new ByteArrayInputStream(colorScaleXml.getBytes()));
  }

  @Test(expected = DataProviderNotFound.class)
  @Category(UnitTest.class)
  public void testGetRasterNonExistent() throws Exception
  {
    testIslandsElevationFor("jpeg", "512", "512", ISLANDS_ELEVATION_V2_IN_BOUNDS_MULTIPLE_TILES,
            islandsElevationNonExistant, null, "testGetRasterJpgMultipleSourceTiles.jpg",
      "[image/jpeg]", ImageUtils.createImageReader("image/jpeg"));
  }

  // The following test is commented out because we don't support having a
  // default color scale for each specific image any more.
//  @Test
//  @Category(UnitTest.class)
//  public void testGetRasterJpgMultipleSourceTiles() throws Exception
//  {
//    testIslandsElevationFor("jpeg", "512", "512", ISLANDS_ELEVATION_V2_IN_BOUNDS_MULTIPLE_TILES,
//      islandsElevation_unqualified, null, "testGetRasterJpgMultipleSourceTiles.jpg",
//      "[image/jpeg]", ImageUtils.createImageReader("image/jpeg"));
//  }

  @Test
  @Category(UnitTest.class)
  public void testGetRasterJpgMultipleSourceTilesAspectColorScale() throws Exception
  {
    // test jpg, multiple source tile with color scale passed in
    testIslandsElevationFor("jpeg", "512", "512", ISLANDS_ELEVATION_V2_IN_BOUNDS_MULTIPLE_TILES,
      islandsElevation_unqualified, getAspectColorScale(),
      "testGetRasterJpgMultipleSourceTilesAspectColorScale.jpg", "[image/jpeg]",
      ImageUtils.createImageReader("image/jpeg"));
  }

  @Test
  @Category(UnitTest.class)
  public void testGetRasterJpgMultipleSourceTilesAspectColorScaleWithZoom() throws Exception
  {
    // test jpg, multiple source tile with color scale passed in
    testIslandsElevationFor("jpeg", "512", "512", ISLANDS_ELEVATION_V2_IN_BOUNDS_MULTIPLE_TILES,
      islandsElevation_unqualified, getAspectColorScale(), 8,
      "testGetRasterJpgMultipleSourceTilesAspectColorScale.jpg", "[image/jpeg]",
      ImageUtils.createImageReader("image/jpeg"));
  }

  // The following test is commented out because we don't support having a
  // default color scale for each specific image any more.
//  @Test
//  @Category(UnitTest.class)
//  public void testGetRasterJpgMultipleSourceTilesWithColorScale() throws Exception
//  {
//    // test jpg, multiple source tile with color scale defined in Pyramid
//    testIslandsElevationFor("jpeg", "512", "512", ISLANDS_ELEVATION_V2_IN_BOUNDS_MULTIPLE_TILES,
//      islandsElevationColorScale_unqualified, null,
//      "testGetRasterJpgMultipleSourceTilesWithColorScale.jpg", "[image/jpeg]",
//      ImageUtils.createImageReader("image/jpeg"));
//  }

  // The following test is commented out because we don't support having a
  // default color scale for each specific image any more.
//  @Test
//  @Category(UnitTest.class)
//  public void testGetRasterJpgMultipleSourceTilesWithColorScaleWithZoom() throws Exception
//  {
//    // test jpg, multiple source tile with color scale defined in Pyramid
//    testIslandsElevationFor("jpeg", "512", "512", ISLANDS_ELEVATION_V2_IN_BOUNDS_MULTIPLE_TILES,
//      islandsElevationColorScale_unqualified, null, 8,
//      "testGetRasterJpgMultipleSourceTilesWithColorScale.jpg", "[image/jpeg]",
//      ImageUtils.createImageReader("image/jpeg"));
//  }

  // The following test is commented out because we don't support having a
  // default color scale for each specific image any more.
//  @Test
//  @Category(UnitTest.class)
//  public void testGetRasterJpgMultipleSourceTilesWithZoom() throws Exception
//  {
//    // test jpg, multiple source tile
//    testIslandsElevationFor("jpeg", "512", "512", ISLANDS_ELEVATION_V2_IN_BOUNDS_MULTIPLE_TILES,
//      islandsElevation_unqualified, null, 8, "testGetRasterJpgMultipleSourceTiles.jpg",
//      "[image/jpeg]", ImageUtils.createImageReader("image/jpeg"));
//  }

  // The following test is commented out because we don't support having a
  // default color scale for each specific image any more.
//  @Test
//  @Category(UnitTest.class)
//  public void testGetRasterJpgSingleSourceTile() throws Exception
//  {
//    testIslandsElevationFor("jpeg", "512", "512", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_TILE,
//      islandsElevation_unqualified, null, "testGetRasterJpgSingleSourceTile.jpg", "[image/jpeg]",
//      ImageUtils.createImageReader("image/jpeg"));
//  }

  @Test
  @Category(UnitTest.class)
  public void testGetRasterJpgSingleSourceTileAspectColorScale() throws Exception
  {
    // test jpg, single source tile with color scale passed in
    testIslandsElevationFor("jpeg", "512", "512", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_TILE,
      islandsElevation_unqualified, getAspectColorScale(),
      "testGetRasterJpgSingleSourceTileAspectColorScale.jpg", "[image/jpeg]",
      ImageUtils.createImageReader("image/jpeg"));
  }

  @Test
  @Category(UnitTest.class)
  public void testGetRasterJpgSingleSourceTileAspectColorScaleWithZoom() throws Exception
  {
    // test jpg, single source tile with color scale passed in
    testIslandsElevationFor("jpeg", "512", "512", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_TILE,
      islandsElevation_unqualified, getAspectColorScale(), 7,
      "testGetRasterJpgSingleSourceTileAspectColorScale.jpg", "[image/jpeg]",
      ImageUtils.createImageReader("image/jpeg"));
  }

  // The following test is commented out because we don't support having a
  // default color scale for each specific image any more.
//  @Test
//  @Category(UnitTest.class)
//  public void testGetRasterJpgSingleSourceTileWithColorScale() throws Exception
//  {
//    // test jpg, single source tile with color scale defined in Pyramid
//    testIslandsElevationFor("jpg", "512", "512", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_TILE,
//      islandsElevationColorScale_unqualified, null,
//      "testGetRasterJpgSingleSourceTileWithColorScale.jpg", "[image/jpeg]",
//      ImageUtils.createImageReader("image/jpeg"));
//  }

  // The following test is commented out because we don't support having a
  // default color scale for each specific image any more.
//  @Test
//  @Category(UnitTest.class)
//  public void testGetRasterJpgSingleSourceTileWithColorScaleWithZoom() throws Exception
//  {
//    // test jpg, single source tile with color scale defined in Pyramid
//    testIslandsElevationFor("jpg", "512", "512", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_TILE,
//      islandsElevationColorScale_unqualified, null, 7,
//      "testGetRasterJpgSingleSourceTileWithColorScale.jpg", "[image/jpeg]",
//      ImageUtils.createImageReader("image/jpeg"));
//  }

  // The following test is commented out because we don't support having a
  // default color scale for each specific image any more.
//  @Test
//  @Category(UnitTest.class)
//  public void testGetRasterJpgSingleSourceTileWithZoom() throws Exception
//  {
//    // test jpg, single source tile
//    testIslandsElevationFor("jpeg", "512", "512", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_TILE,
//      islandsElevation_unqualified, null, 7, "testGetRasterJpgSingleSourceTile.jpg",
//      "[image/jpeg]", ImageUtils.createImageReader("image/jpeg"));
//  }

//  @Test
//  @Category(UnitTest.class)
//  public void testGetRasterOutOfBoundsJpg() throws Exception
//  {
//    testIslandsElevationFor("jpg", "512", "512", ISLANDS_ELEVATION_V2_OUT_OF_BOUNDS,
//      islandsElevation_unqualified, null, "testGetRasterOutOfBoundsJpg.jpg", "[image/jpeg]",
//      ImageUtils.createImageReader("image/jpeg"));
//  }
//
  @Test
  @Category(UnitTest.class)
  public void testGetRasterOutOfBoundsJpgWithZoom() throws Exception
  {
    // test out of bounds jpg
    testIslandsElevationFor("jpg", "512", "512", ISLANDS_ELEVATION_V2_OUT_OF_BOUNDS,
      islandsElevation_unqualified, getAspectColorScale(), -1, "testGetRasterOutOfBoundsJpg.jpg", "[image/jpeg]",
      ImageUtils.createImageReader("image/jpeg"));
  }

  @Test
  @Category(UnitTest.class)
  public void testGetRasterOutOfBoundsPng() throws Exception
  {
    testIslandsElevationFor("png", "512", "512", ISLANDS_ELEVATION_V2_OUT_OF_BOUNDS,
      islandsElevation_unqualified, getAspectColorScale(), "testGetRasterOutOfBoundsPng.png", "[image/png]",
      ImageUtils.createImageReader("image/png"));
  }

  @Test
  @Category(UnitTest.class)
  public void testGetRasterOutOfBoundsPngWithZoom() throws Exception
  {
    // test out of bounds png
    testIslandsElevationFor("png", "512", "512", ISLANDS_ELEVATION_V2_OUT_OF_BOUNDS,
      islandsElevation_unqualified, getAspectColorScale(), -1, "testGetRasterOutOfBoundsPng.png", "[image/png]",
      ImageUtils.createImageReader("image/png"));
  }

  @Test
  @Category(UnitTest.class)
  public void testGetRasterOutOfBoundsTif() throws Exception
  {
    testIslandsElevationFor("tiff", "512", "512", ISLANDS_ELEVATION_V2_OUT_OF_BOUNDS,
      islandsElevation_unqualified, getAspectColorScale(), "testGetRasterOutOfBoundsTif.tif", "[image/tiff]",
      ImageUtils.createImageReader("image/tiff"));
  }

  @Test
  @Category(UnitTest.class)
  public void testGetRasterOutOfBoundsTifWithZoom() throws Exception
  {
    // test out of bounds tiff
    testIslandsElevationFor("tiff", "512", "512", ISLANDS_ELEVATION_V2_OUT_OF_BOUNDS,
      islandsElevation_unqualified, getAspectColorScale(), -1, "testGetRasterOutOfBoundsTif.tif", "[image/tiff]",
      ImageUtils.createImageReader("image/tiff"));
  }

  @Test
  @Category(UnitTest.class)
  public void testGetRasterPngLargerThanTileSize() throws Exception
  {
    testIslandsElevationFor("png", "1024", "1024", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_TILE,
      islandsElevation_unqualified, getDefaultColorScale(), "testGetRasterPngLargerThanTileSize.png",
      "[image/png]", ImageUtils.createImageReader("image/png"));
  }

  @Test
  @Category(UnitTest.class)
  public void testGetRasterPngMultipleSourceTiles() throws Exception
  {
    testIslandsElevationFor("png", "512", "512", ISLANDS_ELEVATION_V2_IN_BOUNDS_MULTIPLE_TILES,
      islandsElevation_unqualified, getDefaultColorScale(), "testGetRasterPngMultipleSourceTiles.png",
      "[image/png]", ImageUtils.createImageReader("image/png"));
  }

  @Test
  @Category(UnitTest.class)
  public void testGetRasterPngMultipleSourceTilesWithAspectColorScale() throws Exception
  {
    // test png, multiple source tiles with color scale passed in
    testIslandsElevationFor("png", "512", "512", ISLANDS_ELEVATION_V2_IN_BOUNDS_MULTIPLE_TILES,
      islandsElevation_unqualified, getAspectColorScale(),
      "testGetRasterPngMultipleSourceTilesWithAspectColorScale.png", "[image/png]",
      ImageUtils.createImageReader("image/png"));
  }

  @Test
  @Category(UnitTest.class)
  public void testGetRasterPngMultipleSourceTilesWithColorScale() throws Exception
  {
    // test png, multiple source tile with color scale defined in Pyramid
    testIslandsElevationFor("png", "512", "512", ISLANDS_ELEVATION_V2_IN_BOUNDS_MULTIPLE_TILES,
      islandsElevationColorScale_unqualified, getDefaultColorScale(),
      "testGetRasterPngMultipleSourceTilesWithColorScale.png", "[image/png]",
      ImageUtils.createImageReader("image/png"));
  }

  @Test
  @Category(UnitTest.class)
  public void testGetRasterPngMultipleSourceTilesWithColorScaleWithZoom() throws Exception
  {
    // test png, multiple source tile with color scale defined in Pyramid
    testIslandsElevationFor("png", "512", "512", ISLANDS_ELEVATION_V2_IN_BOUNDS_MULTIPLE_TILES,
      islandsElevationColorScale_unqualified, getDefaultColorScale(), 8,
      "testGetRasterPngMultipleSourceTilesWithColorScale.png", "[image/png]",
      ImageUtils.createImageReader("image/png"));
  }

  @Test
  @Category(UnitTest.class)
  public void testGetRasterPngMultipleSourceTilesWithZoom() throws Exception
  {
    // test png, multiple source tiles
    testIslandsElevationFor("png", "512", "512", ISLANDS_ELEVATION_V2_IN_BOUNDS_MULTIPLE_TILES,
      islandsElevation_unqualified, getDefaultColorScale(), 8, "testGetRasterPngMultipleSourceTiles.png",
      "[image/png]", ImageUtils.createImageReader("image/png"));
  }

  @Test
  @Category(UnitTest.class)
  public void testGetRasterPngNonExistingZoomLevelAboveWithoutPyramids() throws Exception
  {
    // pyramid only has a single zoom level = 10; pass in zoom level = 8
    testIslandsElevationFor("png", "512", "512", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_TILE,
      islandsElevationNoPyramid_unqualified, getAspectColorScale(),
      "testGetRasterPngNonExistingZoomLevelAboveWithoutPyramids.png", "[image/png]",
      ImageUtils.createImageReader("image/png"));
  }

  @Test
  @Category(UnitTest.class)
  public void testGetRasterPngRectangularTileSize() throws Exception
  {
    testIslandsElevationFor("png", "700", "300", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_TILE,
      islandsElevation_unqualified, getDefaultColorScale(), "testGetRasterPngRectangularTileSize.png",
      "[image/png]", ImageUtils.createImageReader("image/png"));
  }

  @Test
  @Category(UnitTest.class)
  public void testGetRasterPngSingleSourceTile() throws Exception
  {
    testIslandsElevationFor("png", "512", "512", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_TILE,
      islandsElevation_unqualified, getDefaultColorScale(), "testGetRasterPngSingleSourceTile.png", "[image/png]",
      ImageUtils.createImageReader("image/png"));
  }

  @Test
  @Category(UnitTest.class)
  public void testGetRasterPngSingleSourceTileWithAspectColorScale() throws Exception
  {
    // test png, single source tile with color scale passed in
    testIslandsElevationFor("png", "512", "512", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_TILE,
      islandsElevation_unqualified, getAspectColorScale(),
      "testGetRasterPngSingleSourceTileWithAspectColorScale.png", "[image/png]",
      ImageUtils.createImageReader("image/png"));
  }

  @Test
  @Category(UnitTest.class)
  public void testGetRasterPngSingleSourceTileWithAspectColorScaleWithZoom() throws Exception
  {
    // test png, single source tile with color scale passed in
    testIslandsElevationFor("png", "512", "512", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_TILE,
      islandsElevation_unqualified, getAspectColorScale(), 7,
      "testGetRasterPngSingleSourceTileWithAspectColorScale.png", "[image/png]",
      ImageUtils.createImageReader("image/png"));
  }

  @Test
  @Category(UnitTest.class)
  public void testGetRasterPngSingleSourceTileWithColorScale() throws Exception
  {
    // test png, single source tile with color scale defined in Pyramid
    testIslandsElevationFor("png", "512", "512", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_TILE,
      islandsElevationColorScale_unqualified, getDefaultColorScale(),
      "testGetRasterPngSingleSourceTileWithColorScale.png", "[image/png]",
      ImageUtils.createImageReader("image/png"));
  }

  @Test
  @Category(UnitTest.class)
  public void testGetRasterPngSingleSourceTileWithColorScaleWithZoom() throws Exception
  {
    // test png, single source tile with color scale defined in Pyramid
    testIslandsElevationFor("png", "512", "512", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_TILE,
      islandsElevationColorScale_unqualified, getDefaultColorScale(), 7,
      "testGetRasterPngSingleSourceTileWithColorScale.png", "[image/png]",
      ImageUtils.createImageReader("image/png"));
  }

  @Test
  @Category(UnitTest.class)
  public void testGetRasterPngSingleSourceTileWithZoom() throws Exception
  {
    // test png, single source tile
    testIslandsElevationFor("png", "512", "512", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_TILE,
      islandsElevation_unqualified, getDefaultColorScale(), 7, "testGetRasterPngSingleSourceTile.png",
      "[image/png]", ImageUtils.createImageReader("image/png"));
  }

  @Test
  @Category(UnitTest.class)
  public void testGetRasterTifMultipleSourceTiles() throws Exception
  {
    // test tif, multiple source tile
    testIslandsElevationFor("tiff", "512", "512", ISLANDS_ELEVATION_V2_IN_BOUNDS_MULTIPLE_TILES,
      islandsElevation_unqualified, getAspectColorScale(), "testGetRasterTifMultipleSourceTiles.tif",
      "[image/tiff]", ImageUtils.createImageReader("image/tiff"));
  }

  @Test
  @Category(UnitTest.class)
  public void testGetRasterTifMultipleSourceTilesWithZoom() throws Exception
  {
    // test tif, multiple source tile
    testIslandsElevationFor("tiff", "512", "512", ISLANDS_ELEVATION_V2_IN_BOUNDS_MULTIPLE_TILES,
      islandsElevation_unqualified, getAspectColorScale(), 8, "testGetRasterTifMultipleSourceTiles.tif",
      "[image/tiff]", ImageUtils.createImageReader("image/tiff"));
  }

  @Test
  @Category(UnitTest.class)
  public void testGetRasterTifSingleSourceTile() throws Exception
  {
    // test tif, single source tile
    testIslandsElevationFor("tiff", "512", "512", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_TILE,
      islandsElevation_unqualified, getAspectColorScale(), "testGetRasterTifSingleSourceTile.tif", "[image/tiff]",
      ImageUtils.createImageReader("image/tiff"));
  }

  @Test
  @Category(UnitTest.class)
  public void testGetRasterTifSingleSourceTileWithZoom() throws Exception
  {
    // test tif, single source tile
    testIslandsElevationFor("tiff", "512", "512", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_TILE,
      islandsElevation_unqualified, getAspectColorScale(), 7, "testGetRasterTifSingleSourceTile.tif",
      "[image/tiff]", ImageUtils.createImageReader("image/tiff"));
  }


  @Test
  @Category(UnitTest.class)
  public void testGetRasterPngMultipleSourceTilesWithAspectColorScaleWithZoom() throws Exception
  {
    // test png, multiple source tiles with color scale passed in
    testIslandsElevationFor("png", "512", "512", ISLANDS_ELEVATION_V2_IN_BOUNDS_MULTIPLE_TILES,
      islandsElevation_unqualified, getAspectColorScale(), 8,
      "testGetRasterPngMultipleSourceTilesWithAspectColorScale.png", "[image/png]",
      ImageUtils.createImageReader("image/png"));
  }
  /*
   * TODO: move these color scale related tests to a new test or merge with the
   * ColorScaleResourceTest
   */

  private void testIslandsElevationFor(final String format, final String width,
    final String height, final String bbox,
    final String reqImgName, final String colorScale, final int zoomLevel,
    final String goldenImgName,
    final String expectedContentType, final ImageReader reader) throws Exception
    {
    ColorScale cs = null;
    Properties mrgeoProperties = MrGeoProperties.getInstance();
    Properties unusedMrgeoProperties = new Properties();
    mrgeoProperties.put(MrGeoConstants.MRGEO_ENV_HOME, TestUtils.composeInputDir(RasterResourceTest.class));
    mrgeoProperties.put("image.base", "file://" + TestUtils.composeInputDir(RasterResourceTest.class));
    mrgeoProperties.put(MrGeoConstants.MRGEO_HDFS_COLORSCALE, "file://" + TestUtils.composeInputDir(RasterResourceTest.class) + "color-scales");
    MrsPyramidService service = new MrsPyramidService(unusedMrgeoProperties);

    String[] bBoxValues = bbox.split(",");
    if (bBoxValues.length != 4)
    {
      throw new IllegalArgumentException("Bounding box must have four comma delimited arguments.");
    }
    double minX = Double.valueOf(bBoxValues[0]);
    double minY = Double.valueOf(bBoxValues[1]);
    double maxX = Double.valueOf(bBoxValues[2]);
    double maxY = Double.valueOf(bBoxValues[3]);

    Bounds bounds = new Bounds(minX, minY, maxX, maxY);

    int w = Integer.valueOf(width);
    int h = Integer.valueOf(height);

    // retrieve jpg
    if (colorScale != null)
    {
      cs = service.getColorScaleFromJSON(colorScale);
    } else {
      // TODO: Not sure if the following is correct. It used to get the color scale
      // defined for the pyramid itself. That is no longer supported.
//      cs = service.getColorScaleFromPyramid(pyramidPathStr);
//      cs = ColorScaleManager.fromJSON(getDefaultColorScale());
    }


    if (zoomLevel != -1)
    {
      if ( !service.isZoomLevelValid(reqImgName, (Properties)null, zoomLevel) ) {
        throw new IllegalArgumentException("Zoom level " + zoomLevel + " is not in pyramid " + reqImgName);
      }
    }
    String goldenPathStr = TestUtils.composeInputDir(RasterResourceTest.class) + goldenImgName;

    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    InputStream is = null;
    ImageRenderer renderer = service.getImageRenderer(format);
    Raster result = renderer.renderImage(reqImgName, bounds, w, h, (Properties)null, null);
    double[] extrema = renderer.getExtrema();
    if ( !format.equalsIgnoreCase("TIFF") )
      result = service.applyColorScaleToImage(format, result, cs, renderer, extrema);


    //ImageIO.write(RasterUtils.makeBufferedImage(result), "PNG", byteStream);

    //        ImageWorker worker = new ImageWorker(raster);
    //        worker.writePNG(byteStream, "", 0.8F, true, false);

    byteStream.close();

    if ( format.equalsIgnoreCase("JPG") || format.equalsIgnoreCase("JPEG") ) {
      ImageIO.write(RasterUtils.makeBufferedImage(result), "JPEG", byteStream);
    } else if ( format.equalsIgnoreCase("PNG") ) {
      ImageIO.write(RasterUtils.makeBufferedImage(result), "PNG", byteStream);
    } else if ( format.equalsIgnoreCase("TIFF") ) {
      ImageIO.write(RasterUtils.makeBufferedImage(result), "TIFF", byteStream);
    }
    IOUtils.closeQuietly(byteStream);


    if (GEN_BASELINE_DATA_ONLY)
    {
      byte[] bytes = byteStream.toByteArray();
      OutputStream outputStream = new FileOutputStream(new File(goldenPathStr));
      outputStream.write(bytes, 0, bytes.length);
      outputStream.close();
    }
    else
    {
      is = new ByteArrayInputStream(byteStream.toByteArray());
      final BufferedImage outputImg = readImageFromStream(is, reader);

      final BufferedImage goldenImg = ImageIO.read(new File(goldenPathStr));

      TestUtils.compareRenderedImages(goldenImg, outputImg);

      IOUtils.closeQuietly(is);
    }
    }

  private void testIslandsElevationFor(final String format, final String width,
    final String height, final String bbox,
    final String reqImgName, final String colorScale, final String goldenImgName,
    final String expectedContentType,
    final ImageReader reader) throws Exception
    {
    testIslandsElevationFor(format, width, height, bbox, reqImgName, colorScale, -1, goldenImgName,
      expectedContentType, reader);
    }

  private BufferedImage readImageFromStream(final InputStream stream,
    final ImageReader reader)
        throws IOException
        {
    final ImageInputStream imageInputStream = ImageIO.createImageInputStream(stream);
    reader.setInput(imageInputStream, false);

    Assert.assertEquals("Unexpected number of images in the stream", 1, reader.getNumImages(true));

    return reader.read(0);
        }
  private String getDefaultColorScale() throws JsonGenerationException, JsonMappingException,
  IOException
  {
    // create colorScale json
    final ObjectMapper mapper = new ObjectMapper();

    final Map<String, Object> colorScale = new HashMap<String, Object>();
    colorScale.put("Scaling", "MinMax");
    colorScale.put("ForceValuesIntoRange", "1");

    final Map<String, String> nullColor = new HashMap<String, String>();
    nullColor.put("color", "0,0,0");
    nullColor.put("opacity", "0");
    colorScale.put("NullColor", nullColor);
//    final Map<String, String> color1 = new HashMap<String, String>();
//    color1.put("value", "0.0");
//    color1.put("color", "255,0,0");
//    final Map<String, String> color2 = new HashMap<String, String>();
//    color2.put("value", "0.25");
//    color2.put("color", "255,255,0");
//    final Map<String, String> color3 = new HashMap<String, String>();
//    color3.put("value", "0.75");
//    color3.put("color", "0,255,255");
//    final Map<String, String> color4 = new HashMap<String, String>();
//    color4.put("value", "1.0");
//    color4.put("color", "255,255,255");
    final Map<String, String> color1 = new HashMap<String, String>();
    color1.put("value", "0.0");
    color1.put("color", "0,0,127");
    final Map<String, String> color2 = new HashMap<String, String>();
    color2.put("value", "0.2");
    color2.put("color", "0,0,255");
    final Map<String, String> color3 = new HashMap<String, String>();
    color3.put("value", "0.4");
    color3.put("color", "0,255,255");
    final Map<String, String> color4 = new HashMap<String, String>();
    color4.put("value", "0.6");
    color4.put("color", "0,255,0");
    final Map<String, String> color5 = new HashMap<String, String>();
    color5.put("value", "0.8");
    color5.put("color", "255,255,0");
    final Map<String, String> color6 = new HashMap<String, String>();
    color6.put("value", "1.0");
    color6.put("color", "255,0,0");

    final ArrayList<Map<String, String>> colors = new ArrayList<Map<String, String>>();
    colors.add(color1);
    colors.add(color2);
    colors.add(color3);
    colors.add(color4);
    colors.add(color5);
    colors.add(color6);

    colorScale.put("Colors", colors);

    return mapper.writeValueAsString(colorScale);
  }

  private String getAspectColorScale() throws JsonGenerationException, JsonMappingException,
  IOException
  {
    // create colorScale json
    final ObjectMapper mapper = new ObjectMapper();

    final Map<String, Object> colorScale = new HashMap<String, Object>();
    colorScale.put("Scaling", "MinMax");
    colorScale.put("ForceValuesIntoRange", "1");

    final Map<String, String> nullColor = new HashMap<String, String>();
    nullColor.put("color", "0,0,0");
    nullColor.put("opacity", "0");
    colorScale.put("NullColor", nullColor);
    final Map<String, String> color1 = new HashMap<String, String>();
    color1.put("value", "0.0");
    color1.put("color", "0,0,255");
    color1.put("opacity", "128");
    final Map<String, String> color2 = new HashMap<String, String>();
    color2.put("value", "0.26");
    color2.put("color", "255,255,0");
    color2.put("opacity", "128");
    final Map<String, String> color3 = new HashMap<String, String>();
    color3.put("value", "0.51");
    color3.put("color", "34,139,34");
    color3.put("opacity", "128");
    final Map<String, String> color4 = new HashMap<String, String>();
    color4.put("value", "0.76");
    color4.put("color", "255,0,0");
    color4.put("opacity", "128");
    final Map<String, String> color5 = new HashMap<String, String>();
    color5.put("value", "1.0");
    color5.put("color", "0,0,255");
    color5.put("opacity", "128");

    final ArrayList<Map<String, String>> colors = new ArrayList<Map<String, String>>();
    colors.add(color1);
    colors.add(color2);
    colors.add(color3);
    colors.add(color4);
    colors.add(color5);

    colorScale.put("Colors", colors);

    return mapper.writeValueAsString(colorScale);
  }
}
