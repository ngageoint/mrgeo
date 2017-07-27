/*
 * Copyright 2009-2016 DigitalGlobe, Inc.
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
 *
 */

package org.mrgeo.resources.wms;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.image.MrsPyramidMetadata;
import org.mrgeo.junit.IntegrationTest;
import org.mrgeo.test.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;

@SuppressWarnings("all") // Test code, not included in production
public class GetMapTest extends WmsGeneratorTestAbstract
{
private static final Logger log = LoggerFactory.getLogger(GetMapTest.class);

@BeforeClass
public static void setUpForJUnit()
{
  try
  {
    baselineInput = TestUtils.composeInputDir(GetMapTest.class);
    WmsGeneratorTestAbstract.setUpForJUnit();
  }
  catch (final Exception e)
  {
    e.printStackTrace();
  }
}

@AfterClass
public static void teardown()
{
  log.debug("done");
}

@Test
@Category(IntegrationTest.class)
public void testGetMapFullLayerPath() throws Exception
{
  String contentType = "image/png";

  Response response = target("wms")
      .queryParam("SERVICE", "WMS")
      .queryParam("REQUEST", "getmap")
      .queryParam("LAYERS",
          "/mrgeo/test-files/org.mrgeo.resources.wms/WmsGeneratorTestAbstract/IslandsElevation-v2")
      .queryParam("FORMAT", contentType)
      .queryParam("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE)
      .queryParam("WIDTH", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
      .queryParam("HEIGHT", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
      .request().get();

  processImageResponse(response, contentType, "png");
  response.close();
}

@Test
@Category(IntegrationTest.class)
public void testGetMapGeoTifLargerThanTileSize() throws Exception
{
  String contentType = "image/geotiff";

  Response response = target("wms")
      .queryParam("SERVICE", "WMS")
      .queryParam("REQUEST", "getmap")
      .queryParam("LAYERS", "IslandsElevation-v2")
      .queryParam("FORMAT", contentType)
      .queryParam("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE)
      .queryParam("WIDTH", "1024")
      .queryParam("HEIGHT", "1024")
      .request().get();

  processImageResponse(response, contentType, "tif");
  response.close();
}

@Test
@Category(IntegrationTest.class)
public void testGetMapGeoTifMultipleSourceTiles() throws Exception
{
  Response response = target("wms")
      .queryParam("SERVICE", "WMS")
      .queryParam("REQUEST", "getmap")
      .queryParam("LAYERS", "IslandsElevation-v2")
      .queryParam("FORMAT", "image/geotif")
      .queryParam("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_MULTIPLE_SOURCE_TILES)
      .queryParam("WIDTH", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
      .queryParam("HEIGHT", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
      .request().get();

  processImageResponse(response, "image/geotiff", "tif");
  response.close();
}

@Test
@Category(IntegrationTest.class)
public void testGetMapGeoTifNonExistingZoomLevelBelowWithPyramids() throws Exception
{
  String contentType = "image/geotiff";
  Response response = target("wms")
      .queryParam("SERVICE", "WMS")
      .queryParam("REQUEST", "getmap")
      .queryParam("LAYERS", "IslandsElevation-v2")
      .queryParam("FORMAT", contentType)
      // pyramid only goes up to zoom level = 10; pass in zoom level = 11
      .queryParam("BBOX", ISLANDS_ELEVATION_V2_PAST_HIGHEST_RES_ZOOM_LEVEL)
      .queryParam("WIDTH", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
      .queryParam("HEIGHT", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
      .request().get();

  processImageResponse(response, contentType, "tif");
  response.close();
}

@Test
@Category(IntegrationTest.class)
public void testGetMapGeoTifRectangularTileSize() throws Exception
{
  String contentType = "image/geotiff";
  Response response = target("wms")
      .queryParam("SERVICE", "WMS")
      .queryParam("REQUEST", "getmap")
      .queryParam("LAYERS", "IslandsElevation-v2")
      .queryParam("FORMAT", contentType)
      .queryParam("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE)
      .queryParam("WIDTH", "700")
      .queryParam("HEIGHT", "300")
      .request().get();

  processImageResponse(response, contentType, "tif", true);
  response.close();
}

@Test
@Category(IntegrationTest.class)
public void testGetMapGeoTifSingleSourceTile() throws Exception
{
  Response response = target("wms")
      .queryParam("SERVICE", "WMS")
      .queryParam("REQUEST", "getmap")
      .queryParam("LAYERS", "IslandsElevation-v2")
      .queryParam("FORMAT", "image/geotif")
      .queryParam("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE)
      .queryParam("WIDTH", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
      .queryParam("HEIGHT", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
      .request().get();

  processImageResponse(response, "image/geotiff", "tif");
  response.close();
}

@Test
@Category(IntegrationTest.class)
public void testGetMapInvalidFormat() throws Exception
{
  String contentType = "image/abc";

  Response response = target("wms")
      .queryParam("SERVICE", "WMS")
      .queryParam("REQUEST", "getmap")
      .queryParam("LAYERS", "IslandsElevation-v2")
      .queryParam("FORMAT", contentType)
      .queryParam("BBOX", "160.312500,-11.250000,161.718750,-9.843750")
      .queryParam("WIDTH", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
      .queryParam("HEIGHT", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
      .request().get();

  processXMLResponse(response, "testGetMapInvalidFormat.xml", Response.Status.BAD_REQUEST);
}

@Test
@Category(IntegrationTest.class)
public void testGetMapInvalidLayer() throws Exception
{
  String contentType = "image/png";

  Response response = target("wms")
      .queryParam("SERVICE", "WMS")
      .queryParam("REQUEST", "getmap")
      .queryParam("LAYERS", "IslandsElevation-v3")
      .queryParam("FORMAT", contentType)
      .queryParam("BBOX", "160.312500,-11.250000,161.718750,-9.843750")
      .queryParam("WIDTH", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
      .queryParam("HEIGHT", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
      .request().get();

  processXMLResponse(response, "testGetMapInvalidLayer.xml", Response.Status.BAD_REQUEST);
}

@Test
@Category(IntegrationTest.class)
public void testGetMapJpgLargerThanTileSize() throws Exception
{
  String contentType = "image/jpeg";

  Response response = target("wms")
      .queryParam("SERVICE", "WMS")
      .queryParam("REQUEST", "getmap")
      .queryParam("LAYERS", "IslandsElevation-v2")
      .queryParam("FORMAT", contentType)
      .queryParam("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE)
      .queryParam("WIDTH", "1024")
      .queryParam("HEIGHT", "1024")
      .request().get();

  processImageResponse(response, contentType, "jpg");
  response.close();
}

@Test
@Category(IntegrationTest.class)
public void testGetMapJpgMultipleSourceTiles() throws Exception
{
  Response response = target("wms")
      .queryParam("SERVICE", "WMS")
      .queryParam("REQUEST", "getmap")
      .queryParam("LAYERS", "IslandsElevation-v2")
      .queryParam("FORMAT", "image/jpg")
      .queryParam("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_MULTIPLE_SOURCE_TILES)
      .queryParam("WIDTH", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
      .queryParam("HEIGHT", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
      .request().get();

  processImageResponse(response, "image/jpeg", "jpg");
  response.close();
}

@Test
@Category(IntegrationTest.class)
public void testGetMapJpgNonExistingZoomLevelBelowWithPyramids() throws Exception
{
  String contentType = "image/jpeg";

  Response response = target("wms")
      .queryParam("SERVICE", "WMS")
      .queryParam("REQUEST", "getmap")
      .queryParam("LAYERS", "IslandsElevation-v2")
      .queryParam("FORMAT", contentType)
      // pyramid only goes up to zoom level = 10; pass in zoom level = 11
      .queryParam("BBOX", ISLANDS_ELEVATION_V2_PAST_HIGHEST_RES_ZOOM_LEVEL)
      .queryParam("WIDTH", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
      .queryParam("HEIGHT", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
      .request().get();

  processImageResponse(response, contentType, "jpg");
  response.close();
}

@Test
@Category(IntegrationTest.class)
public void testGetMapJpgRectangularTileSize() throws Exception
{
  String contentType = "image/jpeg";

  Response response = target("wms")
      .queryParam("SERVICE", "WMS")
      .queryParam("REQUEST", "getmap")
      .queryParam("LAYERS", "IslandsElevation-v2")
      .queryParam("FORMAT", contentType)
      .queryParam("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE)
      .queryParam("WIDTH", "700")
      .queryParam("HEIGHT", "300")
      .request().get();

  processImageResponse(response, contentType, "jpg", true);
  response.close();
}

@Test
@Category(IntegrationTest.class)
public void testGetMapJpgSingleSourceTile() throws Exception
{
  Response response = target("wms")
      .queryParam("SERVICE", "WMS")
      .queryParam("REQUEST", "getmap")
      .queryParam("LAYERS", "IslandsElevation-v2")
      .queryParam("FORMAT", "image/jpg")
      .queryParam("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE)
      .queryParam("WIDTH", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
      .queryParam("HEIGHT", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
      .request().get();

  processImageResponse(response, "image/jpeg", "jpg");
  response.close();
}

@Test
@Category(IntegrationTest.class)
public void testGetMapLowerCaseParams() throws Exception
{
  String contentType = "image/png";
  Response response = target("wms")
      .queryParam("SERVICE", "WMS")
      .queryParam("request", "getmap")
      .queryParam("layers", "IslandsElevation-v2")
      .queryParam("format", contentType)
      .queryParam("bbox", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE)
      .queryParam("width", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
      .queryParam("height", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
      .request().get();

  processImageResponse(response, contentType, "png");
  response.close();
}

  /*
   * WmsGenerator doesn't support more than one layer per request.
   */

@Test
@Category(IntegrationTest.class)
public void testGetMapMultipleRequestLayers() throws Exception
{
  String contentType = "image/png";
  Response response = target("wms")
      .queryParam("SERVICE", "WMS")
      .queryParam("request", "getmap")
      .queryParam("LAYERS", "IslandsElevation-v2,IslandsElevation-v3")
      .queryParam("format", contentType)
      .queryParam("bbox", "160.312500,-11.250000,161.718750,-9.843750")
      .queryParam("width", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
      .queryParam("height", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
      .request().get();

  processXMLResponse(response, "testGetMapMultipleRequestLayers.xml", Response.Status.BAD_REQUEST);
}

  /*
   * JPG doesn't support transparency, so a black image is returned for out of bounds requests.
   */

@Test
@Category(IntegrationTest.class)
public void testGetMapOutOfBoundsJpg() throws Exception
{
  Response response = target("wms")
      .queryParam("SERVICE", "WMS")
      .queryParam("REQUEST", "getmap")
      .queryParam("LAYERS", "IslandsElevation-v2")
      .queryParam("FORMAT", "image/jpg")
      .queryParam("BBOX", ISLANDS_ELEVATION_V2_OUT_OF_BOUNDS)
      .queryParam("WIDTH", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
      .queryParam("HEIGHT", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
      .request().get();

  processImageResponse(response, "image/jpeg", "jpg");
  response.close();
}

  /*
   * PNG out of bounds requests should return a transparent image.
   */

@Test
@Category(IntegrationTest.class)
public void testGetMapOutOfBoundsPng() throws Exception
{
  String contentType = "image/png";
  Response response = target("wms")
      .queryParam("SERVICE", "WMS")
      .queryParam("REQUEST", "getmap")
      .queryParam("LAYERS", "IslandsElevation-v2")
      .queryParam("FORMAT", contentType)
      .queryParam("BBOX", ISLANDS_ELEVATION_V2_OUT_OF_BOUNDS)
      .queryParam("WIDTH", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
      .queryParam("HEIGHT", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
      .request().get();

  processImageResponse(response, contentType, "png");
  response.close();
}

@Test
@Category(IntegrationTest.class)
public void testGetMapOutOfBoundsTif() throws Exception
{
  Response response = target("wms")
      .queryParam("SERVICE", "WMS")
      .queryParam("REQUEST", "getmap")
      .queryParam("LAYERS", "IslandsElevation-v2")
      .queryParam("FORMAT", "image/tif")
      .queryParam("BBOX", ISLANDS_ELEVATION_V2_OUT_OF_BOUNDS)
      .queryParam("WIDTH", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
      .queryParam("HEIGHT", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
      .request().get();

  processImageResponse(response, "image/tiff", "tif");
  response.close();
}

@Test
@Category(IntegrationTest.class)
public void testGetMapPngLargerThanTileSize() throws Exception
{
  String contentType = "image/png";
  Response response = target("wms")
      .queryParam("SERVICE", "WMS")
      .queryParam("REQUEST", "getmap")
      .queryParam("LAYERS", "IslandsElevation-v2")
      .queryParam("FORMAT", contentType)
      .queryParam("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE)
      .queryParam("WIDTH", "1024")
      .queryParam("HEIGHT", "1024")
      .request().get();

  processImageResponse(response, contentType, "png");
  response.close();
}

@Test
@Category(IntegrationTest.class)
public void testGetMapPngMultipleSourceTiles() throws Exception
{
  String contentType = "image/png";

  Response response = target("wms")
      .queryParam("SERVICE", "WMS")
      .queryParam("REQUEST", "getmap")
      .queryParam("LAYERS", "IslandsElevation-v2")
      .queryParam("FORMAT", contentType)
      .queryParam("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_MULTIPLE_SOURCE_TILES)
      .queryParam("WIDTH", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
      .queryParam("HEIGHT", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
      .request().get();

  processImageResponse(response, contentType, "png");
  response.close();
}

  /*
   * These should return the highest res available image resampled to the requested bounds.
   */

@Test
@Category(IntegrationTest.class)
public void testGetMapPngNonExistingZoomLevelBelowWithPyramids() throws Exception
{
  String contentType = "image/png";

  Response response = target("wms")
      .queryParam("SERVICE", "WMS")
      .queryParam("REQUEST", "getmap")
      .queryParam("LAYERS", "IslandsElevation-v2")
      .queryParam("FORMAT", contentType)
      // pyramid only goes up to zoom level = 10; pass in zoom level = 11
      .queryParam("BBOX", ISLANDS_ELEVATION_V2_PAST_HIGHEST_RES_ZOOM_LEVEL)
      .queryParam("WIDTH", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
      .queryParam("HEIGHT", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
      .request().get();

  processImageResponse(response, contentType, "png");
  response.close();
}

@Test
@Category(IntegrationTest.class)
public void testGetMapPngRectangularTileSize() throws Exception
{
  String contentType = "image/png";

  Response response = target("wms")
      .queryParam("SERVICE", "WMS")
      .queryParam("REQUEST", "getmap")
      .queryParam("LAYERS", "IslandsElevation-v2")
      .queryParam("FORMAT", contentType)
      .queryParam("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE)
      .queryParam("WIDTH", "700")
      .queryParam("HEIGHT", "300")
      .request().get();

  processImageResponse(response, contentType, "png", true);
  response.close();
}

@Test
@Category(IntegrationTest.class)
public void testGetMapPngSingleSourceTile() throws Exception
{
  String contentType = "image/png";

  Response response = target("wms")
      .queryParam("SERVICE", "WMS")
      .queryParam("REQUEST", "getmap")
      .queryParam("LAYERS", "IslandsElevation-v2")
      .queryParam("FORMAT", contentType)
      .queryParam("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE)
      .queryParam("WIDTH", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
      .queryParam("HEIGHT", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
      .request().get();

  processImageResponse(response, contentType, "png");
  response.close();
}

@Test
@Category(IntegrationTest.class)
public void testGetMapTifLargerThanTileSize() throws Exception
{
  String contentType = "image/tiff";

  Response response = target("wms")
      .queryParam("SERVICE", "WMS")
      .queryParam("REQUEST", "getmap")
      .queryParam("LAYERS", "IslandsElevation-v2")
      .queryParam("FORMAT", contentType)
      .queryParam("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE)
      .queryParam("WIDTH", "1024")
      .queryParam("HEIGHT", "1024")
      .request().get();

  processImageResponse(response, contentType, "tif");
  response.close();
}

@Test
@Category(IntegrationTest.class)
public void testGetMapTifMultipleSourceTiles() throws Exception
{
  Response response = target("wms")
      .queryParam("SERVICE", "WMS")
      .queryParam("REQUEST", "getmap")
      .queryParam("LAYERS", "IslandsElevation-v2")
      .queryParam("FORMAT", "image/tif")
      .queryParam("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_MULTIPLE_SOURCE_TILES)
      .queryParam("WIDTH", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
      .queryParam("HEIGHT", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
      .request().get();

  processImageResponse(response, "image/tiff", "tif");
  response.close();
}

  /*
   * These should return the highest res available image resampled to the requested bounds.
   */

@Test
@Category(IntegrationTest.class)
public void testGetMapTifNonExistingZoomLevelBelowWithPyramids() throws Exception
{
  String contentType = "image/tiff";

  Response response = target("wms")
      .queryParam("SERVICE", "WMS")
      .queryParam("REQUEST", "getmap")
      .queryParam("LAYERS", "IslandsElevation-v2")
      .queryParam("FORMAT", contentType)
      // pyramid only goes up to zoom level = 10; pass in zoom level = 11
      .queryParam("BBOX", ISLANDS_ELEVATION_V2_PAST_HIGHEST_RES_ZOOM_LEVEL)
      .queryParam("WIDTH", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
      .queryParam("HEIGHT", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
      .request().get();

  processImageResponse(response, contentType, "tif");
  response.close();
}

@Test
@Category(IntegrationTest.class)
public void testGetMapTifRectangularTileSize() throws Exception
{
  String contentType = "image/tiff";

  Response response = target("wms")
      .queryParam("SERVICE", "WMS")
      .queryParam("REQUEST", "getmap")
      .queryParam("LAYERS", "IslandsElevation-v2")
      .queryParam("FORMAT", contentType)
      .queryParam("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE)
      .queryParam("WIDTH", "700")
      .queryParam("HEIGHT", "300")
      .request().get();

  processImageResponse(response, contentType, "tif", true);
  response.close();
}

@Test
@Category(IntegrationTest.class)
public void testGetMapTifSingleSourceTile() throws Exception
{
  Response response = target("wms")
      .queryParam("SERVICE", "WMS")
      .queryParam("REQUEST", "getmap")
      .queryParam("LAYERS", "IslandsElevation-v2")
      .queryParam("FORMAT", "image/tif")
      .queryParam("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE)
      .queryParam("WIDTH", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
      .queryParam("HEIGHT", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
      .request().get();

  processImageResponse(response, "image/tiff", "tif");
  response.close();
}

// See image stretch test notes in RasterResourceTest::testImageStretch

@Test
@Category(IntegrationTest.class)
public void testImageStretch() throws Exception
{
  String contentType = "image/png";

  Response response = target("wms")
      .queryParam("SERVICE", "WMS")
      .queryParam("REQUEST", "getmap")
      .queryParam("LAYERS", imageStretchUnqualified)
      .queryParam("FORMAT", contentType)
      .queryParam("BBOX", "69.14932562595378,34.85619123437472,69.37012237404623,35.038450765625285")
      .queryParam("WIDTH", "800")
      .queryParam("HEIGHT", "600")
      .request().get();

  processImageResponse(response, contentType, "png", true);
  response.close();
}

@Test
@Category(IntegrationTest.class)
public void testImageStretch2() throws Exception
{
  String contentType = "image/png";

  Response response = target("wms")
      .queryParam("SERVICE", "WMS")
      .queryParam("REQUEST", "getmap")
      .queryParam("LAYERS", imageStretch2Unqualified)
      .queryParam("FORMAT", contentType)
      .queryParam("BBOX", "-112.53799295569956,35.768445274925874,-111.64052704430043,36.49839472507413")
      .queryParam("WIDTH", "900")
      .queryParam("HEIGHT", "700")
      .request().get();

  processImageResponse(response, contentType, "png", true);
  response.close();
}

@Test
@Category(IntegrationTest.class)
public void testStylePng() throws Exception
{

  String contentType = "image/png";

  Response response = target("wms")
      .queryParam("SERVICE", "WMS")
      .queryParam("REQUEST", "getmap")
      .queryParam("LAYERS", "IslandsElevation-v2")
      .queryParam("FORMAT", contentType)
      .queryParam("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE)
      .queryParam("WIDTH", "900")
      .queryParam("HEIGHT", "700")
      .queryParam("STYLES", "elevation")

      .request().get();

  processImageResponse(response, contentType, "png", true);
  response.close();
}

@Test
@Category(IntegrationTest.class)
public void testStyleJpg() throws Exception
{

  String contentType = "image/jpeg";

  Response response = target("wms")
      .queryParam("SERVICE", "WMS")
      .queryParam("REQUEST", "getmap")
      .queryParam("LAYERS", "IslandsElevation-v2")
      .queryParam("FORMAT", contentType)
      .queryParam("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE)
      .queryParam("WIDTH", "900")
      .queryParam("HEIGHT", "700")
      .queryParam("STYLES", "elevation")

      .request().get();

  processImageResponse(response, contentType, "jpg", true);
  response.close();
}
@Test
@Category(IntegrationTest.class)
public void testStyleTiff() throws Exception
{

  String contentType = "image/geotiff";

  Response response = target("wms")
      .queryParam("SERVICE", "WMS")
      .queryParam("REQUEST", "getmap")
      .queryParam("LAYERS", "IslandsElevation-v2")
      .queryParam("FORMAT", contentType)
      .queryParam("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE)
      .queryParam("WIDTH", "900")
      .queryParam("HEIGHT", "700")
      .queryParam("STYLES", "elevation")

      .request().get();

  processImageResponse(response, contentType, "tif", true);
  response.close();
}

@Test
@Category(IntegrationTest.class)
public void testDefaultColorscale() throws Exception
{

  MrsImageDataProvider dp = DataProviderFactory.getMrsImageDataProvider("IslandsElevation-v2",
      DataProviderFactory.AccessMode.READ, new ProviderProperties());
  MrsPyramidMetadata meta = dp.getMetadataReader().read();

  meta.setTag(MrGeoConstants.MRGEO_DEFAULT_COLORSCALE, "elevation");

  String contentType = "image/png";

  Response response = target("wms")
      .queryParam("SERVICE", "WMS")
      .queryParam("REQUEST", "getmap")
      .queryParam("LAYERS", "IslandsElevation-v2")
      .queryParam("FORMAT", contentType)
      .queryParam("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE)
      .queryParam("WIDTH", "900")
      .queryParam("HEIGHT", "700")
      .request().get();

  processImageResponse(response, contentType, "png", true);
  response.close();

  meta.setTag(MrGeoConstants.MRGEO_DEFAULT_COLORSCALE, "");
  dp.getMetadataReader().reload();
}

@Test
@Category(IntegrationTest.class)
public void testJpg3band() throws Exception
{
  Response response = target("wms")
      .queryParam("SERVICE", "WMS")
      .queryParam("REQUEST", "getmap")
      .queryParam("LAYERS", small3bandUnqualified)
      .queryParam("FORMAT", "image/jpg")
      .queryParam("BBOX", "8.200266813859766,54.86003345745267,8.482915124747016,55.0210075283041")
      .queryParam("WIDTH", "800")
      .queryParam("HEIGHT", "600")
      .request().get();

  processImageResponse(response, "image/jpeg", "jpg", true);
  response.close();
}

@Test
@Category(IntegrationTest.class)
public void testPng3band() throws Exception
{
  String contentType = "image/png";

  Response response = target("wms")
      .queryParam("SERVICE", "WMS")
      .queryParam("REQUEST", "getmap")
      .queryParam("LAYERS", small3bandUnqualified)
      .queryParam("FORMAT", contentType)
      .queryParam("BBOX", "8.200266813859766,54.86003345745267,8.482915124747016,55.0210075283041")
      .queryParam("WIDTH", "800")
      .queryParam("HEIGHT", "600")
      .request().get();

  processImageResponse(response, contentType, "png", true);
  response.close();
}

@Test
@Category(IntegrationTest.class)
public void testTif3band() throws Exception
{
  String contentType = "image/tiff";

  Response response = target("wms")
      .queryParam("SERVICE", "WMS")
      .queryParam("REQUEST", "getmap")
      .queryParam("LAYERS", small3bandUnqualified)
      .queryParam("FORMAT", contentType)
      .queryParam("BBOX", "8.200266813859766,54.86003345745267,8.482915124747016,55.0210075283041")
      .queryParam("WIDTH", "800")
      .queryParam("HEIGHT", "600")
      .request().get();

  processImageResponse(response, contentType, "tiff", true);
  response.close();
}

}
