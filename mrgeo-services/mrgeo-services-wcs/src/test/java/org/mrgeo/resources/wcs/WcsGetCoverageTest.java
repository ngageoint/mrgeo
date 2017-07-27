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

package org.mrgeo.resources.wcs;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.junit.IntegrationTest;
import org.mrgeo.test.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;

@SuppressWarnings("all") // Test code, not included in production
public class WcsGetCoverageTest extends WcsGeneratorTestAbstract
{
private static final Logger log = LoggerFactory.getLogger(WcsGetCoverageTest.class);

@BeforeClass
public static void setUpForJUnit()
{
  try
  {
    baselineInput = TestUtils.composeInputDir(WcsGetCoverageTest.class);
    WcsGeneratorTestAbstract.setUpForJUnit();
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
  String contentType = "image/geotiff";

  Response response = target("wcs")
      .queryParam("SERVICE", "WCS")
      .queryParam("VERSION", "1.1.0")
      .queryParam("REQUEST", "getcoverage")
      .queryParam("IDENTIFIER",
          "/mrgeo/test-files/org.mrgeo.resources.wcs/WcsGeneratorTestAbstract/IslandsElevation-v2")
      .queryParam("FORMAT", contentType)
      .queryParam("BOUNDINGBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE)
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

  Response response = target("wcs")
      .queryParam("SERVICE", "WCS")
      .queryParam("VERSION", "1.1.0")
      .queryParam("REQUEST", "getcoverage")
      .queryParam("IDENTIFIER", "IslandsElevation-v2")
      .queryParam("FORMAT", contentType)
      .queryParam("BOUNDINGBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE)
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
  Response response = target("wcs")
      .queryParam("SERVICE", "WCS")
      .queryParam("VERSION", "1.1.0")
      .queryParam("REQUEST", "getcoverage")
      .queryParam("IDENTIFIER", "IslandsElevation-v2")
      .queryParam("FORMAT", "image/geotif")
      .queryParam("BOUNDINGBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_MULTIPLE_SOURCE_TILES)
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
  Response response = target("wcs")
      .queryParam("SERVICE", "WCS")
      .queryParam("VERSION", "1.1.0")
      .queryParam("REQUEST", "getcoverage")
      .queryParam("IDENTIFIER", "IslandsElevation-v2")
      .queryParam("FORMAT", contentType)
      // pyramid only goes up to zoom level = 10; pass in zoom level = 11
      .queryParam("BOUNDINGBOX", ISLANDS_ELEVATION_V2_PAST_HIGHEST_RES_ZOOM_LEVEL)
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
  Response response = target("wcs")
      .queryParam("SERVICE", "WCS")
      .queryParam("VERSION", "1.1.0")
      .queryParam("REQUEST", "getcoverage")
      .queryParam("IDENTIFIER", "IslandsElevation-v2")
      .queryParam("FORMAT", contentType)
      .queryParam("BOUNDINGBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE)
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
  Response response = target("wcs")
      .queryParam("SERVICE", "WCS")
      .queryParam("VERSION", "1.1.0")
      .queryParam("REQUEST", "getcoverage")
      .queryParam("IDENTIFIER", "IslandsElevation-v2")
      .queryParam("FORMAT", "image/geotif")
      .queryParam("BOUNDINGBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE)
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

  Response response = target("wcs")
      .queryParam("SERVICE", "WCS")
      .queryParam("VERSION", "1.1.0")
      .queryParam("REQUEST", "getcoverage")
      .queryParam("IDENTIFIER", "IslandsElevation-v2")
      .queryParam("FORMAT", contentType)
      .queryParam("BOUNDINGBOX", "160.312500,-11.250000,161.718750,-9.843750")
      .queryParam("WIDTH", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
      .queryParam("HEIGHT", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
      .request().get();

  processXMLResponse(response, "testGetMapInvalidFormat.xml", Response.Status.BAD_REQUEST);
}

@Test
@Category(IntegrationTest.class)
public void testGetMapLowerCaseParams() throws Exception
{
  String contentType = "image/tiff";
  Response response = target("wcs")
      .queryParam("service", "WCS")
      .queryParam("version", "1.1.0")
      .queryParam("request", "getcoverage")
      .queryParam("identifier", "IslandsElevation-v2")
      .queryParam("format", contentType)
      .queryParam("boundingbox", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE)
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
  String contentType = "image/tiff";
  Response response = target("wcs")
      .queryParam("service", "WCS")
      .queryParam("version", "1.1.0")
      .queryParam("request", "getcoverage")
      .queryParam("identifier", "IslandsElevation-v2,IslandsElevation-v3")
      .queryParam("format", contentType)
      .queryParam("boundingbopx", "160.312500,-11.250000,161.718750,-9.843750")
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
public void testGetMapOutOfBoundsTif() throws Exception
{
  Response response = target("wcs")
      .queryParam("SERVICE", "WCS")
      .queryParam("VERSION", "1.1.0")
      .queryParam("REQUEST", "getcoverage")
      .queryParam("IDENTIFIER", "IslandsElevation-v2")
      .queryParam("FORMAT", "image/tif")
      .queryParam("BOUNDINGBOX", ISLANDS_ELEVATION_V2_OUT_OF_BOUNDS)
      .queryParam("WIDTH", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
      .queryParam("HEIGHT", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
      .request().get();

  processImageResponse(response, "image/tiff", "tif");
  response.close();
}


@Test
@Category(IntegrationTest.class)
public void testGetMapTifLargerThanTileSize() throws Exception
{
  String contentType = "image/tiff";

  Response response = target("wcs")
      .queryParam("SERVICE", "WCS")
      .queryParam("VERSION", "1.1.0")
      .queryParam("REQUEST", "getcoverage")
      .queryParam("IDENTIFIER", "IslandsElevation-v2")
      .queryParam("FORMAT", contentType)
      .queryParam("BOUNDINGBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE)
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
  Response response = target("wcs")
      .queryParam("SERVICE", "WCS")
      .queryParam("VERSION", "1.1.0")
      .queryParam("REQUEST", "getcoverage")
      .queryParam("IDENTIFIER", "IslandsElevation-v2")
      .queryParam("FORMAT", "image/tif")
      .queryParam("BOUNDINGBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_MULTIPLE_SOURCE_TILES)
      .queryParam("WIDTH", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
      .queryParam("HEIGHT", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
      .request().get();

  processImageResponse(response, "image/tiff", "tif");
  response.close();
}

@Test
@Category(IntegrationTest.class)
public void testGetMapTifNonExistingZoomLevelBelowWithPyramids() throws Exception
{
  String contentType = "image/tiff";

  Response response = target("wcs")
      .queryParam("SERVICE", "WCS")
      .queryParam("VERSION", "1.1.0")
      .queryParam("REQUEST", "getcoverage")
      .queryParam("IDENTIFIER", "IslandsElevation-v2")
      .queryParam("FORMAT", contentType)
      // pyramid only goes up to zoom level = 10; pass in zoom level = 11
      .queryParam("BOUNDINGBOX", ISLANDS_ELEVATION_V2_PAST_HIGHEST_RES_ZOOM_LEVEL)
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

  Response response = target("wcs")
      .queryParam("SERVICE", "WCS")
      .queryParam("VERSION", "1.1.0")
      .queryParam("REQUEST", "getcoverage")
      .queryParam("IDENTIFIER", "IslandsElevation-v2")
      .queryParam("FORMAT", contentType)
      .queryParam("BOUNDINGBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE)
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
  Response response = target("wcs")
      .queryParam("SERVICE", "WCS")
      .queryParam("VERSION", "1.1.0")
      .queryParam("REQUEST", "getcoverage")
      .queryParam("IDENTIFIER", "IslandsElevation-v2")
      .queryParam("FORMAT", "image/tif")
      .queryParam("BOUNDINGBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE)
      .queryParam("WIDTH", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
      .queryParam("HEIGHT", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
      .request().get();

  processImageResponse(response, "image/tiff", "tif");
  response.close();
}



}
