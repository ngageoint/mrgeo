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

package org.mrgeo.resources.wms;

import com.sun.jersey.api.client.ClientResponse;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.junit.IntegrationTest;
import org.mrgeo.test.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("static-method")
public class MissingStatsTest extends WmsGeneratorTestAbstract
{
  @SuppressWarnings("unused")
  private static final Logger log = LoggerFactory.getLogger(MissingStatsTest.class);

  @BeforeClass
  public static void setUpForJUnit()
  {
    try
    {
      baselineInput = TestUtils.composeInputDir(MissingStatsTest.class);
      WmsGeneratorTestAbstract.setUpForJUnit();
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }

  /*
   * If no stats have been calculated on an image, a default range of 0.0 to 1.0 is used for the
   * extrema during color scale application.
   */

  @Test
  @Category(IntegrationTest.class)
  public void testGetMapPngNoStats() throws Exception
  {
    String contentType = "image/png";
    ClientResponse response = resource().path("/wms")
        .queryParam("SERVICE", "WMS")
        .queryParam("REQUEST", "getmap")
        .queryParam("LAYERS", "IslandsElevation-v2-no-stats")
        .queryParam("FORMAT", contentType)
        .queryParam("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE)
        .queryParam("WIDTH", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
        .queryParam("HEIGHT", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
        .get(ClientResponse.class);

    processImageResponse(response, contentType, "png");
  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMapJpgNoStats() throws Exception
  {
    try
    {
      String contentType = "image/jpeg";
      ClientResponse response = resource().path("/wms")
          .queryParam("SERVICE", "WMS")
          .queryParam("REQUEST", "getmap")
          .queryParam("LAYERS", "IslandsElevation-v2-no-stats")
          .queryParam("FORMAT", contentType)
          .queryParam("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE)
          .queryParam("WIDTH", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
          .queryParam("HEIGHT", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
          .get(ClientResponse.class);

      processImageResponse(response, contentType, "jpg");
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMapTifNoStats() throws Exception
  {
    String contentType = "image/tiff";
    ClientResponse response = resource().path("/wms")
        .queryParam("SERVICE", "WMS")
        .queryParam("REQUEST", "getmap")
        .queryParam("LAYERS", "IslandsElevation-v2-no-stats")
        .queryParam("FORMAT", contentType)
        .queryParam("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE)
        .queryParam("WIDTH", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
        .queryParam("HEIGHT", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
        .get(ClientResponse.class);

    processImageResponse(response, contentType, "tif");
  }

  /*
   * If no stats have been calculated on an image, a default range of 0.0 to 1.0 is used for the
   * extrema during color scale application.
   */

  @Test
  @Category(IntegrationTest.class)
  public void testGetMosaicPngNoStats() throws Exception
  {
    String contentType = "image/png";
    ClientResponse response = resource().path("/wms")
        .queryParam("SERVICE", "WMS")
        .queryParam("REQUEST", "getmosaic")
        .queryParam("LAYERS", "IslandsElevation-v2-no-stats")
        .queryParam("FORMAT", contentType)
        .queryParam("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE)
        .get(ClientResponse.class);

    processImageResponse(response, contentType, "png");
  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMosaicJpgNoStats() throws Exception
  {
    String contentType = "image/jpeg";
    ClientResponse response = resource().path("/wms")
        .queryParam("SERVICE", "WMS")
        .queryParam("REQUEST", "getmosaic")
        .queryParam("LAYERS", "IslandsElevation-v2-no-stats")
        .queryParam("FORMAT", contentType)
        .queryParam("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE)
        .get(ClientResponse.class);

    processImageResponse(response, contentType, "jpg");
  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMosaicTifNoStats() throws Exception
  {
    String contentType = "image/tiff";
    ClientResponse response = resource().path("/wms")
        .queryParam("SERVICE", "WMS")
        .queryParam("REQUEST", "getmosaic")
        .queryParam("LAYERS", "IslandsElevation-v2-no-stats")
        .queryParam("FORMAT", contentType)
        .queryParam("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE)
        .get(ClientResponse.class);

    processImageResponse(response, contentType, "tif");
  }

  /*
   * If no stats have been calculated on an image, a default range of 0.0 to 1.0 is used for the
   * extrema during color scale application.
   */

  @Test
  @Category(IntegrationTest.class)
  public void testGetTilePngNoStats() throws Exception
  {
    String contentType = "image/png";
    ClientResponse response = resource().path("/wms")
        .queryParam("SERVICE", "WMS")
        .queryParam("REQUEST", "gettile")
        .queryParam("LAYER", "IslandsElevation-v2-no-stats")
        .queryParam("FORMAT", contentType)
        .queryParam("TILEROW", "56")
        .queryParam("TILECOL", "242")
        .queryParam("SCALE", "0.0027465820") // zoom level 8
        .get(ClientResponse.class);

    processImageResponse(response, contentType, "png");
  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetTileJpgNoStats() throws Exception
  {
    String contentType = "image/jpeg";
    ClientResponse response = resource().path("/wms")
        .queryParam("SERVICE", "WMS")
        .queryParam("REQUEST", "gettile")
        .queryParam("LAYER", "IslandsElevation-v2-no-stats")
        .queryParam("FORMAT", contentType)
        .queryParam("TILEROW", "56")
        .queryParam("TILECOL", "242")
        .queryParam("SCALE", "0.0027465820") // zoom level 8
        .get(ClientResponse.class);

    processImageResponse(response, contentType, "jpg");
  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetTileTifNoStats() throws Exception
  {
    String contentType = "image/tiff";
    ClientResponse response = resource().path("/wms")
        .queryParam("SERVICE", "WMS")
        .queryParam("REQUEST", "gettile")
        .queryParam("LAYER", "IslandsElevation-v2-no-stats")
        .queryParam("FORMAT", contentType)
        .queryParam("TILEROW", "56")
        .queryParam("TILECOL", "242")
        .queryParam("SCALE", "0.0027465820") // zoom level 8
        .get(ClientResponse.class);

    processImageResponse(response, contentType, "tif");
  }
}
