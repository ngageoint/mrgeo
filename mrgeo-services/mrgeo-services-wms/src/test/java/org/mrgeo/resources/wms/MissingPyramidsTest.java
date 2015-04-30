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
import org.mrgeo.junit.IntegrationTest;
import org.mrgeo.test.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("static-method")
public class MissingPyramidsTest extends WmsGeneratorTestAbstract
{
  @SuppressWarnings("unused")
  private static final Logger log =
    LoggerFactory.getLogger(MissingPyramidsTest.class);

  @BeforeClass
  public static void setUpForJUnit()
  {
    try
    {
      baselineInput = TestUtils.composeInputDir(MissingPyramidsTest.class);
      WmsGeneratorTestAbstract.setUpForJUnit();
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }

  /*
   * These should return an empty image, since we don't support missing zoom levels above what
   * exists in MrsImagePyramid yet.
   * @todo Once the WMS can handle this case for non-pyramid data, update the baseline image for
   * this test.
   */

  @Test
  @Category(IntegrationTest.class)
  public void testGetMapPngNonExistingZoomLevelAboveWithoutPyramids() throws Exception
  {
    String contentType = "image/png";
    ClientResponse response = resource().path("/wms")
            .queryParam("SERVICE", "WMS")
            .queryParam("REQUEST", "getmap")
            .queryParam("LAYERS", "IslandsElevation-v2-no-pyramid")
            .queryParam("FORMAT", contentType)
            //pyramid only has a single zoom level = 10; pass in zoom level = 8
            .queryParam("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE)
            .queryParam("WIDTH", "512")
            .queryParam("HEIGHT", "512")
            .get(ClientResponse.class);

    processImageResponse(response, contentType, "png");
  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMapJpgNonExistingZoomLevelAboveWithoutPyramids() throws Exception
  {
    String contentType = "image/jpeg";
    ClientResponse response = resource().path("/wms")
            .queryParam("SERVICE", "WMS")
            .queryParam("REQUEST", "getmap")
            .queryParam("LAYERS", "IslandsElevation-v2-no-pyramid")
            .queryParam("FORMAT", contentType)
            //pyramid only has a single zoom level = 10; pass in zoom level = 8
            .queryParam("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE)
            .queryParam("WIDTH", "512")
            .queryParam("HEIGHT", "512")
            .get(ClientResponse.class);

    processImageResponse(response, contentType, "jpg");
  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMapTifNonExistingZoomLevelAboveWithoutPyramids() throws Exception
  {
    String contentType = "image/tiff";
    ClientResponse response = resource().path("/wms")
            .queryParam("SERVICE", "WMS")
            .queryParam("REQUEST", "getmap")
            .queryParam("LAYERS", "IslandsElevation-v2-no-pyramid")
            .queryParam("FORMAT", contentType)
            //pyramid only has a single zoom level = 10; pass in zoom level = 8
            .queryParam("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE)
            .queryParam("WIDTH", "512")
            .queryParam("HEIGHT", "512")
            .get(ClientResponse.class);

    processImageResponse(response, contentType, "tif");
  }

  /*
   * These should return an empty image, since we don't support missing zoom levels above what
   * exists in MrsImagePyramid yet.  "Extra metadata" implies that the levels described in the file don't
   * reflect what raster images actually exist in the pyramid.
   */

  @Test
  @Category(IntegrationTest.class)
  public void testGetMapPngNonExistingZoomLevelAboveWithoutPyramidsExtraMetadata() throws Exception
  {
    String contentType = "image/png";
    ClientResponse response = resource().path("/wms")
            .queryParam("SERVICE", "WMS")
            .queryParam("REQUEST", "getmap")
            .queryParam("LAYERS", "IslandsElevation-v2-no-pyramid-extra-metadata")
            .queryParam("FORMAT", contentType)
            //pyramid only has a single zoom level = 10; pass in zoom level = 8
            .queryParam("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE)
            .queryParam("WIDTH", "512")
            .queryParam("HEIGHT", "512")
            .get(ClientResponse.class);

    processImageResponse(response, contentType, "png");
  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMapJpgNonExistingZoomLevelAboveWithoutPyramidsExtraMetadata() throws Exception
  {
    String contentType = "image/jpeg";
    ClientResponse response = resource().path("/wms")
            .queryParam("SERVICE", "WMS")
            .queryParam("REQUEST", "getmap")
            .queryParam("LAYERS", "IslandsElevation-v2-no-pyramid-extra-metadata")
            .queryParam("FORMAT", contentType)
            //pyramid only has a single zoom level = 10; pass in zoom level = 8
            .queryParam("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE)
            .queryParam("WIDTH", "512")
            .queryParam("HEIGHT", "512")
            .get(ClientResponse.class);

    processImageResponse(response, contentType, "jpg");
  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMapTifNonExistingZoomLevelAboveWithoutPyramidsExtraMetadata() throws Exception
  {
    String contentType = "image/tiff";
    ClientResponse response = resource().path("/wms")
            .queryParam("SERVICE", "WMS")
            .queryParam("REQUEST", "getmap")
            .queryParam("LAYERS", "IslandsElevation-v2-no-pyramid-extra-metadata")
            .queryParam("FORMAT", contentType)
            //pyramid only has a single zoom level = 10; pass in zoom level = 8
            .queryParam("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE)
            .queryParam("WIDTH", "512")
            .queryParam("HEIGHT", "512")
            .get(ClientResponse.class);

    processImageResponse(response, contentType, "tif");
  }

  /*
   * These should return the highest res available image resampled to the requested bounds.
   */

  @Test
  @Category(IntegrationTest.class)
  public void testGetMapPngNonExistingZoomLevelBelowWithoutPyramids() throws Exception
  {
    String contentType = "image/png";
    ClientResponse response = resource().path("/wms")
            .queryParam("SERVICE", "WMS")
            .queryParam("REQUEST", "getmap")
            .queryParam("LAYERS", "IslandsElevation-v2-no-pyramid")
            .queryParam("FORMAT", contentType)
            //pyramid only has a single zoom level = 10; pass in zoom level = 11
            .queryParam("BBOX", ISLANDS_ELEVATION_V2_PAST_HIGHEST_RES_ZOOM_LEVEL)
            .queryParam("WIDTH", "512")
            .queryParam("HEIGHT", "512")
            .get(ClientResponse.class);

    processImageResponse(response, contentType, "png");
  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMapJpgNonExistingZoomLevelBelowWithoutPyramids() throws Exception
  {
    String contentType = "image/jpeg";
    ClientResponse response = resource().path("/wms")
            .queryParam("SERVICE", "WMS")
            .queryParam("REQUEST", "getmap")
            .queryParam("LAYERS", "IslandsElevation-v2-no-pyramid")
            .queryParam("FORMAT", contentType)
            //pyramid only has a single zoom level = 10; pass in zoom level = 11
            .queryParam("BBOX", ISLANDS_ELEVATION_V2_PAST_HIGHEST_RES_ZOOM_LEVEL)
            .queryParam("WIDTH", "512")
            .queryParam("HEIGHT", "512")
            .get(ClientResponse.class);

    processImageResponse(response, contentType, "jpeg");
  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMapTifNonExistingZoomLevelBelowWithoutPyramids() throws Exception
  {
    String contentType = "image/tiff";
    ClientResponse response = resource().path("/wms")
            .queryParam("SERVICE", "WMS")
            .queryParam("REQUEST", "getmap")
            .queryParam("LAYERS", "IslandsElevation-v2-no-pyramid")
            .queryParam("FORMAT", contentType)
            //pyramid only has a single zoom level = 10; pass in zoom level = 11
            .queryParam("BBOX", ISLANDS_ELEVATION_V2_PAST_HIGHEST_RES_ZOOM_LEVEL)
            .queryParam("WIDTH", "512")
            .queryParam("HEIGHT", "512")
            .get(ClientResponse.class);

    processImageResponse(response, contentType, "tif");
  }

  /*
   * These should return the highest res available image resampled to the requested bounds.
   * "Extra metadata" implies that the levels described in the file don't reflect what raster images
   * actually exist in the pyramid.
   */

  @Test
  @Category(IntegrationTest.class)
  public void testGetMapPngNonExistingZoomLevelBelowWithoutPyramidsExtraMetadata() throws Exception
  {
    String contentType = "image/png";
    ClientResponse response = resource().path("/wms")
            .queryParam("SERVICE", "WMS")
            .queryParam("REQUEST", "getmap")
            .queryParam("LAYERS", "IslandsElevation-v2-no-pyramid-extra-metadata")
            .queryParam("FORMAT", contentType)
            //pyramid only has a single zoom level = 10; pass in zoom level = 11
            .queryParam("BBOX", ISLANDS_ELEVATION_V2_PAST_HIGHEST_RES_ZOOM_LEVEL)
            .queryParam("WIDTH", "512")
            .queryParam("HEIGHT", "512")
            .get(ClientResponse.class);

    processImageResponse(response, contentType, "png");
  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMapJpgNonExistingZoomLevelBelowWithoutPyramidsExtraMetadata() throws Exception
  {
    String contentType = "image/jpeg";
    ClientResponse response = resource().path("/wms")
            .queryParam("SERVICE", "WMS")
            .queryParam("REQUEST", "getmap")
            .queryParam("LAYERS", "IslandsElevation-v2-no-pyramid-extra-metadata")
            .queryParam("FORMAT", contentType)
            //pyramid only has a single zoom level = 10; pass in zoom level = 11
            .queryParam("BBOX", ISLANDS_ELEVATION_V2_PAST_HIGHEST_RES_ZOOM_LEVEL)
            .queryParam("WIDTH", "512")
            .queryParam("HEIGHT", "512")
            .get(ClientResponse.class);

    processImageResponse(response, contentType, "jpg");
  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMapTifNonExistingZoomLevelBelowWithoutPyramidsExtraMetadata() throws Exception
  {
    String contentType = "image/tiff";
    ClientResponse response = resource().path("/wms")
            .queryParam("SERVICE", "WMS")
            .queryParam("REQUEST", "getmap")
            .queryParam("LAYERS", "IslandsElevation-v2-no-pyramid-extra-metadata")
            .queryParam("FORMAT", contentType)
            //pyramid only has a single zoom level = 10; pass in zoom level = 11
            .queryParam("BBOX", ISLANDS_ELEVATION_V2_PAST_HIGHEST_RES_ZOOM_LEVEL)
            .queryParam("WIDTH", "512")
            .queryParam("HEIGHT", "512")
            .get(ClientResponse.class);

    processImageResponse(response, contentType, "tif");
  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMosaicJpgNonExistingZoomLevelAboveWithoutPyramids() throws Exception
  {
    String contentType = "image/jpeg";
    ClientResponse response = resource().path("/wms")
            .queryParam("SERVICE", "WMS")
            .queryParam("REQUEST", "getmosaic")
            .queryParam("LAYERS", "IslandsElevation-v2-no-pyramid")
            .queryParam("FORMAT", contentType)
            //pyramid only has a single zoom level = 10; pass in zoom level = 8
            .queryParam("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE)
            .get(ClientResponse.class);

    processImageResponse(response, contentType, "jpg");
  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMosaicTifNonExistingZoomLevelAboveWithoutPyramids() throws Exception
  {
    String contentType = "image/tiff";
    ClientResponse response = resource().path("/wms")
            .queryParam("SERVICE", "WMS")
            .queryParam("REQUEST", "getmosaic")
            .queryParam("LAYERS", "IslandsElevation-v2-no-pyramid")
            .queryParam("FORMAT", contentType)
            //pyramid only has a single zoom level = 10; pass in zoom level = 8
            .queryParam("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE)
            .get(ClientResponse.class);

    processImageResponse(response, contentType, "tif");
  }

  /*
   * These should return an empty image, since we don't support missing zoom levels above what
   * exists in MrsImagePyramid yet.  "Extra metadata" implies that the levels described in the file don't
   * reflect what raster images actually exist in the pyramid.
   */

  @Test
  @Category(IntegrationTest.class)
  public void testGetMosaicPngNonExistingZoomLevelAboveWithoutPyramidsExtraMetadata()
    throws Exception
  {
    String contentType = "image/png";
    ClientResponse response = resource().path("/wms")
            .queryParam("SERVICE", "WMS")
            .queryParam("REQUEST", "getmosaic")
            .queryParam("LAYERS", "IslandsElevation-v2-no-pyramid-extra-metadata")
            .queryParam("FORMAT", contentType)
            //pyramid only has a single zoom level = 10; pass in zoom level = 8
            .queryParam("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE)
            .get(ClientResponse.class);

    processImageResponse(response, contentType, "png");
  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMosaicJpgNonExistingZoomLevelAboveWithoutPyramidsExtraMetadata()
    throws Exception
  {
    String contentType = "image/jpeg";
    ClientResponse response = resource().path("/wms")
            .queryParam("SERVICE", "WMS")
            .queryParam("REQUEST", "getmosaic")
            .queryParam("LAYERS", "IslandsElevation-v2-no-pyramid-extra-metadata")
            .queryParam("FORMAT", contentType)
            //pyramid only has a single zoom level = 10; pass in zoom level = 8
            .queryParam("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE)
            .get(ClientResponse.class);

    processImageResponse(response, contentType, "jpg");
  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMosaicTifNonExistingZoomLevelAboveWithoutPyramidsExtraMetadata()
    throws Exception
  {
    String contentType = "image/tiff";
    ClientResponse response = resource().path("/wms")
            .queryParam("SERVICE", "WMS")
            .queryParam("REQUEST", "getmosaic")
            .queryParam("LAYERS", "IslandsElevation-v2-no-pyramid-extra-metadata")
            .queryParam("FORMAT", contentType)
            //pyramid only has a single zoom level = 10; pass in zoom level = 8
            .queryParam("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE)
            .get(ClientResponse.class);

    processImageResponse(response, contentType, "tif");
  }

  /*
   * These should return the highest res available image resampled to the requested bounds.
   */

  @Test
  @Category(IntegrationTest.class)
  public void testGetMosaicPngNonExistingZoomLevelBelowWithPyramids() throws Exception
  {
    String contentType = "image/png";
    ClientResponse response = resource().path("/wms")
            .queryParam("SERVICE", "WMS")
            .queryParam("REQUEST", "getmosaic")
            .queryParam("LAYERS", "IslandsElevation-v2")
            .queryParam("FORMAT", contentType)
            //pyramid only goes up to zoom level = 10; pass in zoom level = 11
            .queryParam("BBOX", ISLANDS_ELEVATION_V2_PAST_HIGHEST_RES_ZOOM_LEVEL)
            .get(ClientResponse.class);

    processImageResponse(response, contentType, "png");
  }

  /*
   * These should return the highest res available image resampled to the requested bounds.
   */

  @Test
  @Category(IntegrationTest.class)
  public void testGetMosaicPngNonExistingZoomLevelBelowWithoutPyramids() throws Exception
  {
    String contentType = "image/png";
    ClientResponse response = resource().path("/wms")
            .queryParam("SERVICE", "WMS")
            .queryParam("REQUEST", "getmosaic")
            .queryParam("LAYERS", "IslandsElevation-v2-no-pyramid")
            .queryParam("FORMAT", contentType)
            //pyramid only has a single zoom level = 10; pass in zoom level = 11
            .queryParam("BBOX", ISLANDS_ELEVATION_V2_PAST_HIGHEST_RES_ZOOM_LEVEL)
            .get(ClientResponse.class);

    processImageResponse(response, contentType, "png");
  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMosaicJpgNonExistingZoomLevelBelowWithoutPyramids() throws Exception
  {
    String contentType = "image/jpeg";
    ClientResponse response = resource().path("/wms")
            .queryParam("SERVICE", "WMS")
            .queryParam("REQUEST", "getmosaic")
            .queryParam("LAYERS", "IslandsElevation-v2-no-pyramid")
            .queryParam("FORMAT", contentType)
            //pyramid only has a single zoom level = 10; pass in zoom level = 11
            .queryParam("BBOX", ISLANDS_ELEVATION_V2_PAST_HIGHEST_RES_ZOOM_LEVEL)
            .get(ClientResponse.class);

    processImageResponse(response, contentType, "jpeg");
  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMosaicTifNonExistingZoomLevelBelowWithoutPyramids() throws Exception
  {
    String contentType = "image/tiff";
    ClientResponse response = resource().path("/wms")
            .queryParam("SERVICE", "WMS")
            .queryParam("REQUEST", "getmosaic")
            .queryParam("LAYERS", "IslandsElevation-v2-no-pyramid")
            .queryParam("FORMAT", contentType)
            //pyramid only has a single zoom level = 10; pass in zoom level = 11
            .queryParam("BBOX", ISLANDS_ELEVATION_V2_PAST_HIGHEST_RES_ZOOM_LEVEL)
            .get(ClientResponse.class);

    processImageResponse(response, contentType, "tif");
  }

  /*
   * These should return the highest res available image resampled to the requested bounds.
   * "Extra metadata" implies that the levels described in the file don't reflect what raster images
   * actually exist in the pyramid.
   */

  @Test
  @Category(IntegrationTest.class)
  public void testGetMosaicPngNonExistingZoomLevelBelowWithoutPyramidsExtraMetadata() throws Exception
  {
    String contentType = "image/png";
    ClientResponse response = resource().path("/wms")
            .queryParam("SERVICE", "WMS")
            .queryParam("REQUEST", "getmosaic")
            .queryParam("LAYERS", "IslandsElevation-v2-no-pyramid-extra-metadata")
            .queryParam("FORMAT", contentType)
            //pyramid only has a single zoom level = 10; pass in zoom level = 11
            .queryParam("BBOX", ISLANDS_ELEVATION_V2_PAST_HIGHEST_RES_ZOOM_LEVEL)
            .get(ClientResponse.class);

    processImageResponse(response, contentType, "png");
  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMosaicJpgNonExistingZoomLevelBelowWithoutPyramidsExtraMetadata() throws Exception
  {
    String contentType = "image/jpeg";
    ClientResponse response = resource().path("/wms")
            .queryParam("SERVICE", "WMS")
            .queryParam("REQUEST", "getmosaic")
            .queryParam("LAYERS", "IslandsElevation-v2-no-pyramid-extra-metadata")
            .queryParam("FORMAT", contentType)
            //pyramid only has a single zoom level = 10; pass in zoom level = 11
            .queryParam("BBOX", ISLANDS_ELEVATION_V2_PAST_HIGHEST_RES_ZOOM_LEVEL)
            .get(ClientResponse.class);

    processImageResponse(response, contentType, "jpg");
  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMosaicTifNonExistingZoomLevelBelowWithoutPyramidsExtraMetadata() throws Exception
  {
    String contentType = "image/tiff";
    ClientResponse response = resource().path("/wms")
            .queryParam("SERVICE", "WMS")
            .queryParam("REQUEST", "getmosaic")
            .queryParam("LAYERS", "IslandsElevation-v2-no-pyramid-extra-metadata")
            .queryParam("FORMAT", contentType)
            //pyramid only has a single zoom level = 10; pass in zoom level = 11
            .queryParam("BBOX", ISLANDS_ELEVATION_V2_PAST_HIGHEST_RES_ZOOM_LEVEL)
            .get(ClientResponse.class);

    processImageResponse(response, contentType, "tif");
  }

  /*
   * These should return an empty image, since we don't support missing zoom levels above what
   * exists in MrsImagePyramid yet.
   * @todo Once the WMS can handle this case for non-pyramid data, update the baseline image for
   * this test.
   */

  @Test
  @Category(IntegrationTest.class)
  public void testGetMosaicPngNonExistingZoomLevelAboveWithoutPyramids() throws Exception
  {
    String contentType = "image/png";
    ClientResponse response = resource().path("/wms")
            .queryParam("SERVICE", "WMS")
            .queryParam("REQUEST", "getmosaic")
            .queryParam("LAYERS", "IslandsElevation-v2-no-pyramid")
            .queryParam("FORMAT", "image/png")
            //pyramid only has a single zoom level = 10; pass in zoom level = 8
            .queryParam("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE)
            .get(ClientResponse.class);

    processImageResponse(response, contentType, "png");
  }
}
