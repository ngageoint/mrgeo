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

package org.mrgeo.services.wms;

import com.meterware.httpunit.WebRequest;
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
  public static void setUp()
  {
    try
    {
      baselineInput = TestUtils.composeInputDir(MissingPyramidsTest.class);
      WmsGeneratorTestAbstract.setUp();
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
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmap");
      request.setParameter("LAYERS", "IslandsElevation-v2-no-pyramid");
      request.setParameter("FORMAT", "image/png");
      //pyramid only has a single zoom level = 10; pass in zoom level = 8
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE);
      request.setParameter("WIDTH", "512");
      request.setParameter("HEIGHT", "512");

      processImageResponse(webClient.getResponse(request), "png");
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMapJpgNonExistingZoomLevelAboveWithoutPyramids() throws Exception
  {
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmap");
      request.setParameter("LAYERS", "IslandsElevation-v2-no-pyramid");
      request.setParameter("FORMAT", "image/jpeg");
      //pyramid only has a single zoom level = 10; pass in zoom level = 8
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE);
      request.setParameter("WIDTH", "512");
      request.setParameter("HEIGHT", "512");

      processImageResponse(webClient.getResponse(request), "jpg");
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMapTifNonExistingZoomLevelAboveWithoutPyramids() throws Exception
  {
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmap");
      request.setParameter("LAYERS", "IslandsElevation-v2-no-pyramid");
      request.setParameter("FORMAT", "image/tiff");
      //pyramid only has a single zoom level = 10; pass in zoom level = 8
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE);
      request.setParameter("WIDTH", "512");
      request.setParameter("HEIGHT", "512");

      processImageResponse(webClient.getResponse(request), "tif");
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
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
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmap");
      request.setParameter("LAYERS", "IslandsElevation-v2-no-pyramid-extra-metadata");
      request.setParameter("FORMAT", "image/png");
      //pyramid only has a single zoom level = 10; pass in zoom level = 8
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE);
      request.setParameter("WIDTH", "512");
      request.setParameter("HEIGHT", "512");

      processImageResponse(webClient.getResponse(request), "png");
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMapJpgNonExistingZoomLevelAboveWithoutPyramidsExtraMetadata() throws Exception
  {
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmap");
      request.setParameter("LAYERS", "IslandsElevation-v2-no-pyramid-extra-metadata");
      request.setParameter("FORMAT", "image/jpeg");
      //pyramid only has a single zoom level = 10; pass in zoom level = 8
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE);
      request.setParameter("WIDTH", "512");
      request.setParameter("HEIGHT", "512");

      processImageResponse(webClient.getResponse(request), "jpg");
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMapTifNonExistingZoomLevelAboveWithoutPyramidsExtraMetadata() throws Exception
  {
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmap");
      request.setParameter("LAYERS", "IslandsElevation-v2-no-pyramid-extra-metadata");
      request.setParameter("FORMAT", "image/tiff");
      //pyramid only has a single zoom level = 10; pass in zoom level = 8
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE);
      request.setParameter("WIDTH", "512");
      request.setParameter("HEIGHT", "512");

      processImageResponse(webClient.getResponse(request), "tif");
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }

  /*
   * These should return the highest res available image resampled to the requested bounds.
   */

  @Test
  @Category(IntegrationTest.class)
  public void testGetMapPngNonExistingZoomLevelBelowWithoutPyramids() throws Exception
  {
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmap");
      request.setParameter("LAYERS", "IslandsElevation-v2-no-pyramid");
      request.setParameter("FORMAT", "image/png");
      //pyramid only has a single zoom level = 10; pass in zoom level = 11
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_PAST_HIGHEST_RES_ZOOM_LEVEL);
      request.setParameter("WIDTH", "512");
      request.setParameter("HEIGHT", "512");

      processImageResponse(webClient.getResponse(request), "png");
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMapJpgNonExistingZoomLevelBelowWithoutPyramids() throws Exception
  {
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmap");
      request.setParameter("LAYERS", "IslandsElevation-v2-no-pyramid");
      request.setParameter("FORMAT", "image/jpeg");
      //pyramid only has a single zoom level = 10; pass in zoom level = 11
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_PAST_HIGHEST_RES_ZOOM_LEVEL);
      request.setParameter("WIDTH", "512");
      request.setParameter("HEIGHT", "512");

      processImageResponse(webClient.getResponse(request), "jpeg");
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMapTifNonExistingZoomLevelBelowWithoutPyramids() throws Exception
  {
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmap");
      request.setParameter("LAYERS", "IslandsElevation-v2-no-pyramid");
      request.setParameter("FORMAT", "image/tiff");
      //pyramid only has a single zoom level = 10; pass in zoom level = 11
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_PAST_HIGHEST_RES_ZOOM_LEVEL);
      request.setParameter("WIDTH", "512");
      request.setParameter("HEIGHT", "512");

      processImageResponse(webClient.getResponse(request), "tif");
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
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
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmap");
      request.setParameter("LAYERS", "IslandsElevation-v2-no-pyramid-extra-metadata");
      request.setParameter("FORMAT", "image/png");
      //pyramid only has a single zoom level = 10; pass in zoom level = 11
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_PAST_HIGHEST_RES_ZOOM_LEVEL);
      request.setParameter("WIDTH", "512");
      request.setParameter("HEIGHT", "512");

      processImageResponse(webClient.getResponse(request), "png");
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMapJpgNonExistingZoomLevelBelowWithoutPyramidsExtraMetadata() throws Exception
  {
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmap");
      request.setParameter("LAYERS", "IslandsElevation-v2-no-pyramid-extra-metadata");
      request.setParameter("FORMAT", "image/jpeg");
      //pyramid only has a single zoom level = 10; pass in zoom level = 11
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_PAST_HIGHEST_RES_ZOOM_LEVEL);
      request.setParameter("WIDTH", "512");
      request.setParameter("HEIGHT", "512");

      processImageResponse(webClient.getResponse(request), "jpg");
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMapTifNonExistingZoomLevelBelowWithoutPyramidsExtraMetadata() throws Exception
  {
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmap");
      request.setParameter("LAYERS", "IslandsElevation-v2-no-pyramid-extra-metadata");
      request.setParameter("FORMAT", "image/tiff");
      //pyramid only has a single zoom level = 10; pass in zoom level = 11
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_PAST_HIGHEST_RES_ZOOM_LEVEL);
      request.setParameter("WIDTH", "512");
      request.setParameter("HEIGHT", "512");

      processImageResponse(webClient.getResponse(request), "tif");
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMosaicJpgNonExistingZoomLevelAboveWithoutPyramids() throws Exception
  {
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmosaic");
      request.setParameter("LAYERS", "IslandsElevation-v2-no-pyramid");
      request.setParameter("FORMAT", "image/jpeg");
      //pyramid only has a single zoom level = 10; pass in zoom level = 8
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE);

      processImageResponse(webClient.getResponse(request), "jpg");
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMosaicTifNonExistingZoomLevelAboveWithoutPyramids() throws Exception
  {
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmosaic");
      request.setParameter("LAYERS", "IslandsElevation-v2-no-pyramid");
      request.setParameter("FORMAT", "image/tiff");
      //pyramid only has a single zoom level = 10; pass in zoom level = 8
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE);

      processImageResponse(webClient.getResponse(request), "tif");
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
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
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmosaic");
      request.setParameter("LAYERS", "IslandsElevation-v2-no-pyramid-extra-metadata");
      request.setParameter("FORMAT", "image/png");
      //pyramid only has a single zoom level = 10; pass in zoom level = 8
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE);

      processImageResponse(webClient.getResponse(request), "png");
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMosaicJpgNonExistingZoomLevelAboveWithoutPyramidsExtraMetadata()
    throws Exception
  {
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmosaic");
      request.setParameter("LAYERS", "IslandsElevation-v2-no-pyramid-extra-metadata");
      request.setParameter("FORMAT", "image/jpeg");
      //pyramid only has a single zoom level = 10; pass in zoom level = 8
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE);

      processImageResponse(webClient.getResponse(request), "jpg");
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMosaicTifNonExistingZoomLevelAboveWithoutPyramidsExtraMetadata()
    throws Exception
  {
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmosaic");
      request.setParameter("LAYERS", "IslandsElevation-v2-no-pyramid-extra-metadata");
      request.setParameter("FORMAT", "image/tiff");
      //pyramid only has a single zoom level = 10; pass in zoom level = 8
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE);

      processImageResponse(webClient.getResponse(request), "tif");
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }

  /*
   * These should return the highest res available image resampled to the requested bounds.
   */

  @Test
  @Category(IntegrationTest.class)
  public void testGetMosaicPngNonExistingZoomLevelBelowWithPyramids() throws Exception
  {
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmosaic");
      request.setParameter("LAYERS", "IslandsElevation-v2");
      request.setParameter("FORMAT", "image/png");
      //pyramid only goes up to zoom level = 10; pass in zoom level = 11
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_PAST_HIGHEST_RES_ZOOM_LEVEL);

      processImageResponse(webClient.getResponse(request), "png");
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }

  /*
   * These should return the highest res available image resampled to the requested bounds.
   */

  @Test
  @Category(IntegrationTest.class)
  public void testGetMosaicPngNonExistingZoomLevelBelowWithoutPyramids() throws Exception
  {
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmosaic");
      request.setParameter("LAYERS", "IslandsElevation-v2-no-pyramid");
      request.setParameter("FORMAT", "image/png");
      //pyramid only has a single zoom level = 10; pass in zoom level = 11
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_PAST_HIGHEST_RES_ZOOM_LEVEL);

      processImageResponse(webClient.getResponse(request), "png");
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMosaicJpgNonExistingZoomLevelBelowWithoutPyramids() throws Exception
  {
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmosaic");
      request.setParameter("LAYERS", "IslandsElevation-v2-no-pyramid");
      request.setParameter("FORMAT", "image/jpeg");
      //pyramid only has a single zoom level = 10; pass in zoom level = 11
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_PAST_HIGHEST_RES_ZOOM_LEVEL);

      processImageResponse(webClient.getResponse(request), "jpeg");
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMosaicTifNonExistingZoomLevelBelowWithoutPyramids() throws Exception
  {
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmosaic");
      request.setParameter("LAYERS", "IslandsElevation-v2-no-pyramid");
      request.setParameter("FORMAT", "image/tiff");
      //pyramid only has a single zoom level = 10; pass in zoom level = 11
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_PAST_HIGHEST_RES_ZOOM_LEVEL);

      processImageResponse(webClient.getResponse(request), "tif");
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
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
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmosaic");
      request.setParameter("LAYERS", "IslandsElevation-v2-no-pyramid-extra-metadata");
      request.setParameter("FORMAT", "image/png");
      //pyramid only has a single zoom level = 10; pass in zoom level = 11
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_PAST_HIGHEST_RES_ZOOM_LEVEL);

      processImageResponse(webClient.getResponse(request), "png");
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMosaicJpgNonExistingZoomLevelBelowWithoutPyramidsExtraMetadata() throws Exception
  {
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmosaic");
      request.setParameter("LAYERS", "IslandsElevation-v2-no-pyramid-extra-metadata");
      request.setParameter("FORMAT", "image/jpeg");
      //pyramid only has a single zoom level = 10; pass in zoom level = 11
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_PAST_HIGHEST_RES_ZOOM_LEVEL);

      processImageResponse(webClient.getResponse(request), "jpg");
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMosaicTifNonExistingZoomLevelBelowWithoutPyramidsExtraMetadata() throws Exception
  {
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmosaic");
      request.setParameter("LAYERS", "IslandsElevation-v2-no-pyramid-extra-metadata");
      request.setParameter("FORMAT", "image/tiff");
      //pyramid only has a single zoom level = 10; pass in zoom level = 11
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_PAST_HIGHEST_RES_ZOOM_LEVEL);

      processImageResponse(webClient.getResponse(request), "tif");
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
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
  public void testGetMosaicPngNonExistingZoomLevelAboveWithoutPyramids() throws Exception
  {
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmosaic");
      request.setParameter("LAYERS", "IslandsElevation-v2-no-pyramid");
      request.setParameter("FORMAT", "image/png");
      //pyramid only has a single zoom level = 10; pass in zoom level = 8
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE);

      processImageResponse(webClient.getResponse(request), "png");
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }
}
