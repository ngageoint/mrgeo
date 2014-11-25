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
import com.meterware.httpunit.WebResponse;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.junit.IntegrationTest;
import org.mrgeo.services.utils.ImageTestUtils;
import org.mrgeo.test.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("static-method")
public class GetMapTest extends WmsGeneratorTestAbstract
{
  private static final Logger log = LoggerFactory.getLogger(GetMapTest.class);

  public static void main(final String[] args) throws Exception
  {
    WebResponse response = null;

    try
    {
      // LoggingUtils.setLogLevel("org.mrgeo", LoggingUtils.DEBUG);
      setUp();

      final WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmap");
      request.setParameter("LAYERS", "IslandsElevation-v2");
      request.setParameter("FORMAT", "image/png");
      request.setParameter("BBOX", "160.0,-12.0,164.0,-8.0");
      request.setParameter("WIDTH", "512");
      request.setParameter("HEIGHT", "512");

      long start = System.currentTimeMillis();

      System.out.println("starting");
      response = webClient.getResponse(request);

      System.out.println("time: " + (System.currentTimeMillis() - start));

      long totaltime = 0;
      final int loops = 10;
      for (int i = 0; i < loops; i++)
      {
        start = System.currentTimeMillis();
        response = webClient.getResponse(request);

        final long time = (System.currentTimeMillis() - start);
        totaltime += time;
        System.out.println("time: " + time);
      }

      System.out.println("Average time: " + (totaltime / loops));
      final String outputPath = "test.png";
      log.info("Generating baseline image: " + outputPath);
      ImageTestUtils.writeBaselineImage(response, outputPath);

      // processImageResponse(response, "png");
    }
    catch (final Exception e)
    {
      e.printStackTrace();
      throw e;
    }
    finally
    {
      if (response != null)
      {
        response.close();
      }
    }
  }

  @BeforeClass
  public static void setUp()
  {
    try
    {
      baselineInput = TestUtils.composeInputDir(GetMapTest.class);
      WmsGeneratorTestAbstract.setUp();
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
    WebResponse response = null;

    try
    {
      final WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmap");
      request.setParameter("LAYERS",
        "/mrgeo/test-files/org.mrgeo.services.wms/WmsGeneratorTestAbstract/IslandsElevation-v2");
      request.setParameter("FORMAT", "image/png");
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE);
      request.setParameter("WIDTH", "512");
      request.setParameter("HEIGHT", "512");

      response = webClient.getResponse(request);

      processImageResponse(response, "png");
    }
    catch (final Exception e)
    {
      e.printStackTrace();
      throw e;
    }
    finally
    {
      if (response != null)
      {
        response.close();
      }
    }

  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMapGeoTifLargerThanTileSize() throws Exception
  {
    WebResponse response = null;

    try
    {
      final WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmap");
      request.setParameter("LAYERS", "IslandsElevation-v2");
      request.setParameter("FORMAT", "image/geotiff");
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE);
      request.setParameter("WIDTH", "1024");
      request.setParameter("HEIGHT", "1024");

      response = webClient.getResponse(request);

      processImageResponse(response, "tif");
    }
    catch (final Exception e)
    {
      e.printStackTrace();
      throw e;
    }
    finally
    {
      if (response != null)
      {
        response.close();
      }
    }

  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMapGeoTifMultipleSourceTiles() throws Exception
  {
    WebResponse response = null;

    try
    {
      final WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmap");
      request.setParameter("LAYERS", "IslandsElevation-v2");
      request.setParameter("FORMAT", "image/geotif");
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_MULTIPLE_SOURCE_TILES);
      request.setParameter("WIDTH", "512");
      request.setParameter("HEIGHT", "512");

      response = webClient.getResponse(request);

      processImageResponse(response, "tif");
    }
    catch (final Exception e)
    {
      e.printStackTrace();
      throw e;
    }
    finally
    {
      if (response != null)
      {
        response.close();
      }
    }

  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMapGeoTifNonExistingZoomLevelBelowWithPyramids() throws Exception
  {
    WebResponse response = null;

    try
    {
      final WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmap");
      request.setParameter("LAYERS", "IslandsElevation-v2");
      request.setParameter("FORMAT", "image/geotiff");
      // pyramid only goes up to zoom level = 10; pass in zoom level = 11
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_PAST_HIGHEST_RES_ZOOM_LEVEL);
      request.setParameter("WIDTH", "512");
      request.setParameter("HEIGHT", "512");

      response = webClient.getResponse(request);

      processImageResponse(response, "tif");
    }
    catch (final Exception e)
    {
      e.printStackTrace();
      throw e;
    }
    finally
    {
      if (response != null)
      {
        response.close();
      }
    }

  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMapGeoTifRectangularTileSize() throws Exception
  {
    WebResponse response = null;

    try
    {
      final WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmap");
      request.setParameter("LAYERS", "IslandsElevation-v2");
      request.setParameter("FORMAT", "image/geotiff");
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE);
      request.setParameter("WIDTH", "700");
      request.setParameter("HEIGHT", "300");

      response = webClient.getResponse(request);

      processImageResponse(response, "tif");
    }
    catch (final Exception e)
    {
      e.printStackTrace();
      throw e;
    }
    finally
    {
      if (response != null)
      {
        response.close();
      }
    }

  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMapGeoTifSingleSourceTile() throws Exception
  {
    WebResponse response = null;

    try
    {
      final WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmap");
      request.setParameter("LAYERS", "IslandsElevation-v2");
      request.setParameter("FORMAT", "image/geotif");
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE);
      request.setParameter("WIDTH", "512");
      request.setParameter("HEIGHT", "512");

      response = webClient.getResponse(request);

      processImageResponse(response, "tif");
    }
    catch (final Exception e)
    {
      e.printStackTrace();
      throw e;
    }
    finally
    {
      if (response != null)
      {
        response.close();
      }
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMapInvalidFormat() throws Exception
  {
    WebResponse response = null;
    try
    {
      final WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmap");
      request.setParameter("LAYERS", "IslandsElevation-v2");
      request.setParameter("FORMAT", "image/abc");
      request.setParameter("BBOX", "160.312500,-11.250000,161.718750,-9.843750");
      request.setParameter("WIDTH", "512");
      request.setParameter("HEIGHT", "512");

      response = webClient.getResponse(request);
    }
    catch (final Exception e)
    {
      e.printStackTrace();
      throw e;
    }
    finally
    {
      if (response != null)
      {
        assertEquals(response.getResponseCode(), 200);
        assertTrue(response.getText().contains(
          "<ServiceException code=\"Unsupported image format - image/abc"));
        response.close();
      }
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMapInvalidLayer() throws Exception
  {
    WebResponse response = null;
    try
    {
      final WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmap");
      request.setParameter("LAYERS", "IslandsElevation-v3");
      request.setParameter("FORMAT", "image/png");
      request.setParameter("BBOX", "160.312500,-11.250000,161.718750,-9.843750");
      request.setParameter("WIDTH", "512");
      request.setParameter("HEIGHT", "512");

      response = webClient.getResponse(request);
    }
    catch (final Exception e)
    {
      e.printStackTrace();
      throw e;
    }
    finally
    {
      if (response != null)
      {
        assertEquals(response.getResponseCode(), 200);
        assertTrue(response
          .getText()
          .contains(
            "org.mrgeo.data.DataProviderNotFound: Unable to find a MrsImage data provider for IslandsElevation-v3"));
        response.close();
      }
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMapJpgLargerThanTileSize() throws Exception
  {
    WebResponse response = null;

    try
    {
      final WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmap");
      request.setParameter("LAYERS", "IslandsElevation-v2");
      request.setParameter("FORMAT", "image/jpeg");
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE);
      request.setParameter("WIDTH", "1024");
      request.setParameter("HEIGHT", "1024");

      response = webClient.getResponse(request);

      processImageResponse(response, "jpg");
    }
    catch (final Exception e)
    {
      e.printStackTrace();
      throw e;
    }
    finally
    {
      if (response != null)
      {
        response.close();
      }
    }

  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMapJpgMultipleSourceTiles() throws Exception
  {
    WebResponse response = null;

    try
    {
      final WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmap");
      request.setParameter("LAYERS", "IslandsElevation-v2");
      request.setParameter("FORMAT", "image/jpg");
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_MULTIPLE_SOURCE_TILES);
      request.setParameter("WIDTH", "512");
      request.setParameter("HEIGHT", "512");

      response = webClient.getResponse(request);

      processImageResponse(response, "jpg");
    }
    catch (final Exception e)
    {
      e.printStackTrace();
      throw e;
    }
    finally
    {
      if (response != null)
      {
        response.close();
      }
    }

  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMapJpgNonExistingZoomLevelBelowWithPyramids() throws Exception
  {
    WebResponse response = null;

    try
    {
      final WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmap");
      request.setParameter("LAYERS", "IslandsElevation-v2");
      request.setParameter("FORMAT", "image/jpeg");
      // pyramid only goes up to zoom level = 10; pass in zoom level = 11
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_PAST_HIGHEST_RES_ZOOM_LEVEL);
      request.setParameter("WIDTH", "512");
      request.setParameter("HEIGHT", "512");

      response = webClient.getResponse(request);

      processImageResponse(response, "jpg");
    }
    catch (final Exception e)
    {
      e.printStackTrace();
      throw e;
    }
    finally
    {
      if (response != null)
      {
        response.close();
      }
    }

  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMapJpgRectangularTileSize() throws Exception
  {
    WebResponse response = null;

    try
    {
      final WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmap");
      request.setParameter("LAYERS", "IslandsElevation-v2");
      request.setParameter("FORMAT", "image/jpeg");
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE);
      request.setParameter("WIDTH", "700");
      request.setParameter("HEIGHT", "300");

      response = webClient.getResponse(request);

      processImageResponse(response, "jpg");
    }
    catch (final Exception e)
    {
      e.printStackTrace();
      throw e;
    }
    finally
    {
      if (response != null)
      {
        response.close();
      }
    }

  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMapJpgSingleSourceTile() throws Exception
  {
    WebResponse response = null;

    try
    {
      final WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmap");
      request.setParameter("LAYERS", "IslandsElevation-v2");
      request.setParameter("FORMAT", "image/jpg");
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE);
      request.setParameter("WIDTH", "512");
      request.setParameter("HEIGHT", "512");

      response = webClient.getResponse(request);

      processImageResponse(response, "jpg");
    }
    catch (final Exception e)
    {
      e.printStackTrace();
      throw e;
    }
    finally
    {
      if (response != null)
      {
        response.close();
      }
    }

  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMapLowerCaseParams() throws Exception
  {
    WebResponse response = null;

    try
    {
      final WebRequest request = createRequest();
      request.setParameter("request", "getmap");
      request.setParameter("layers", "IslandsElevation-v2");
      request.setParameter("format", "image/png");
      request.setParameter("bbox", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE);
      request.setParameter("width", "512");
      request.setParameter("height", "512");

      response = webClient.getResponse(request);

      processImageResponse(response, "png");
    }
    catch (final Exception e)
    {
      e.printStackTrace();
      throw e;
    }
    finally
    {
      if (response != null)
      {
        response.close();
      }
    }

  }

  /*
   * WmsGenerator doesn't support more than one layer per request.
   */

  @Test
  @Category(IntegrationTest.class)
  public void testGetMapMultipleRequestLayers() throws Exception
  {
    WebResponse response = null;
    try
    {
      final WebRequest request = createRequest();
      request.setParameter("request", "getmap");
      request.setParameter("LAYERS", "IslandsElevation-v2,IslandsElevation-v3");
      request.setParameter("format", "image/png");
      request.setParameter("bbox", "160.312500,-11.250000,161.718750,-9.843750");
      request.setParameter("width", "512");
      request.setParameter("height", "512");

      response = webClient.getResponse(request);
    }
    catch (final Exception e)
    {
      e.printStackTrace();
      throw e;
    }
    finally
    {
      if (response != null)
      {
        assertEquals(response.getResponseCode(), 200);
        assertTrue(response
          .getText()
          .contains(
            "org.mrgeo.data.DataProviderNotFound: Unable to find a MrsImage data provider for IslandsElevation-v2,IslandsElevation-v3"));
        response.close();
      }
    }
  }

  /*
   * JPG doesn't support transparency, so a black image is returned for out of bounds requests.
   */

  @Test
  @Category(IntegrationTest.class)
  public void testGetMapOutOfBoundsJpg() throws Exception
  {
    WebResponse response = null;

    try
    {
      final WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmap");
      request.setParameter("LAYERS", "IslandsElevation-v2");
      request.setParameter("FORMAT", "image/jpg");
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_OUT_OF_BOUNDS);
      request.setParameter("WIDTH", "512");
      request.setParameter("HEIGHT", "512");

      response = webClient.getResponse(request);

      processImageResponse(response, "jpg");
    }
    catch (final Exception e)
    {
      e.printStackTrace();
      throw e;
    }
    finally
    {
      if (response != null)
      {
        response.close();
      }
    }

  }

  /*
   * PNG out of bounds requests should return a transparent image.
   */

  @Test
  @Category(IntegrationTest.class)
  public void testGetMapOutOfBoundsPng() throws Exception
  {
    WebResponse response = null;

    try
    {
      final WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmap");
      request.setParameter("LAYERS", "IslandsElevation-v2");
      request.setParameter("FORMAT", "image/png");
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_OUT_OF_BOUNDS);
      request.setParameter("WIDTH", "512");
      request.setParameter("HEIGHT", "512");

      response = webClient.getResponse(request);

      processImageResponse(response, "png");
    }
    catch (final Exception e)
    {
      e.printStackTrace();
      throw e;
    }
    finally
    {
      if (response != null)
      {
        response.close();
      }
    }

  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMapOutOfBoundsTif() throws Exception
  {
    WebResponse response = null;

    try
    {
      final WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmap");
      request.setParameter("LAYERS", "IslandsElevation-v2");
      request.setParameter("FORMAT", "image/tif");
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_OUT_OF_BOUNDS);
      request.setParameter("WIDTH", "512");
      request.setParameter("HEIGHT", "512");

      response = webClient.getResponse(request);

      processImageResponse(response, "tif");
    }
    catch (final Exception e)
    {
      e.printStackTrace();
      throw e;
    }
    finally
    {
      if (response != null)
      {
        response.close();
      }
    }

  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMapPngLargerThanTileSize() throws Exception
  {
    WebResponse response = null;

    try
    {
      final WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmap");
      request.setParameter("LAYERS", "IslandsElevation-v2");
      request.setParameter("FORMAT", "image/png");
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE);
      request.setParameter("WIDTH", "1024");
      request.setParameter("HEIGHT", "1024");

      response = webClient.getResponse(request);

      processImageResponse(response, "png");
    }
    catch (final Exception e)
    {
      e.printStackTrace();
      throw e;
    }
    finally
    {
      if (response != null)
      {
        response.close();
      }
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMapPngMultipleSourceTiles() throws Exception
  {
    WebResponse response = null;

    try
    {
      final WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmap");
      request.setParameter("LAYERS", "IslandsElevation-v2");
      request.setParameter("FORMAT", "image/png");
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_MULTIPLE_SOURCE_TILES);
      request.setParameter("WIDTH", "512");
      request.setParameter("HEIGHT", "512");

      response = webClient.getResponse(request);

      processImageResponse(response, "png");
    }
    catch (final Exception e)
    {
      e.printStackTrace();
      throw e;
    }
    finally
    {
      if (response != null)
      {
        response.close();
      }
    }

  }

  /*
   * These should return the highest res available image resampled to the requested bounds.
   */

  @Test
  @Category(IntegrationTest.class)
  public void testGetMapPngNonExistingZoomLevelBelowWithPyramids() throws Exception
  {
    WebResponse response = null;

    try
    {
      final WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmap");
      request.setParameter("LAYERS", "IslandsElevation-v2");
      request.setParameter("FORMAT", "image/png");
      // pyramid only goes up to zoom level = 10; pass in zoom level = 11
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_PAST_HIGHEST_RES_ZOOM_LEVEL);
      request.setParameter("WIDTH", "512");
      request.setParameter("HEIGHT", "512");

      response = webClient.getResponse(request);

      processImageResponse(response, "png");
    }
    catch (final Exception e)
    {
      e.printStackTrace();
      throw e;
    }
    finally
    {
      if (response != null)
      {
        response.close();
      }
    }

  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMapPngRectangularTileSize() throws Exception
  {
    WebResponse response = null;

    try
    {
      final WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmap");
      request.setParameter("LAYERS", "IslandsElevation-v2");
      request.setParameter("FORMAT", "image/png");
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE);
      request.setParameter("WIDTH", "700");
      request.setParameter("HEIGHT", "300");

      response = webClient.getResponse(request);

      processImageResponse(response, "png");
    }
    catch (final Exception e)
    {
      e.printStackTrace();
      throw e;
    }
    finally
    {
      if (response != null)
      {
        response.close();
      }
    }

  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMapPngSingleSourceTile() throws Exception
  {
    WebResponse response = null;

    try
    {
      final WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmap");
      request.setParameter("LAYERS", "IslandsElevation-v2");
      request.setParameter("FORMAT", "image/png");
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE);
      request.setParameter("WIDTH", "512");
      request.setParameter("HEIGHT", "512");

      response = webClient.getResponse(request);

      processImageResponse(response, "png");
    }
    catch (final Exception e)
    {
      e.printStackTrace();
      throw e;
    }
    finally
    {
      if (response != null)
      {
        response.close();
      }
    }

  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMapTifLargerThanTileSize() throws Exception
  {
    WebResponse response = null;

    try
    {
      final WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmap");
      request.setParameter("LAYERS", "IslandsElevation-v2");
      request.setParameter("FORMAT", "image/tiff");
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE);
      request.setParameter("WIDTH", "1024");
      request.setParameter("HEIGHT", "1024");

      response = webClient.getResponse(request);

      processImageResponse(response, "tif");
    }
    catch (final Exception e)
    {
      e.printStackTrace();
      throw e;
    }
    finally
    {
      if (response != null)
      {
        response.close();
      }
    }

  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMapTifMultipleSourceTiles() throws Exception
  {
    WebResponse response = null;

    try
    {
      final WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmap");
      request.setParameter("LAYERS", "IslandsElevation-v2");
      request.setParameter("FORMAT", "image/tif");
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_MULTIPLE_SOURCE_TILES);
      request.setParameter("WIDTH", "512");
      request.setParameter("HEIGHT", "512");

      response = webClient.getResponse(request);

      processImageResponse(response, "tif");
    }
    catch (final Exception e)
    {
      e.printStackTrace();
      throw e;
    }
    finally
    {
      if (response != null)
      {
        response.close();
      }
    }

  }

  /*
   * These should return the highest res available image resampled to the requested bounds.
   */

  @Test
  @Category(IntegrationTest.class)
  public void testGetMapTifNonExistingZoomLevelBelowWithPyramids() throws Exception
  {
    WebResponse response = null;

    try
    {
      final WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmap");
      request.setParameter("LAYERS", "IslandsElevation-v2");
      request.setParameter("FORMAT", "image/tiff");
      // pyramid only goes up to zoom level = 10; pass in zoom level = 11
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_PAST_HIGHEST_RES_ZOOM_LEVEL);
      request.setParameter("WIDTH", "512");
      request.setParameter("HEIGHT", "512");

      response = webClient.getResponse(request);

      processImageResponse(response, "tif");
    }
    catch (final Exception e)
    {
      e.printStackTrace();
      throw e;
    }
    finally
    {
      if (response != null)
      {
        response.close();
      }
    }

  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMapTifRectangularTileSize() throws Exception
  {
    WebResponse response = null;

    try
    {
      final WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmap");
      request.setParameter("LAYERS", "IslandsElevation-v2");
      request.setParameter("FORMAT", "image/tiff");
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE);
      request.setParameter("WIDTH", "700");
      request.setParameter("HEIGHT", "300");

      response = webClient.getResponse(request);

      processImageResponse(response, "tif");
    }
    catch (final Exception e)
    {
      e.printStackTrace();
      throw e;
    }
    finally
    {
      if (response != null)
      {
        response.close();
      }
    }

  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMapTifSingleSourceTile() throws Exception
  {
    WebResponse response = null;

    try
    {
      final WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmap");
      request.setParameter("LAYERS", "IslandsElevation-v2");
      request.setParameter("FORMAT", "image/tif");
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE);
      request.setParameter("WIDTH", "512");
      request.setParameter("HEIGHT", "512");

      response = webClient.getResponse(request);

      processImageResponse(response, "tif");
    }
    catch (final Exception e)
    {
      e.printStackTrace();
      throw e;
    }
    finally
    {
      if (response != null)
      {
        response.close();
      }
    }
  }

  // See image stretch test notes in RasterResourceTest::testImageStretch

  @Test
  @Category(IntegrationTest.class)
  public void testImageStretch() throws Exception
  {
    WebResponse response = null;
    try
    {
      final WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmap");
      request.setParameter("LAYERS", imageStretchUnqualified);
      request.setParameter("FORMAT", "image/png");
      request.setParameter("BBOX",
        "69.14932562595378,34.85619123437472,69.37012237404623,35.038450765625285");
      request.setParameter("WIDTH", "800");
      request.setParameter("HEIGHT", "600");

      response = webClient.getResponse(request);

      processImageResponse(response, "png");
    }
    catch (final Exception e)
    {
      e.printStackTrace();
      throw e;
    }
    finally
    {
      if (response != null)
      {
        response.close();
      }
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void testImageStretch2() throws Exception
  {
    WebResponse response = null;
    try
    {
      final WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmap");
      request.setParameter("LAYERS", imageStretch2Unqualified);
      request.setParameter("FORMAT", "image/png");
      request.setParameter("BBOX",
        "-112.53799295569956,35.768445274925874,-111.64052704430043,36.49839472507413");
      request.setParameter("WIDTH", "900");
      request.setParameter("HEIGHT", "700");

      response = webClient.getResponse(request);

      processImageResponse(response, "png");
    }
    catch (final Exception e)
    {
      e.printStackTrace();
      throw e;
    }
    finally
    {
      if (response != null)
      {
        response.close();
      }
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void testJpg3band() throws Exception
  {
    WebResponse response = null;
    try
    {
      final WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmap");
      request.setParameter("LAYERS", small3bandUnqualified);
      request.setParameter("FORMAT", "image/jpg");
      request.setParameter("BBOX",
        "8.200266813859766,54.86003345745267,8.482915124747016,55.0210075283041");
      request.setParameter("WIDTH", "800");
      request.setParameter("HEIGHT", "600");

      response = webClient.getResponse(request);

      processImageResponse(response, "jpg");
    }
    catch (final Exception e)
    {
      e.printStackTrace();
      throw e;
    }
    finally
    {
      if (response != null)
      {
        response.close();
      }
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void testPng3band() throws Exception
  {
    WebResponse response = null;
    try
    {
      final WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmap");
      request.setParameter("LAYERS", small3bandUnqualified);
      request.setParameter("FORMAT", "image/png");
      request.setParameter("BBOX",
        "8.200266813859766,54.86003345745267,8.482915124747016,55.0210075283041");
      request.setParameter("WIDTH", "800");
      request.setParameter("HEIGHT", "600");

      response = webClient.getResponse(request);

      processImageResponse(response, "png");
    }
    catch (final Exception e)
    {
      e.printStackTrace();
      throw e;
    }
    finally
    {
      if (response != null)
      {
        response.close();
      }
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void testTif3band() throws Exception
  {
    WebResponse response = null;
    try
    {
      final WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmap");
      request.setParameter("LAYERS", small3bandUnqualified);
      request.setParameter("FORMAT", "image/tiff");
      request.setParameter("BBOX",
        "8.200266813859766,54.86003345745267,8.482915124747016,55.0210075283041");
      request.setParameter("WIDTH", "800");
      request.setParameter("HEIGHT", "600");

      response = webClient.getResponse(request);

      processImageResponse(response, "tiff");
    }
    catch (final Exception e)
    {
      e.printStackTrace();
      throw e;
    }
    finally
    {
      if (response != null)
      {
        response.close();
      }
    }
  }

}
