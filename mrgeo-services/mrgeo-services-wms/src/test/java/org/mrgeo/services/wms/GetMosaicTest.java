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
import junit.framework.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.junit.IntegrationTest;
import org.mrgeo.test.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("static-method")
public class GetMosaicTest extends WmsGeneratorTestAbstract
{
  @SuppressWarnings("unused")
  private static final Logger log = LoggerFactory.getLogger(GetMosaicTest.class);

  @BeforeClass 
  public static void setUp()
  {    
    try 
    {
      baselineInput = TestUtils.composeInputDir(GetMosaicTest.class);
      WmsGeneratorTestAbstract.setUp();
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }
  
  @Override
  @Before
  public void init()
  {
    DataProviderFactory.invalidateCache();
  }

  /*
   * WmsGenerator doesn't support more than one layer per request.
   */
  @Test 
  @Category(IntegrationTest.class)  
  public void testGetMosaicMultipleRequestLayers() throws Exception
  {
    WebResponse response = null;
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmosaic");
      request.setParameter("LAYERS", "IslandsElevation-v2,IslandsElevation-v3");
      request.setParameter("FORMAT", "image/png");
      request.setParameter("BBOX", "160.312500,-11.250000,161.718750,-9.843750");

      response = webClient.getResponse(request);
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
    finally
    {
      Assert.assertNotNull(response);
        assertEquals(response.getResponseCode(), 200);
        assertTrue(
            response.getText().contains(
                "Unable to find a MrsImage data provider for IslandsElevation-v2,IslandsElevation-v3"));
        response.close();
    }
  }

  @Test 
  @Category(IntegrationTest.class)  
  public void testGetMosaicInvalidFormat() throws Exception
  {
    WebResponse response = null;
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmosaic");
      request.setParameter("LAYERS", "IslandsElevation-v2");
      request.setParameter("FORMAT", "image/abc");
      request.setParameter("BBOX", "160.312500,-11.250000,161.718750,-9.843750");

      response = webClient.getResponse(request);
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
    finally
    {
      Assert.assertNotNull(response);
        assertEquals(response.getResponseCode(), 200);
        assertTrue(
            response.getText().contains(
                "<ServiceException code=\"Unsupported image format - image/abc"));
        response.close();

    }
  }

  @Test 
  @Category(IntegrationTest.class)  
  public void testGetMosaicInvalidLayer() throws Exception
  {
    WebResponse response = null;
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmosaic");
      request.setParameter("LAYERS", "IslandsElevation-v3");
      request.setParameter("FORMAT", "image/png");
      request.setParameter("BBOX", "160.312500,-11.250000,161.718750,-9.843750");

      response = webClient.getResponse(request);
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
    finally
    {
      Assert.assertNotNull(response);
        assertEquals(response.getResponseCode(), 200);
        String text = response.getText();
        assertTrue(
            response.getText().contains(
              "Unable to find a MrsImage data provider for IslandsElevation-v3"));
              response.close();
    }
  }

  /*
   * PNG out of bounds requests should return a transparent image.
   */
  @Test 
  @Category(IntegrationTest.class)  
  public void testGetMosaicOutOfBoundsPng() throws Exception
  {
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmosaic");
      request.setParameter("LAYERS", "IslandsElevation-v2");
      request.setParameter("FORMAT", "image/png");
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_OUT_OF_BOUNDS);

      processImageResponse(webClient.getResponse(request), "png");
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }

  /*
   * JPG doesn't support transparency, so a black image is returned for out of bounds requests.
   */
  @Test 
  @Category(IntegrationTest.class)  
  public void testGetMosaicOutOfBoundsJpg() throws Exception
  {
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmosaic");
      request.setParameter("LAYERS", "IslandsElevation-v2");
      request.setParameter("FORMAT", "image/jpg");
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_OUT_OF_BOUNDS);

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
  public void testGetMosaicOutOfBoundsTif() throws Exception
  {
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmosaic");
      request.setParameter("LAYERS", "IslandsElevation-v2");
      request.setParameter("FORMAT", "image/tif");
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_OUT_OF_BOUNDS);

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
  public void testGetMosaicPngSingleSourceTile() throws Exception
  {
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmosaic");
      request.setParameter("LAYERS", "IslandsElevation-v2");
      request.setParameter("FORMAT", "image/png");
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
  public void testGetMosaicLowerCaseParams() throws Exception
  {
    try
    {
      WebRequest request = createRequest();
      request.setParameter("request", "getmosaic");
      request.setParameter("layers", "IslandsElevation-v2");
      request.setParameter("format", "image/png");
      request.setParameter("bbox", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE);

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
  public void testGetMosaicFullLayerPath() throws Exception
  {
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmosaic");
      request.setParameter(
          "LAYERS", 
          "/mrgeo/test-files/org.mrgeo.services.wms/WmsGeneratorTestAbstract/IslandsElevation-v2");
      request.setParameter("FORMAT", "image/png");
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
  public void testGetMosaicPngMultipleSourceTiles() throws Exception
  {
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmosaic");
      request.setParameter("LAYERS", "IslandsElevation-v2");
      request.setParameter("FORMAT", "image/png");
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_MULTIPLE_SOURCE_TILES);

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
  public void testGetMosaicJpgSingleSourceTile() throws Exception
  {
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmosaic");
      request.setParameter("LAYERS", "IslandsElevation-v2");
      request.setParameter("FORMAT", "image/jpeg");
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
  public void testGetMosaicJpgMultipleSourceTiles() throws Exception
  {
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmosaic");
      request.setParameter("LAYERS", "IslandsElevation-v2");
      request.setParameter("FORMAT", "image/jpeg");
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_MULTIPLE_SOURCE_TILES);

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
  public void testGetMosaicTifSingleSourceTile() throws Exception
  {
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmosaic");
      request.setParameter("LAYERS", "IslandsElevation-v2");
      request.setParameter("FORMAT", "image/tif");
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE);

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
  public void testGetMosaicTifMultipleSourceTiles() throws Exception
  {
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmosaic");
      request.setParameter("LAYERS", "IslandsElevation-v2");
      request.setParameter("FORMAT", "image/tif");
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_MULTIPLE_SOURCE_TILES);

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
  public void testGetMosaicJpgNonExistingZoomLevelBelowWithPyramids() throws Exception
  {
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmosaic");
      request.setParameter("LAYERS", "IslandsElevation-v2");
      request.setParameter("FORMAT", "image/jpeg");
      //pyramid only goes up to zoom level = 10; pass in zoom level = 11
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
  public void testGetMosaicTifNonExistingZoomLevelBelowWithPyramids() throws Exception
  {
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmosaic");
      request.setParameter("LAYERS", "IslandsElevation-v2");
      request.setParameter("FORMAT", "image/tiff");
      //pyramid only goes up to zoom level = 10; pass in zoom level = 11
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_PAST_HIGHEST_RES_ZOOM_LEVEL);

      processImageResponse(webClient.getResponse(request), "tif");
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }
}
