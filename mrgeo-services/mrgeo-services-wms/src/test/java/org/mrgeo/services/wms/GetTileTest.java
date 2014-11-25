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
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.junit.IntegrationTest;
import org.mrgeo.test.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("static-method")
public class GetTileTest extends WmsGeneratorTestAbstract
{
  @SuppressWarnings("unused")
  private static final Logger log = LoggerFactory.getLogger(GetTileTest.class);

  @BeforeClass 
  public static void setUp()
  {    
    try 
    {
      baselineInput = TestUtils.composeInputDir(GetTileTest.class);
      WmsGeneratorTestAbstract.setUp();
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }

  /*
   * WmsGenerator doesn't support more than one layer per request.
   */
  @Test 
  @Category(IntegrationTest.class)  
  public void testGetTileMultipleRequestLayers() throws Exception
  {
    WebResponse response = null;
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "gettile");
      request.setParameter("LAYER", "IslandsElevation-v2,IslandsElevation-v3");
      request.setParameter("FORMAT", "image/tiff");
      request.setParameter("TILEROW", "224");
      request.setParameter("TILECOL", "970");
      request.setParameter("SCALE", "0.0027465820");

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
                "Unable to open pyramid: IslandsElevation-v2,IslandsElevation-v3"));
        response.close();
    }
  }

  @Test 
  @Category(IntegrationTest.class)  
  public void testGetTileInvalidFormat() throws Exception
  {
    WebResponse response = null;
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "gettile");
      request.setParameter("LAYER", "IslandsElevation-v2");
      request.setParameter("FORMAT", "image/abc");
      request.setParameter("TILEROW", "224");
      request.setParameter("TILECOL", "970");
      request.setParameter("SCALE", "0.0027465820");

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
                "Unsupported image format - image/abc"));
        response.close();
    }
  }

  @Test 
  @Category(IntegrationTest.class)  
  public void testGetTileInvalidLayer() throws Exception
  {
    WebResponse response = null;
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "gettile");
      request.setParameter("LAYER", "IslandsElevation-v3");
      request.setParameter("FORMAT", "image/tiff");
      request.setParameter("TILEROW", "224");
      request.setParameter("TILECOL", "970");
      request.setParameter("SCALE", "0.0027465820");

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
                "Unable to open pyramid: IslandsElevation-v3"));
        response.close();
    }
  }

  @Test 
  @Category(IntegrationTest.class)  
  public void testGetTileOutOfBoundsPng() throws Exception
  {
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "gettile");
      request.setParameter("LAYER", "IslandsElevation-v2");
      request.setParameter("FORMAT", "image/png");
      request.setParameter("TILEROW", "1");
      request.setParameter("TILECOL", "1");
      request.setParameter("SCALE", "0.0027465820");

      WebResponse response = webClient.getResponse(request);
      Assert.assertEquals(response.getResponseCode(), 200);
      assertTrue(
          response.getText().contains("Tile x/y out of range. (1, 1) range: (243, 56) to (243, 56) (inclusive)"));
        response.close();
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }

  @Test 
  @Category(IntegrationTest.class)  
  public void testGetTilePng() throws Exception
  {
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "gettile");
      request.setParameter("LAYER", "IslandsElevation-v2");
      request.setParameter("FORMAT", "image/png");
      request.setParameter("TILEROW", "56");
      request.setParameter("TILECOL", "242");
      request.setParameter("SCALE", "0.0027465820");  //zoom level 8

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
  public void testGetTileLowerCaseParams() throws Exception
  {
    try
    {
      WebRequest request = createRequest();
      request.setParameter("request", "gettile");
      request.setParameter("layer", "IslandsElevation-v2");
      request.setParameter("format", "image/png");
      request.setParameter("tilerow", "56");
      request.setParameter("tilecol", "242");
      request.setParameter("scale", "0.0027465820");  //zoom level 8

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
  public void testGetTileFullLayerPath() throws Exception
  {
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "gettile");
      request.setParameter("LAYER", 
          "/mrgeo/test-files/org.mrgeo.services.wms/WmsGeneratorTestAbstract/IslandsElevation-v2");
      request.setParameter("FORMAT", "image/png");
      request.setParameter("TILEROW", "56");
      request.setParameter("TILECOL", "242");
      request.setParameter("SCALE", "0.0027465820");  //zoom level 8

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
  public void testGetTileOutOfBoundsJpg() throws Exception
  {
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "gettile");
      request.setParameter("LAYER", "IslandsElevation-v2");
      request.setParameter("FORMAT", "image/jpg");
      request.setParameter("TILEROW", "1");
      request.setParameter("TILECOL", "1");
      request.setParameter("SCALE", "0.0027465820");

      WebResponse response = webClient.getResponse(request);
      Assert.assertEquals(response.getResponseCode(), 200);
      assertTrue(
          response.getText().contains("Tile x/y out of range. (1, 1) range: (243, 56) to (243, 56) (inclusive)"));
        response.close();
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }

  @Test 
  @Category(IntegrationTest.class)  
  public void testGetTileJpg() throws Exception
  {
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "gettile");
      request.setParameter("LAYER", "IslandsElevation-v2");
      request.setParameter("FORMAT", "image/jpg");
      request.setParameter("TILEROW", "56");
      request.setParameter("TILECOL", "242");
      request.setParameter("SCALE", "0.0027465820");  //zoom level 8

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
  public void testGetTileOutOfBoundsTif() throws Exception
  {
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "gettile");
      request.setParameter("LAYER", "IslandsElevation-v2");
      request.setParameter("FORMAT", "image/tiff");
      request.setParameter("TILEROW", "1");
      request.setParameter("TILECOL", "1");
      request.setParameter("SCALE", "0.0027465820");

      WebResponse response = webClient.getResponse(request);
      Assert.assertEquals(response.getResponseCode(), 200);
      assertTrue(
          response.getText().contains("Tile x/y out of range. (1, 1) range: (243, 56) to (243, 56) (inclusive)"));
        response.close();
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }

  @Test 
  @Category(IntegrationTest.class)  
  public void testGetTileTif() throws Exception
  {
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "gettile");
      request.setParameter("LAYER", "IslandsElevation-v2");
      request.setParameter("FORMAT", "image/tif");
      request.setParameter("TILEROW", "56");
      request.setParameter("TILECOL", "242");
      request.setParameter("SCALE", "0.0027465820");  //zoom level 8

      processImageResponse(webClient.getResponse(request), "tif");
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }
}
