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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("static-method")
public class MissingParamsTest extends WmsGeneratorTestAbstract
{
  @SuppressWarnings("unused")
  private static final Logger log = 
  LoggerFactory.getLogger(MissingParamsTest.class);

  @BeforeClass 
  public static void setUp()
  {    
    try 
    {
      WmsGeneratorTestAbstract.setUp();
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }

  private void testGetMapMissingParam(String paramName) throws Exception
  {
    WebResponse response = null;
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmap");
      if (!paramName.equals("LAYERS"))
      {
        request.setParameter("LAYERS", "IslandsElevation-v2");
      }
      if (!paramName.equals("FORMAT"))
      {
        request.setParameter("FORMAT", "image/png");
      }
      if (!paramName.equals("BBOX"))
      {
        request.setParameter("BBOX", "160.312500,-11.250000,161.718750,-9.843750");
      }
      if (!paramName.equals("WIDTH"))
      {
        request.setParameter("WIDTH", "512");
      }
      if (!paramName.equals("HEIGHT"))
      {
        request.setParameter("HEIGHT", "512");
      }

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
          "<ServiceException code=\"Missing request parameter: " + paramName.toLowerCase()));
      response.close();
    }
  }

  @Test 
  @Category(IntegrationTest.class)  
  public void testGetMapMissingParams() throws Exception
  {
    String[] paramNames = 
        new String[]
            {
      "FORMAT",
      "BBOX",
      "LAYERS",
      "WIDTH",
      "HEIGHT"
            };
    for (int i = 0; i < paramNames.length; i++)
    {
      testGetMapMissingParam(paramNames[i]);
    }
  }

  private void testGetMosaicMissingParam(String paramName) throws Exception
  {
    WebResponse response = null;
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmosaic");
      if (!paramName.equals("LAYERS"))
      {
        request.setParameter("LAYERS", "IslandsElevation-v2");
      }
      if (!paramName.equals("FORMAT"))
      {
        request.setParameter("FORMAT", "image/png");
      }
      if (!paramName.equals("BBOX"))
      {
        request.setParameter("BBOX", "160.312500,-11.250000,161.718750,-9.843750");
      }

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
          "<ServiceException code=\"Missing request parameter: " + paramName.toLowerCase()));
      response.close();
    }
  }

  @Test 
  @Category(IntegrationTest.class)  
  public void testGetMosaicMissingParams() throws Exception
  {
    String[] paramNames = 
        new String[]
            {
      "FORMAT",
      "BBOX",
      "LAYERS"
            };
    for (int i = 0; i < paramNames.length; i++)
    {
      testGetMosaicMissingParam(paramNames[i]);
    }
  }

  private void testGetTileMissingParam(String paramName) throws Exception
  {
    WebResponse response = null;
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "gettile");
      if (!paramName.equals("LAYER"))
      {
        request.setParameter("LAYER", "IslandsElevation-v2");
      }
      if (!paramName.equals("FORMAT"))
      {
        request.setParameter("FORMAT", "image/tif");
      }
      if (!paramName.equals("TILEROW"))
      {
        request.setParameter("TILEROW", "224");
      }
      if (!paramName.equals("TILECOL"))
      {
        request.setParameter("TILECOL", "970");
      }
      if (!paramName.equals("SCALE"))
      {
        request.setParameter("SCALE", "272989.38673277234");
      }

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
          "<ServiceException code=\"Missing request parameter: " + paramName.toLowerCase()));
      response.close();
    }
  }

  @Test 
  @Category(IntegrationTest.class)  
  public void testGetTileMissingParams() throws Exception
  {
    String[] paramNames = 
        new String[]
            {
      "FORMAT",
      "LAYER",
      "TILEROW",
      "TILECOL",
      "SCALE"
            };
    for (int i = 0; i < paramNames.length; i++)
    {
      testGetTileMissingParam(paramNames[i]);
    }
  }
}
