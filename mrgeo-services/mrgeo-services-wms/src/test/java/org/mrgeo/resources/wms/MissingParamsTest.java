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
import com.sun.jersey.api.client.WebResource;
import junit.framework.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.junit.IntegrationTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("static-method")
public class MissingParamsTest extends WmsGeneratorTestAbstract
{
  @SuppressWarnings("unused")
  private static final Logger log = 
  LoggerFactory.getLogger(MissingParamsTest.class);

  @BeforeClass 
  public static void setUpForJUnit()
  {    
    try 
    {
      WmsGeneratorTestAbstract.setUpForJUnit();
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }

  private void testGetMapMissingParam(String paramName) throws Exception
  {
    WebResource webResource = resource().path("/wms")
            .queryParam("SERVICE", "WMS")
            .queryParam("REQUEST", "getmap");
    if (!paramName.equals("LAYERS"))
    {
      webResource = webResource.queryParam("LAYERS", "IslandsElevation-v2");
    }
    if (!paramName.equals("FORMAT"))
    {
      webResource = webResource.queryParam("FORMAT", "image/png");
    }
    if (!paramName.equals("BBOX"))
    {
      webResource = webResource.queryParam("BBOX", "160.312500,-11.250000,161.718750,-9.843750");
    }
    if (!paramName.equals("WIDTH"))
    {
      webResource = webResource.queryParam("WIDTH", "512");
    }
    if (!paramName.equals("HEIGHT"))
    {
      webResource = webResource.queryParam("HEIGHT", "512");
    }

    ClientResponse response = webResource.get(ClientResponse.class);
    Assert.assertNotNull(response);
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    String content = response.getEntity(String.class);
    assertTrue("Unexpected response: " + content,
               content.contains("<ServiceException><![CDATA[Missing required " + paramName + " parameter]]></ServiceException>"));
    response.close();
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
    WebResource webResource = resource().path("/wms")
            .queryParam("SERVICE", "WMS")
            .queryParam("REQUEST", "getmosaic");
    if (!paramName.equals("LAYERS"))
    {
      webResource = webResource.queryParam("LAYERS", "IslandsElevation-v2");
    }
    if (!paramName.equals("FORMAT"))
    {
      webResource = webResource.queryParam("FORMAT", "image/png");
    }
    if (!paramName.equals("BBOX"))
    {
      webResource = webResource.queryParam("BBOX", "160.312500,-11.250000,161.718750,-9.843750");
    }

    ClientResponse response = webResource.get(ClientResponse.class);
    Assert.assertNotNull(response);
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    String content = response.getEntity(String.class);
    assertTrue("Unexpected response: " + content,
               content.contains("<ServiceException><![CDATA[Missing required " + paramName + " parameter]]></ServiceException>"));
//    assertTrue("Unexpected response: " + content,
//               content.contains("<ServiceException code=\"Missing request parameter: " + paramName));
    response.close();
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
    WebResource webResource = resource().path("/wms")
            .queryParam("SERVICE", "WMS")
            .queryParam("REQUEST", "gettile");
    if (!paramName.equals("LAYER"))
    {
      webResource = webResource.queryParam("LAYER", "IslandsElevation-v2");
    }
    if (!paramName.equals("FORMAT"))
    {
      webResource = webResource.queryParam("FORMAT", "image/tif");
    }
    if (!paramName.equals("TILEROW"))
    {
      webResource = webResource.queryParam("TILEROW", "224");
    }
    if (!paramName.equals("TILECOL"))
    {
      webResource = webResource.queryParam("TILECOL", "970");
    }
    if (!paramName.equals("SCALE"))
    {
      webResource = webResource.queryParam("SCALE", "272989.38673277234");
    }

    ClientResponse response = webResource.get(ClientResponse.class);
    Assert.assertNotNull(response);

    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    String content = response.getEntity(String.class);
    assertTrue("Unexpected response: " + content,
               content.contains("<ServiceException><![CDATA[Missing required " + paramName + " parameter]]></ServiceException>"));
    response.close();
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
