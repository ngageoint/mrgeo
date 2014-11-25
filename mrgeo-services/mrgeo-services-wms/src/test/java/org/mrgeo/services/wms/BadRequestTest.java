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
public class BadRequestTest extends WmsGeneratorTestAbstract
{
  @SuppressWarnings("unused")
  private static final Logger log = LoggerFactory.getLogger(BadRequestTest.class);

  @BeforeClass 
  public static void setUp()
  {    
    try 
    {
      baselineInput = TestUtils.composeInputDir(BadRequestTest.class);
      WmsGeneratorTestAbstract.setUp();
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }

  @Test 
  @Category(IntegrationTest.class)  
  public void testInvalidRequestType() throws Exception
  {
    WebResponse response = null;
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "blah");
      response = webClient.getResponse(request);
    }
    finally
    {
      Assert.assertNotNull(response);

      assertEquals(response.getResponseCode(), 200);
      assertTrue(
        response.getText().contains("<ServiceException code=\"Invalid request type made"));
      response.close();
    }
  }

  @Test 
  @Category(IntegrationTest.class)  
  public void testInvalidServiceType() throws Exception
  {
    WebResponse response = null;
    try
    {
      WebRequest request = createRequest();
      request.setParameter("SERVICE", "WFS");

      response = webClient.getResponse(request);
    }
    finally
    {
      Assert.assertNotNull(response);
      assertEquals(response.getResponseCode(), 200);
      assertTrue(
        response.getText().contains("Invalid service type was requested. (only WMS is supported"));
      response.close();
    }
  }

  /*
   * The WMS only supports WGS84.  Requests not specifying a CRS will be defaulted to WGS84.
   */
  @Test 
  @Category(IntegrationTest.class)  
  public void testInvalidCoordSys() throws Exception
  {
    WebResponse response = null;
    try
    {
      WebRequest request = createRequest();
      request.setParameter("request", "getmap");
      request.setParameter(
        "LAYERS", "/mrgeo/test-files/org.mrgeo.services.wms/WmsGeneratorTestAbstract/IslandsElevation-v2");
      request.setParameter("FORMAT", "image/png");
      request.setParameter("BBOX", "160.312500,-11.250000,161.718750,-9.843750");
      request.setParameter("CRS", "CRS:85");
      request.setParameter("WIDTH", "512");
      request.setParameter("HEIGHT", "512");

      response = webClient.getResponse(request);
    }
    finally
    {
      Assert.assertNotNull(response);

      assertEquals(response.getResponseCode(), 200);
      assertTrue(response.getText().contains("<ServiceException code=\"InvalidCRS"));
      response.close();
    }
  }
}
