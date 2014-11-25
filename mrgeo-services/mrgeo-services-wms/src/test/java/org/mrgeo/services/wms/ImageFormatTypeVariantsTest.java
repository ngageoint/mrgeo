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
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.junit.IntegrationTest;
import org.mrgeo.test.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@SuppressWarnings("static-method")
public class ImageFormatTypeVariantsTest extends WmsGeneratorTestAbstract
{
  private static final Logger log = LoggerFactory.getLogger(ImageFormatTypeVariantsTest.class);
  
  @BeforeClass 
  public static void setUp()
  {    
    try 
    {
      baselineInput = TestUtils.composeInputDir(ImageFormatTypeVariantsTest.class);
      WmsGeneratorTestAbstract.setUp();
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }
  
  private static WebResponse testGetMapWithVariant(String format) throws IOException, SAXException
  {
    WebRequest request = createRequest();
    request.setParameter("REQUEST", "getmap");
    request.setParameter("LAYERS", "IslandsElevation-v2");
    request.setParameter("FORMAT", format);
    request.setParameter("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE);
    request.setParameter("WIDTH", "512");
    request.setParameter("HEIGHT", "512");
    return webClient.getResponse(request); 
  }
  
  /*
   * These are variants from the standard mime types we're allowing to be flexible.  Its debatable
   * how useful this test is, since it requires maintenance every time a new image format is added.
   * Often, non-standard mime types are passed into WMS requests, so keeping this test for now.  
   * Here only GetMap requests are being tested since the image format parsing code path is the 
   * same for all WMS request types.  Also, only a non-zero response length is checked for here, 
   * rather than a full image comparison, since that coverage is in other tests.
   */
  @Test 
  @Category(IntegrationTest.class)  
  public void testVariants() throws Exception
  {
    WebResponse response  = null;
    try
    {
      //correct format is "image/png"
      String[] pngVariants = new String[]{ "PNG", "png", "IMAGE/PNG", "image/PNG" };
      for (int i = 0; i < pngVariants.length; i++)
      {
        log.info("Checking image format: " + pngVariants[i] + " ...");
        try
        {
          response = testGetMapWithVariant(pngVariants[i]);
        }
        finally
        {
          Assert.assertNotNull(response);
          assertEquals(response.getResponseCode(), 200);
          assertFalse(response.getText().contains("Invalid format"));
          response.close();
        }
      }
      //correct format is "image/jpeg"
      String[] jpgVariants = new String[]{ "jpeg", "jpg", "JPG", "JPEG", "image/jpg", "IMAGE/JPG", 
        "image/JPG", "IMAGE/JPEG", "image/JPEG" };
      for (int i = 0; i < jpgVariants.length; i++)
      {
        log.info("Checking image format: " + jpgVariants[i] + " ...");
        try
        {
          response = testGetMapWithVariant(jpgVariants[i]);
        }
        finally
        {
          Assert.assertNotNull(response);
          assertEquals(response.getResponseCode(), 200);
          assertFalse(response.getText().contains("Invalid format"));
          response.close();
        }
      }
      
      //correct format is "image/tiff"
      String[] tifVariants = new String[]{ "tiff", "tif", "TIF", "TIFF", "image/tif", "IMAGE/TIF", 
        "image/TIF", "IMAGE/TIFF", "image/TIFF" };
      for (int i = 0; i < tifVariants.length; i++)
      {
        log.info("Checking image format: " + tifVariants[i] + " ...");
        try
        {
          response = testGetMapWithVariant(tifVariants[i]);
        }
        finally
        {
          Assert.assertNotNull(response);
          assertEquals(response.getResponseCode(), 200);
          assertFalse(response.getText().contains("Invalid format"));
          response.close();
        }
      }
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }
}
