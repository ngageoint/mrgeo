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
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.junit.IntegrationTest;
import org.mrgeo.test.TestUtils;
import org.mrgeo.utils.HadoopUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("static-method")
public class DescribeTilesTest extends WmsGeneratorTestAbstract
{
  @SuppressWarnings("unused")
  private static final Logger log = LoggerFactory.getLogger(DescribeTilesTest.class);

  @BeforeClass
  public static void setUp()
  {
    try
    {
      baselineInput = TestUtils.composeInputDir(DescribeTilesTest.class);
      WmsGeneratorTestAbstract.setUp();

      Properties mrgeoProperties = MrGeoProperties.getInstance();

      mrgeoProperties.put("MRGEO_HOME", inputHdfs.toString());
      mrgeoProperties.put(HadoopUtils.IMAGE_BASE, inputHdfs.toString());
      mrgeoProperties.put(HadoopUtils.COLOR_SCALE_BASE, inputHdfs.toString());

//    WmsGenerator.setBasePath(inputHdfs);
//    WmsGenerator.setColorScaleBasePath(inputHdfs);

    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }

  /*
   * WmsGenerator only supports describing tiles as a proposed extension in version 1.4.0.  If no 
   * version is specified, then version 1.4.0 is automatically assigned.
   */

  @Test
  @Category(IntegrationTest.class)
  public void testDescribeTilesEmptyVersion() throws Exception
  {
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "describetiles");

      processTextResponse(webClient.getResponse(request), "DescribeTiles.xml");
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void testDescribeTilesLessThan140() throws Exception
  {
    WebResponse response = null;
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "describetiles");
      request.setParameter("VERSION", "1.3.0");

      response = webClient.getResponse(request);
    }
    finally
    {
      Assert.assertNotNull(response);
      assertEquals(response.getResponseCode(), 200);
      assertTrue(
          response.getText().contains(
              "<ServiceException code=\"Describe tiles is only supported with version &gt;= 1.4.0\"")
      );
      response.close();
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void testDescribeTiles140() throws Exception
  {
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "describetiles");
      request.setParameter("VERSION", "1.4.0");

      processTextResponse(webClient.getResponse(request), "DescribeTiles.xml");
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }
}
