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
import junit.framework.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.junit.IntegrationTest;
import org.mrgeo.test.TestUtils;
import org.mrgeo.utils.HadoopUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("static-method")
public class DescribeTilesTest extends WmsGeneratorTestAbstract
{
  @SuppressWarnings("unused")
  private static final Logger log = LoggerFactory.getLogger(DescribeTilesTest.class);

  @BeforeClass
  public static void setUpForJUnit()
  {
    try
    {
      baselineInput = TestUtils.composeInputDir(DescribeTilesTest.class);
      WmsGeneratorTestAbstract.setUpForJUnit();

      Properties mrgeoProperties = MrGeoProperties.getInstance();

      mrgeoProperties.put(MrGeoConstants.MRGEO_ENV_HOME, inputHdfs.toString());
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
    ClientResponse response = resource().path("/wms")
            .queryParam("SERVICE", "WMS")
            .queryParam("REQUEST", "describetiles")
            .get(ClientResponse.class);

    processTextResponse(response, "DescribeTiles.xml");
  }

  @Test
  @Category(IntegrationTest.class)
  public void testDescribeTilesLessThan140() throws Exception
  {
    ClientResponse response = resource().path("/wms")
            .queryParam("SERVICE", "WMS")
            .queryParam("REQUEST", "describetiles")
            .queryParam("VERSION", "1.3.0")
            .get(ClientResponse.class);

    Assert.assertNotNull(response);
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    String content = response.getEntity(String.class);
    assertTrue("Unexpected response: " + content,
               content.contains(
                       "<ServiceException><![CDATA[Describe tiles is only supported with version >= 1.4.0]]></ServiceException>")
    );
    response.close();
  }

  @Test
  @Category(IntegrationTest.class)
  public void testDescribeTiles140() throws Exception
  {
    ClientResponse response = resource().path("/wms")
            .queryParam("SERVICE", "WMS")
            .queryParam("REQUEST", "describetiles")
            .queryParam("VERSION", "1.4.0")
            .get(ClientResponse.class);
    processTextResponse(response, "DescribeTiles.xml");
  }
}
