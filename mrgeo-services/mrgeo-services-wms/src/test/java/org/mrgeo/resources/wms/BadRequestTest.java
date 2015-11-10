/*
 * Copyright 2009-2015 DigitalGlobe, Inc.
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
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.junit.IntegrationTest;
import org.mrgeo.test.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("static-method")
public class BadRequestTest extends WmsGeneratorTestAbstract
{
  @SuppressWarnings("unused")
  private static final Logger log = LoggerFactory.getLogger(BadRequestTest.class);

  @BeforeClass 
  public static void setUpForJUnit()
  {    
    try 
    {
      baselineInput = TestUtils.composeInputDir(BadRequestTest.class);
      WmsGeneratorTestAbstract.setUpForJUnit();
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void testMissingServiceParameter() throws Exception
  {
    ClientResponse response = resource()
            .path("/wms")
            .get(ClientResponse.class);

    processXMLResponse(response, "wms-missing-serviceparameter.xml", Response.Status.BAD_REQUEST);

  }

  @Test
  @Category(IntegrationTest.class)
  public void testInvalidRequestType() throws Exception
  {
    WebResource wr = resource();
    ClientResponse response = resource()
            .path("/wms")
            .queryParam("SERVICE", "WMS")
            .queryParam("REQUEST", "blah")
            .get(ClientResponse.class);

    processXMLResponse(response, "wms-invalid-requesttype.xml", Response.Status.BAD_REQUEST);
  }

  @Test 
  @Category(IntegrationTest.class)  
  public void testInvalidServiceType() throws Exception
  {
    WebResource wr = resource();
    ClientResponse response = resource()
            .path("/wms")
            .queryParam("SERVICE", "WFS")
            .get(ClientResponse.class);

    processXMLResponse(response, "wms-invalid-servicetype.xml", Response.Status.BAD_REQUEST);
  }

  /*
   * The WMS only supports WGS84.  Requests not specifying a CRS will be defaulted to WGS84.
   */
  @Test 
  @Category(IntegrationTest.class)  
  public void testInvalidCoordSys() throws Exception
  {
    ClientResponse response = resource()
            .path("/wms")
            .queryParam("SERVICE", "WMS")
            .queryParam("request", "getmap")
            .queryParam("LAYERS",
                        "/mrgeo/test-files/org.mrgeo.resources.wms/WmsGeneratorTestAbstract/IslandsElevation-v2")
            .queryParam("FORMAT", "image/png")
            .queryParam("BBOX", "160.312500,-11.250000,161.718750,-9.843750")
            .queryParam("CRS", "CRS:85")
            .queryParam("WIDTH", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
            .queryParam("HEIGHT", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
            .get(ClientResponse.class);

    processXMLResponse(response, "wms-invalid-coordsys.xml", Response.Status.BAD_REQUEST);
  }
}
