/*
 * Copyright 2009-2016 DigitalGlobe, Inc.
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
 *
 */
package org.mrgeo.resources.wms;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.junit.IntegrationTest;
import org.mrgeo.test.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;

/*
 * The WmsGenerator originally had some issues with requests that spanned more than one source
 * tile, therefore to be safe, GetMap/GetMosaic tests are done both for types of requests.
 */
@SuppressWarnings("all") // Test code, not included in production
public class GetCapabilitiesTest extends WmsGeneratorTestAbstract
{
@SuppressWarnings("unused")
private static final Logger log = LoggerFactory.getLogger(GetCapabilitiesTest.class);

@BeforeClass
public static void setUpForJUnit()
{
  try
  {
    baselineInput = TestUtils.composeInputDir(GetCapabilitiesTest.class);
    WmsGeneratorTestAbstract.setUpForJUnit();
  }
  catch (Exception e)
  {
    e.printStackTrace();
  }
}

/*
 * defaults to GetCapabilities request when no request type is specified
 */
@Test
@Category(IntegrationTest.class)
public void testEmptyRequestType() throws Exception
{
  Response response = target("wms")
      .queryParam("SERVICE", "WMS")
      .request().get();

  processXMLResponse(response, "GetCapabilities-1-1-1-EmptyRequest.xml");
}

  /*
   * WmsGenerator supports capabilities for versions 1.1.1, 1.3.0, and 1.4.0.  If no version is
   * specified, the default version 1.1.1 is assigned.  If a version less than 1.1.1, greater than
   * 1.4.0, or one in between the three supported version is specified, then the closest lower
   * version number is automatically assigned.
   */

@Test
@Category(IntegrationTest.class)
public void testGetCapabilitiesEmptyVersion() throws Exception
{
  Response response = target("wms")
      .queryParam("SERVICE", "WMS")
      .queryParam("REQUEST", "getcapabilities")
      .request().get();

  processXMLResponse(response, "GetCapabilities-1-1-1-EmptyVersion.xml");
}

@Test
@Category(IntegrationTest.class)
public void testGetCapabilities111() throws Exception
{
  Response response = target("wms")
      .queryParam("SERVICE", "WMS")
      .queryParam("REQUEST", "getcapabilities")
      .queryParam("version", "1.1.1")
      .request().get();

  processXMLResponse(response, "GetCapabilities-1-1-1.xml");
}

@Test
@Category(IntegrationTest.class)
public void testGetCapabilities130() throws Exception
{
  Response response = target("wms")
      .queryParam("SERVICE", "WMS")
      .queryParam("REQUEST", "getcapabilities")
      .queryParam("VERSION", "1.3.0")
      .request().get();

  processXMLResponse(response, "GetCapabilities-1-3-0.xml");
}

@Test
@Category(IntegrationTest.class)
public void testGetCapabilities140() throws Exception
{
  Response response = target("wms")
      .queryParam("SERVICE", "WMS")
      .queryParam("REQUEST", "getcapabilities")
      .queryParam("VERSION", "1.4.0")
      .request().get();

  processXMLResponse(response, "GetCapabilities-1-4-0.xml");
}

@Test
@Category(IntegrationTest.class)
public void testGetCapabilitiesLessThan111() throws Exception
{
  Response response = target("wms")
      .queryParam("SERVICE", "WMS")
      .queryParam("REQUEST", "getcapabilities")
      .queryParam("version", "0.9.9")
      .request().get();

  processXMLResponse(response, "GetCapabilities-1-1-1.xml");
}

@Test
@Category(IntegrationTest.class)
public void testGetCapabilitiesLessThan130() throws Exception
{
  Response response = target("wms")
      .queryParam("SERVICE", "WMS")
      .queryParam("REQUEST", "getcapabilities")
      .queryParam("version", "1.2.9")
      .request().get();

  processXMLResponse(response, "GetCapabilities-1-1-1.xml");
}

@Test
@Category(IntegrationTest.class)
public void testGetCapabilitiesLessThan140() throws Exception
{
  Response response = target("wms")
      .queryParam("SERVICE", "WMS")
      .queryParam("REQUEST", "getcapabilities")
      .queryParam("VERSION", "1.3.9")
      .request().get();

  processXMLResponse(response, "GetCapabilities-1-3-0.xml");
}

@Test
@Category(IntegrationTest.class)
public void testGetCapabilitiesGreaterThan140() throws Exception
{
  Response response = target("wms")
      .queryParam("SERVICE", "WMS")
      .queryParam("REQUEST", "getcapabilities")
      .queryParam("VERSION", "1.4.1")
      .request().get();

  processXMLResponse(response, "GetCapabilities-1-4-0.xml");
}
}