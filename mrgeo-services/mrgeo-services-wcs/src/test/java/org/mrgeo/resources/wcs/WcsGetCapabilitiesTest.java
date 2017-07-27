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
package org.mrgeo.resources.wcs;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.junit.IntegrationTest;
import org.mrgeo.test.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;

@SuppressWarnings("all") // Test code, not included in production
public class WcsGetCapabilitiesTest extends WcsGeneratorTestAbstract
{
@SuppressWarnings("unused")
private static final Logger log = LoggerFactory.getLogger(WcsGetCapabilitiesTest.class);

@BeforeClass
public static void setUpForJUnit()
{
  try
  {
    baselineInput = TestUtils.composeInputDir(WcsGetCapabilitiesTest.class);
    WcsGeneratorTestAbstract.setUpForJUnit();
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
  Response response = target("wcs")
      .queryParam("SERVICE", "WCS")
      .request().get();

  processXMLResponse(response, "GetCapabilities-1-1-0.xml");
}
@Test
@Category(IntegrationTest.class)
public void testGetCapabilitiesEmptyVersion() throws Exception
{
  Response response = target("wcs")
      .queryParam("SERVICE", "WCS")
      .queryParam("REQUEST", "getcapabilities")
      .request().get();

  processXMLResponse(response, "GetCapabilities-1-1-0.xml");
}

@Test
@Category(IntegrationTest.class)
public void testGetCapabilities100() throws Exception
{
  Response response = target("wcs")
      .queryParam("SERVICE", "WCS")
      .queryParam("REQUEST", "getcapabilities")
      .queryParam("ACCEPTVERSIONS", "1.0.0")
      .request().get();

  processXMLResponse(response, "GetCapabilities-1-0-0.xml");
}

@Test
@Category(IntegrationTest.class)
public void testGetCapabilities110() throws Exception
{
  Response response = target("wcs")
      .queryParam("SERVICE", "WCS")
      .queryParam("REQUEST", "getcapabilities")
      .queryParam("ACCEPTVERSIONS", "1.1.0")
      .request().get();

  processXMLResponse(response, "GetCapabilities-1-1-0.xml");
}

@Test
@Category(IntegrationTest.class)
public void testGetCapabilities100_110() throws Exception
{
  Response response = target("wcs")
      .queryParam("SERVICE", "WCS")
      .queryParam("REQUEST", "getcapabilities")
      .queryParam("ACCEPTVERSIONS", "1.0.0, 1.1.0")
      .request().get();

  processXMLResponse(response, "GetCapabilities-1-1-0.xml");
}


@Test
@Category(IntegrationTest.class)
public void testGetCapabilitiesLessThan100() throws Exception
{
  Response response = target("wcs")
      .queryParam("SERVICE", "WCS")
      .queryParam("REQUEST", "getcapabilities")
      .queryParam("ACCEPTVERSIONS", "0.9.9")
      .request().get();

  processXMLResponse(response, "GetCapabilities-1-0-0.xml");
}


@Test
@Category(IntegrationTest.class)
public void testGetCapabilitiesGreaterThan110() throws Exception
{
  Response response = target("wcs")
      .queryParam("SERVICE", "WCS")
      .queryParam("REQUEST", "getcapabilities")
      .queryParam("ACCEPTVERSIONS", "1.2.3")
      .request().get();

  processXMLResponse(response, "GetCapabilities-1-1-0.xml");
}

@Test
@Category(IntegrationTest.class)
public void testGetCapabilitiesProxy() throws Exception
{
  Response response = target("wcs")
      .queryParam("SERVICE", "WCS")
      .queryParam("REQUEST", "getcapabilities")
      .queryParam("ACCEPTVERSIONS", "1.1.0")
      .request()
      .header("X-Forwarded-Host", "www.foo.bar")
      .get();

  processXMLResponse(response, "GetCapabilities-1-1-0-Proxy.xml");
}

@Test
@Category(IntegrationTest.class)
public void testGetCapabilitiesProxyLowercase() throws Exception
{
  Response response = target("wcs")
      .queryParam("SERVICE", "WCS")
      .queryParam("REQUEST", "getcapabilities")
      .queryParam("ACCEPTVERSIONS", "1.1.0")
      .request()
      .header("x-forwarded-host", "www.foo.bar")
      .get();

  processXMLResponse(response, "GetCapabilities-1-1-0-Proxy.xml");
}

@Test
@Category(IntegrationTest.class)
public void testGetCapabilitiesProxyPort() throws Exception
{
  Response response = target("wcs")
      .queryParam("SERVICE", "WCS")
      .queryParam("REQUEST", "getcapabilities")
      .queryParam("ACCEPTVERSIONS", "1.1.0")
      .request()
      .header("X-Forwarded-Host", "www.foo.bar:1234")
      .get();

  processXMLResponse(response, "GetCapabilities-1-1-0-ProxyPort.xml");
}

}