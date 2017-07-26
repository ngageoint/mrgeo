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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.junit.IntegrationTest;
import org.mrgeo.test.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import java.util.Properties;

@SuppressWarnings("all") // Test code, not included in production
public class WcsDescribeCoverageTest extends WcsGeneratorTestAbstract
{
@SuppressWarnings("unused")
private static final Logger log = LoggerFactory.getLogger(WcsDescribeCoverageTest.class);

@BeforeClass
public static void setUpForJUnit()
{
  try
  {
    baselineInput = TestUtils.composeInputDir(WcsDescribeCoverageTest.class);
    WcsGeneratorTestAbstract.setUpForJUnit();

    Properties mrgeoProperties = MrGeoProperties.getInstance();

    mrgeoProperties.put(MrGeoConstants.MRGEO_COMMON_HOME, inputHdfs.toString());
    mrgeoProperties.put(MrGeoConstants.MRGEO_HDFS_IMAGE, inputHdfs.toString());
    mrgeoProperties.put(MrGeoConstants.MRGEO_HDFS_COLORSCALE, inputHdfs.toString());

  }
  catch (Exception e)
  {
    e.printStackTrace();
  }
}

@AfterClass
public static void teardownForJUnit()
{
  MrGeoProperties.resetProperties();
}

  /*
   * WmsGenerator only supports describing tiles as a proposed extension in version 1.4.0.  If no 
   * version is specified, then version 1.4.0 is automatically assigned.
   */

@Test
@Category(IntegrationTest.class)
public void testDescribeCoverage110MissingIdentifiers() throws Exception
{
  Response response = target("wcs")
      .queryParam("SERVICE", "WCS")
      .queryParam("REQUEST", "DescribeCoverage")
      .queryParam("VERSION", "1.1.0")
      .request().get();

  processXMLResponse(response, "DescribeCoverage-1.1.0-MissingIdentifiers.xml", Response.Status.BAD_REQUEST);
}

@Test
@Category(IntegrationTest.class)
public void testDescribeCoverage100MissingCoverage() throws Exception
{
  Response response = target("wcs")
      .queryParam("SERVICE", "WCS")
      .queryParam("REQUEST", "DescribeCoverage")
      .queryParam("VERSION", "1.0.0")
      .request().get();

  processXMLResponse(response, "DescribeCoverage-1.0.0-MissingCoverage.xml", Response.Status.BAD_REQUEST);
}

@Test
@Category(IntegrationTest.class)
public void testDescribeCoverageEmptyVersion() throws Exception
{
  Response response = target("wcs")
      .queryParam("SERVICE", "WCS")
      .queryParam("REQUEST", "DescribeCoverage")
      .queryParam("IDENTIFIERS", "IslandsElevation-v2")
      .request().get();

  processXMLResponse(response, "DescribeCoverage-1.1.0.xml");
}

@Test
@Category(IntegrationTest.class)
public void testDescribeCoverage100() throws Exception
{
  Response response = target("wcs")
      .queryParam("SERVICE", "WCS")
      .queryParam("REQUEST", "DescribeCoverage")
      .queryParam("VERSION", "1.0.0")
      .queryParam("COVERAGE", "IslandsElevation-v2")
      .request().get();

  processXMLResponse(response, "DescribeCoverage-1.0.0.xml");
}

@Test
@Category(IntegrationTest.class)
public void testDescribeCoverage110() throws Exception
{
  Response response = target("wcs")
      .queryParam("SERVICE", "WCS")
      .queryParam("REQUEST", "DescribeCoverage")
      .queryParam("VERSION", "1.1.0")
      .queryParam("IDENTIFIERS", "IslandsElevation-v2")
      .request().get();

  processXMLResponse(response, "DescribeCoverage-1.1.0.xml");
}

}
