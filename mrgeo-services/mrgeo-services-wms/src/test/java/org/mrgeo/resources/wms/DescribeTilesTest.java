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
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.junit.IntegrationTest;
import org.mrgeo.test.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import java.util.Properties;

@SuppressWarnings("all") // Test code, not included in production
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

    mrgeoProperties.put(MrGeoConstants.MRGEO_COMMON_HOME, inputHdfs.toString());
    mrgeoProperties.put(MrGeoConstants.MRGEO_HDFS_IMAGE, inputHdfs.toString());
    mrgeoProperties.put(MrGeoConstants.MRGEO_HDFS_COLORSCALE, inputHdfs.toString());

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
  Response response = target("wms")
      .queryParam("SERVICE", "WMS")
      .queryParam("REQUEST", "describetiles")
      .request().get();

  processXMLResponse(response, "DescribeTiles.xml");
}

@Test
@Category(IntegrationTest.class)
public void testDescribeTilesLessThan140() throws Exception
{
  Response response = target("wms")
      .queryParam("SERVICE", "WMS")
      .queryParam("REQUEST", "describetiles")
      .queryParam("VERSION", "1.3.0")
      .request().get();

  processXMLResponse(response, "DescribeTilesEarlyVersion.xml", Response.Status.BAD_REQUEST);
}

@Test
@Category(IntegrationTest.class)
public void testDescribeTiles140() throws Exception
{
  Response response = target("wms")
      .queryParam("SERVICE", "WMS")
      .queryParam("REQUEST", "describetiles")
      .queryParam("VERSION", "1.4.0")
      .request().get();

  processXMLResponse(response, "DescribeTiles140.xml");
}
}
