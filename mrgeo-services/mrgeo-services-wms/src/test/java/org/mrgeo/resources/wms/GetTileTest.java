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
import org.mrgeo.test.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("all") // Test code, not included in production
public class GetTileTest extends WmsGeneratorTestAbstract
{
@SuppressWarnings("unused")
private static final Logger log = LoggerFactory.getLogger(GetTileTest.class);

@BeforeClass
public static void setUpForJUnit()
{
  try
  {
    baselineInput = TestUtils.composeInputDir(GetTileTest.class);
    WmsGeneratorTestAbstract.setUpForJUnit();
  }
  catch (Exception e)
  {
    e.printStackTrace();
  }
}

  /*
   * WmsGenerator doesn't support more than one layer per request.
   */
//  @Test
//  @Category(IntegrationTest.class)
//  public void testGetTileMultipleRequestLayers() throws Exception
//  {
//    ClientResponse response = resource().path("/wms")
//            .queryParam("SERVICE", "WMS")
//            .queryParam("REQUEST", "gettile")
//            .queryParam("LAYER", "IslandsElevation-v2,IslandsElevation-v3")
//            .queryParam("FORMAT", "image/tiff")
//            .queryParam("TILEROW", "224")
//            .queryParam("TILECOL", "970")
//            .queryParam("SCALE", "0.0027465820")
//            .get(ClientResponse.class);
//
//    processXMLResponse(response, "testGetTileMultipleRequestLayers.xml", Response.Status.BAD_REQUEST);
//  }
//
//  @Test
//  @Category(IntegrationTest.class)
//  public void testGetTileInvalidFormat() throws Exception
//  {
//    ClientResponse response = resource().path("/wms")
//            .queryParam("SERVICE", "WMS")
//            .queryParam("REQUEST", "gettile")
//            .queryParam("LAYER", "IslandsElevation-v2")
//            .queryParam("FORMAT", "image/abc")
//            .queryParam("TILEROW", "224")
//            .queryParam("TILECOL", "970")
//            .queryParam("SCALE", "0.0027465820")
//            .get(ClientResponse.class);
//
//    processXMLResponse(response, "testGetTileInvalidFormat.xml", Response.Status.BAD_REQUEST);
//  }
//
//  @Test
//  @Category(IntegrationTest.class)
//  public void testGetTileInvalidLayer() throws Exception
//  {
//    ClientResponse response = resource().path("/wms")
//            .queryParam("SERVICE", "WMS")
//            .queryParam("REQUEST", "gettile")
//            .queryParam("LAYER", "IslandsElevation-v3")
//            .queryParam("FORMAT", "image/tiff")
//            .queryParam("TILEROW", "224")
//            .queryParam("TILECOL", "970")
//            .queryParam("SCALE", "0.0027465820")
//            .get(ClientResponse.class);
//
//    processXMLResponse(response, "testGetTileInvalidLayer.xml", Response.Status.BAD_REQUEST);
//  }
//
//  @Test
//  @Category(IntegrationTest.class)
//  public void testGetTileOutOfBoundsPng() throws Exception
//  {
//    ClientResponse response = resource().path("/wms")
//            .queryParam("SERVICE", "WMS")
//            .queryParam("REQUEST", "gettile")
//            .queryParam("LAYER", "IslandsElevation-v2")
//            .queryParam("FORMAT", "image/png")
//            .queryParam("TILEROW", "1")
//            .queryParam("TILECOL", "1")
//            .queryParam("SCALE", "0.0027465820")
//            .get(ClientResponse.class);
//
//    processXMLResponse(response, "testGetTileOutOfBoundsPng.xml", Response.Status.BAD_REQUEST);
//  }
//
//  @Test
//  @Category(IntegrationTest.class)
//  public void testGetTilePng() throws Exception
//  {
//    String contentType = "image/png";
//    ClientResponse response = resource().path("/wms")
//            .queryParam("SERVICE", "WMS")
//            .queryParam("REQUEST", "gettile")
//            .queryParam("LAYER", "IslandsElevation-v2")
//            .queryParam("FORMAT", contentType)
//            .queryParam("TILEROW", "56")
//            .queryParam("TILECOL", "242")
//            .queryParam("SCALE", "0.0027465820")  //zoom level 8
//            .get(ClientResponse.class);
//    processImageResponse(response, contentType, "png");
//  }
//
//  @Test
//  @Category(IntegrationTest.class)
//  public void testGetTileLowerCaseParams() throws Exception
//  {
//    String contentType = "image/png";
//    ClientResponse response = resource().path("/wms")
//            .queryParam("SERVICE", "WMS")
//            .queryParam("request", "gettile")
//            .queryParam("layer", "IslandsElevation-v2")
//            .queryParam("format", contentType)
//            .queryParam("tilerow", "56")
//            .queryParam("tilecol", "242")
//            .queryParam("scale", "0.0027465820")  //zoom level 8
//            .get(ClientResponse.class);
//
//    processImageResponse(response, contentType, "png");
//  }
//
//  @Test
//  @Category(IntegrationTest.class)
//  public void testGetTileFullLayerPath() throws Exception
//  {
//    String contentType = "image/png";
//    ClientResponse response = resource().path("/wms")
//            .queryParam("SERVICE", "WMS")
//            .queryParam("REQUEST", "gettile")
//            .queryParam("LAYER",
//                        "/mrgeo/test-files/org.mrgeo.resources.wms/WmsGeneratorTestAbstract/IslandsElevation-v2")
//            .queryParam("FORMAT", "image/png")
//            .queryParam("TILEROW", "56")
//            .queryParam("TILECOL", "242")
//            .queryParam("SCALE", "0.0027465820")  //zoom level 8
//            .get(ClientResponse.class);
//
//    processImageResponse(response, contentType, "png");
//  }
//
//  @Test
//  @Category(IntegrationTest.class)
//  public void testGetTileOutOfBoundsJpg() throws Exception
//  {
//    ClientResponse response = resource().path("/wms")
//            .queryParam("SERVICE", "WMS")
//            .queryParam("REQUEST", "gettile")
//            .queryParam("LAYER", "IslandsElevation-v2")
//            .queryParam("FORMAT", "image/jpg")
//            .queryParam("TILEROW", "1")
//            .queryParam("TILECOL", "1")
//            .queryParam("SCALE", "0.0027465820")
//            .get(ClientResponse.class);
//
//    processXMLResponse(response, "testGetTileOutOfBoundsJpg.xml", Response.Status.BAD_REQUEST);
//  }
//
//  @Test
//  @Category(IntegrationTest.class)
//  public void testGetTileJpg() throws Exception
//  {
//    String contentType = "image/jpeg";
//    ClientResponse response = resource().path("/wms")
//            .queryParam("SERVICE", "WMS")
//            .queryParam("REQUEST", "gettile")
//            .queryParam("LAYER", "IslandsElevation-v2")
//            .queryParam("FORMAT", "image/jpg")
//            .queryParam("TILEROW", "56")
//            .queryParam("TILECOL", "242")
//            .queryParam("SCALE", "0.0027465820")  //zoom level 8
//            .get(ClientResponse.class);
//
//      processImageResponse(response, contentType, "jpg");
//  }
//
//  @Test
//  @Category(IntegrationTest.class)
//  public void testGetTileOutOfBoundsTif() throws Exception
//  {
//    String contentType = "image/tiff";
//    ClientResponse response = resource().path("/wms")
//            .queryParam("SERVICE", "WMS")
//            .queryParam("REQUEST", "gettile")
//            .queryParam("LAYER", "IslandsElevation-v2")
//            .queryParam("FORMAT", contentType)
//            .queryParam("TILEROW", "1")
//            .queryParam("TILECOL", "1")
//            .queryParam("SCALE", "0.0027465820")
//            .get(ClientResponse.class);
//
//    processXMLResponse(response, "testGetTileOutOfBoundsTif.xml", Response.Status.BAD_REQUEST);
//  }
//
//  @Test
//  @Category(IntegrationTest.class)
//  public void testGetTileTif() throws Exception
//  {
//    String contentType = "image/tiff";
//    ClientResponse response = resource().path("/wms")
//            .queryParam("SERVICE", "WMS")
//            .queryParam("REQUEST", "gettile")
//            .queryParam("LAYER", "IslandsElevation-v2")
//            .queryParam("FORMAT", contentType)
//            .queryParam("TILEROW", "56")
//            .queryParam("TILECOL", "242")
//            .queryParam("SCALE", "0.0027465820")  //zoom level 8
//            .get(ClientResponse.class);
//
//    processImageResponse(response, contentType, "tif");
//  }
}
