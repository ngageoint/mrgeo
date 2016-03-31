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

 import com.sun.jersey.api.client.ClientResponse;
 import junit.framework.Assert;
 import org.junit.Before;
 import org.junit.BeforeClass;
 import org.junit.Test;
 import org.junit.experimental.categories.Category;
 import org.mrgeo.data.DataProviderFactory;
 import org.mrgeo.junit.IntegrationTest;
 import org.mrgeo.test.TestUtils;
 import org.slf4j.Logger;
 import org.slf4j.LoggerFactory;

 import javax.ws.rs.core.Response;

 import static org.junit.Assert.assertEquals;
 import static org.junit.Assert.assertTrue;

@SuppressWarnings("static-method")
public class GetMosaicTest extends WmsGeneratorTestAbstract
{
  @SuppressWarnings("unused")
  private static final Logger log = LoggerFactory.getLogger(GetMosaicTest.class);

  @BeforeClass 
  public static void setUpForJUnit()
  {    
    try 
    {
      baselineInput = TestUtils.composeInputDir(GetMosaicTest.class);
      WmsGeneratorTestAbstract.setUpForJUnit();
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }
  
  @Override
  @Before
  public void init()
  {
    DataProviderFactory.invalidateCache();
  }

  /*
   * WmsGenerator doesn't support more than one layer per request.
   */
  @Test 
  @Category(IntegrationTest.class)  
  public void testGetMosaicMultipleRequestLayers() throws Exception
  {
    ClientResponse response = resource().path("/wms")
            .queryParam("SERVICE", "WMS")
            .queryParam("REQUEST", "getmosaic")
            .queryParam("LAYERS", "IslandsElevation-v2,IslandsElevation-v3")
            .queryParam("FORMAT", "image/png")
            .queryParam("BBOX", "160.312500,-11.250000,161.718750,-9.843750")
            .get(ClientResponse.class);

    processXMLResponse(response, "testGetMosaicMultipleRequestLayers.xml", Response.Status.BAD_REQUEST);
  }

  @Test 
  @Category(IntegrationTest.class)  
  public void testGetMosaicInvalidFormat() throws Exception
  {
    ClientResponse response = resource().path("/wms")
            .queryParam("SERVICE", "WMS")
            .queryParam("REQUEST", "getmosaic")
            .queryParam("LAYERS", "IslandsElevation-v2")
            .queryParam("FORMAT", "image/abc")
            .queryParam("BBOX", "160.312500,-11.250000,161.718750,-9.843750")
            .get(ClientResponse.class);

    processXMLResponse(response, "testGetMosaicInvalidFormat.xml", Response.Status.BAD_REQUEST);
  }

  @Test 
  @Category(IntegrationTest.class)  
  public void testGetMosaicInvalidLayer() throws Exception
  {
    ClientResponse response = resource().path("/wms")
            .queryParam("SERVICE", "WMS")
            .queryParam("REQUEST", "getmosaic")
            .queryParam("LAYERS", "IslandsElevation-v3")
            .queryParam("FORMAT", "image/png")
            .queryParam("BBOX", "160.312500,-11.250000,161.718750,-9.843750")
            .get(ClientResponse.class);

    processXMLResponse(response, "testGetMosaicInvalidLayer.xml", Response.Status.BAD_REQUEST);
  }

  /*
   * PNG out of bounds requests should return a transparent image.
   */
  @Test 
  @Category(IntegrationTest.class)  
  public void testGetMosaicOutOfBoundsPng() throws Exception
  {
    String contentType = "image/png";
    ClientResponse response = resource().path("/wms")
            .queryParam("SERVICE", "WMS")
            .queryParam("REQUEST", "getmosaic")
            .queryParam("LAYERS", "IslandsElevation-v2")
            .queryParam("FORMAT", contentType)
            .queryParam("BBOX", ISLANDS_ELEVATION_V2_OUT_OF_BOUNDS)
            .get(ClientResponse.class);

    processImageResponse(response, contentType, "png");
  }

  /*
   * JPG doesn't support transparency, so a black image is returned for out of bounds requests.
   */
  @Test 
  @Category(IntegrationTest.class)  
  public void testGetMosaicOutOfBoundsJpg() throws Exception
  {
    String contentType = "image/jpeg";
    ClientResponse response = resource().path("/wms")
            .queryParam("SERVICE", "WMS")
            .queryParam("REQUEST", "getmosaic")
            .queryParam("LAYERS", "IslandsElevation-v2")
            .queryParam("FORMAT", contentType)
            .queryParam("BBOX", ISLANDS_ELEVATION_V2_OUT_OF_BOUNDS)
            .get(ClientResponse.class);

    processImageResponse(response, contentType, "jpg");
  }

  @Test 
  @Category(IntegrationTest.class)  
  public void testGetMosaicOutOfBoundsTif() throws Exception
  {
    String contentType = "image/tiff";
    ClientResponse response = resource().path("/wms")
            .queryParam("SERVICE", "WMS")
            .queryParam("REQUEST", "getmosaic")
            .queryParam("LAYERS", "IslandsElevation-v2")
            .queryParam("FORMAT", contentType)
            .queryParam("BBOX", ISLANDS_ELEVATION_V2_OUT_OF_BOUNDS)
            .get(ClientResponse.class);

    processImageResponse(response, contentType, "tif");
  }

  @Test 
  @Category(IntegrationTest.class)  
  public void testGetMosaicPngSingleSourceTile() throws Exception
  {
    String contentType = "image/png";
    ClientResponse response = resource().path("/wms")
            .queryParam("SERVICE", "WMS")
            .queryParam("REQUEST", "getmosaic")
            .queryParam("LAYERS", "IslandsElevation-v2")
            .queryParam("FORMAT", contentType)
            .queryParam("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE)
            .get(ClientResponse.class);

    processImageResponse(response, contentType, "png");
  }

  @Test 
  @Category(IntegrationTest.class)  
  public void testGetMosaicLowerCaseParams() throws Exception
  {
    String contentType = "image/png";
    ClientResponse response = resource().path("/wms")
            .queryParam("SERVICE", "WMS")
            .queryParam("request", "getmosaic")
            .queryParam("layers", "IslandsElevation-v2")
            .queryParam("format", contentType)
            .queryParam("bbox", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE)
            .get(ClientResponse.class);

    processImageResponse(response, contentType, "png");
  }

  @Test 
  @Category(IntegrationTest.class)  
  public void testGetMosaicFullLayerPath() throws Exception
  {
    String contentType = "image/png";
    ClientResponse response = resource().path("/wms")
            .queryParam("SERVICE", "WMS")
            .queryParam("REQUEST", "getmosaic")
            .queryParam("LAYERS",
                        "/mrgeo/test-files/org.mrgeo.resources.wms/WmsGeneratorTestAbstract/IslandsElevation-v2")
            .queryParam("FORMAT", contentType)
            .queryParam("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE)
            .get(ClientResponse.class);

    processImageResponse(response, contentType, "png");
  }

  @Test 
  @Category(IntegrationTest.class)  
  public void testGetMosaicPngMultipleSourceTiles() throws Exception
  {
    String contentType = "image/png";
    ClientResponse response = resource().path("/wms")
            .queryParam("SERVICE", "WMS")
            .queryParam("REQUEST", "getmosaic")
            .queryParam("LAYERS", "IslandsElevation-v2")
            .queryParam("FORMAT", contentType)
            .queryParam("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_MULTIPLE_SOURCE_TILES)
            .get(ClientResponse.class);

    processImageResponse(response, contentType, "png");
  }

  @Test 
  @Category(IntegrationTest.class)  
  public void testGetMosaicJpgSingleSourceTile() throws Exception
  {
    String contentType = "image/jpeg";
    ClientResponse response = resource().path("/wms")
            .queryParam("SERVICE", "WMS")
            .queryParam("REQUEST", "getmosaic")
            .queryParam("LAYERS", "IslandsElevation-v2")
            .queryParam("FORMAT", contentType)
            .queryParam("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE)
            .get(ClientResponse.class);

    processImageResponse(response, contentType, "jpg");
  }

  @Test 
  @Category(IntegrationTest.class)  
  public void testGetMosaicJpgMultipleSourceTiles() throws Exception
  {
    String contentType = "image/jpeg";
    ClientResponse response = resource().path("/wms")
            .queryParam("SERVICE", "WMS")
            .queryParam("REQUEST", "getmosaic")
            .queryParam("LAYERS", "IslandsElevation-v2")
            .queryParam("FORMAT", contentType)
            .queryParam("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_MULTIPLE_SOURCE_TILES)
            .get(ClientResponse.class);

    processImageResponse(response, contentType, "jpg");
  }

  @Test 
  @Category(IntegrationTest.class)  
  public void testGetMosaicTifSingleSourceTile() throws Exception
  {
    String contentType = "image/tiff";
    ClientResponse response = resource().path("/wms")
            .queryParam("SERVICE", "WMS")
            .queryParam("REQUEST", "getmosaic")
            .queryParam("LAYERS", "IslandsElevation-v2")
            .queryParam("FORMAT", contentType)
            .queryParam("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE)
            .get(ClientResponse.class);

    processImageResponse(response, contentType, "tif");
  }

  @Test 
  @Category(IntegrationTest.class)  
  public void testGetMosaicTifMultipleSourceTiles() throws Exception
  {
    String contentType = "image/tiff";
    ClientResponse response = resource().path("/wms")
            .queryParam("SERVICE", "WMS")
            .queryParam("REQUEST", "getmosaic")
            .queryParam("LAYERS", "IslandsElevation-v2")
            .queryParam("FORMAT", contentType)
            .queryParam("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_MULTIPLE_SOURCE_TILES)
            .get(ClientResponse.class);

    processImageResponse(response, contentType, "tif");
  }

  @Test 
  @Category(IntegrationTest.class)  
  public void testGetMosaicJpgNonExistingZoomLevelBelowWithPyramids() throws Exception
  {
    String contentType = "image/jpeg";
    ClientResponse response = resource().path("/wms")
            .queryParam("SERVICE", "WMS")
            .queryParam("REQUEST", "getmosaic")
            .queryParam("LAYERS", "IslandsElevation-v2")
            .queryParam("FORMAT", contentType)
            //pyramid only goes up to zoom level = 10; pass in zoom level = 11
            .queryParam("BBOX", ISLANDS_ELEVATION_V2_PAST_HIGHEST_RES_ZOOM_LEVEL)
            .get(ClientResponse.class);

    processImageResponse(response, contentType, "jpg");
  }

  @Test 
  @Category(IntegrationTest.class)  
  public void testGetMosaicTifNonExistingZoomLevelBelowWithPyramids() throws Exception
  {
    String contentType = "image/tiff";
    ClientResponse response = resource().path("/wms")
            .queryParam("SERVICE", "WMS")
            .queryParam("REQUEST", "getmosaic")
            .queryParam("LAYERS", "IslandsElevation-v2")
            .queryParam("FORMAT", contentType)
            //pyramid only goes up to zoom level = 10; pass in zoom level = 11
            .queryParam("BBOX", ISLANDS_ELEVATION_V2_PAST_HIGHEST_RES_ZOOM_LEVEL)
            .get(ClientResponse.class);

    processImageResponse(response, contentType, "tif");
  }
}
