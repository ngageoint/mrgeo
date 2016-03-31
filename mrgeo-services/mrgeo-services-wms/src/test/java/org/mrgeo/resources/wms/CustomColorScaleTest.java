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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.junit.IntegrationTest;
import org.mrgeo.test.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("static-method")
public class CustomColorScaleTest extends WmsGeneratorTestAbstract
{
  @SuppressWarnings("unused")
  private static final Logger log = 
    LoggerFactory.getLogger(CustomColorScaleTest.class);
  
  @BeforeClass 
  public static void setUpForJUnit()
  {    
    try 
    {
      baselineInput = TestUtils.composeInputDir(CustomColorScaleTest.class);
      WmsGeneratorTestAbstract.setUpForJUnit();
      
      FileSystem fileSystem = HadoopFileUtils.getFileSystem(inputHdfs);
      
      //remove the system color scale
      fileSystem.delete(new Path(inputHdfs, "Default.xml"), false);
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }
  
  /*
   * WmsGenerator should use ColorScale's hardcoded color scale if the image doesn't have a color
   * scale file, nor is there a system Default.xml color scale in the image base path.
   */
  @Test 
  @Category(IntegrationTest.class)  
  public void testMissingSystemColorScale() throws Exception
  {
    MrGeoProperties.getInstance().setProperty(MrGeoConstants.MRGEO_HDFS_COLORSCALE, "foo/bar");

    String contentType = "image/png";
    ClientResponse response = resource().path("/wms")
            .queryParam("SERVICE", "WMS")
            .queryParam("REQUEST", "getmap")
            .queryParam("LAYERS", "IslandsElevation-v2")
            .queryParam("FORMAT", contentType)
            .queryParam("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE)
            .queryParam("WIDTH", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
            .queryParam("HEIGHT", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
            .get(ClientResponse.class);

    processImageResponse(response, contentType, "png");
  }
  
  // TODO: Commented out all of the following WMS tests since we no longer support a
  // default color scale (obtained from ColorScale.xml in the pyramid's directory).
  // The solution will be to pass the color scale name as the STYLE element of the
  // WMS request.
//  @Test 
//  @Category(IntegrationTest.class)  
//  public void testGetMapPngWithUserSuppliedColorScale() throws Exception
//  {
//    try
//    {
//      WebRequest request = createRequest();
//      request.setParameter("REQUEST", "getmap");
//      request.setParameter("LAYERS", "IslandsElevation-v2-color-scale");
//      request.setParameter("FORMAT", "image/png");
//      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE);
//      request.setParameter("WIDTH", "512");
//      request.setParameter("HEIGHT", "512");
//        
//      processImageResponse(webClient.getResponse(request), "png");
//    }
//    catch (Exception e)
//    {
//      e.printStackTrace();
//      throw e;
//    }
//  }
//  
//  @Test 
//  @Category(IntegrationTest.class)  
//  public void testGetMapJpgWithUserSuppliedColorScale() throws Exception
//  {
//    try
//    {
//      WebRequest request = createRequest();
//      request.setParameter("REQUEST", "getmap");
//      request.setParameter("LAYERS", "IslandsElevation-v2-color-scale");
//      request.setParameter("FORMAT", "image/jpeg");
//      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE);
//      request.setParameter("WIDTH", "512");
//      request.setParameter("HEIGHT", "512");
//        
//      processImageResponse(webClient.getResponse(request), "jpg");
//    }
//    catch (Exception e)
//    {
//      e.printStackTrace();
//      throw e;
//    }
//  }
//  
//  /*
//   * TIF doesn't support color scales, so shouldn't try to apply it
//   */
//  @Test 
//  @Category(IntegrationTest.class)  
//  public void testGetMapTifWithUserSuppliedColorScale() throws Exception
//  {
//    try
//    {
//      WebRequest request = createRequest();
//      request.setParameter("REQUEST", "getmap");
//      request.setParameter("LAYERS", "IslandsElevation-v2-color-scale");
//      request.setParameter("FORMAT", "image/tiff");
//      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE);
//      request.setParameter("WIDTH", "512");
//      request.setParameter("HEIGHT", "512");
//        
//      processImageResponse(webClient.getResponse(request), "tif");
//    }
//    catch (Exception e)
//    {
//      e.printStackTrace();
//      throw e;
//    }
//  }
//  
//  @Test 
//  @Category(IntegrationTest.class)  
//  public void testGetMosaicPngWithUserSuppliedColorScale() throws Exception
//  {
//    try
//    {
//      WebRequest request = createRequest();
//      request.setParameter("REQUEST", "getmosaic");
//      request.setParameter("LAYERS", "IslandsElevation-v2-color-scale");
//      request.setParameter("FORMAT", "image/png");
//      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE);
//        
//      processImageResponse(webClient.getResponse(request), "png");
//    }
//    catch (Exception e)
//    {
//      e.printStackTrace();
//      throw e;
//    }
//  }
//  
//  @Test 
//  @Category(IntegrationTest.class)  
//  public void testGetMosaicJpgWithUserSuppliedColorScale() throws Exception
//  {
//    try
//    {
//      WebRequest request = createRequest();
//      request.setParameter("REQUEST", "getmosaic");
//      request.setParameter("LAYERS", "IslandsElevation-v2-color-scale");
//      request.setParameter("FORMAT", "image/jpeg");
//      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE);
//        
//      processImageResponse(webClient.getResponse(request), "jpg");
//    }
//    catch (Exception e)
//    {
//      e.printStackTrace();
//      throw e;
//    }
//  }
//  
//  /*
//   * TIF doesn't support color scales, so shouldn't try to apply it
//   */
//  @Test 
//  @Category(IntegrationTest.class)  
//  public void testGetMosaicTifWithUserSuppliedColorScale() throws Exception
//  {
//    try
//    {
//      WebRequest request = createRequest();
//      request.setParameter("REQUEST", "getmosaic");
//      request.setParameter("LAYERS", "IslandsElevation-v2-color-scale");
//      request.setParameter("FORMAT", "image/tiff");
//      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE);
//        
//      processImageResponse(webClient.getResponse(request), "tif");
//    }
//    catch (Exception e)
//    {
//      e.printStackTrace();
//      throw e;
//    }
//  }
//  
//  @Test 
//  @Category(IntegrationTest.class)  
//  public void testGetTilePngWithUserSuppliedColorScale() throws Exception
//  {
//    try
//    {
//      WebRequest request = createRequest();
//      request.setParameter("REQUEST", "gettile");
//      request.setParameter("LAYER", "IslandsElevation-v2-color-scale");
//      request.setParameter("FORMAT", "image/png");
//      request.setParameter("TILEROW", "56");
//      request.setParameter("TILECOL", "242");
//      request.setParameter("SCALE", "0.0027465820");  //zoom level 8
//        
//      processImageResponse(webClient.getResponse(request), "png");
//    }
//    catch (Exception e)
//    {
//      e.printStackTrace();
//      throw e;
//    }
//  }
//  
//  @Test 
//  @Category(IntegrationTest.class)  
//  public void testGetTileJpgWithUserSuppliedColorScale() throws Exception
//  {
//    try
//    {
//      WebRequest request = createRequest();
//      request.setParameter("REQUEST", "gettile");
//      request.setParameter("LAYER", "IslandsElevation-v2-color-scale");
//      request.setParameter("FORMAT", "image/jpeg");
//      request.setParameter("TILEROW", "56");
//      request.setParameter("TILECOL", "242");
//      request.setParameter("SCALE", "0.0027465820");  //zoom level 8
//        
//      processImageResponse(webClient.getResponse(request), "jpg");
//    }
//    catch (Exception e)
//    {
//      e.printStackTrace();
//      throw e;
//    }
//  }
//  
//  /*
//   * TIF doesn't support color scales, so shouldn't try to apply it
//   */
//  @Test 
//  @Category(IntegrationTest.class)  
//  public void testGetTileTifWithUserSuppliedColorScale() throws Exception
//  {
//    try
//    {
//      WebRequest request = createRequest();
//      request.setParameter("REQUEST", "gettile");
//      request.setParameter("LAYER", "IslandsElevation-v2-color-scale");
//      request.setParameter("FORMAT", "image/tiff");
//      request.setParameter("TILEROW", "56");
//      request.setParameter("TILECOL", "242");
//      request.setParameter("SCALE", "0.0027465820");  //zoom level 8
//        
//      processImageResponse(webClient.getResponse(request), "tif");
//    }
//    catch (Exception e)
//    {
//      e.printStackTrace();
//      throw e;
//    }
//  }
}
