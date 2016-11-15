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

import javax.ws.rs.core.Response;

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

    Response response = target("wms")
        .queryParam("SERVICE", "WMS")
        .queryParam("REQUEST", "getmap")
        .queryParam("LAYERS", "IslandsElevation-v2")
        .queryParam("FORMAT", contentType)
        .queryParam("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE)
        .queryParam("WIDTH", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
        .queryParam("HEIGHT", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT)
        .request().get();

    processImageResponse(response, contentType, "png");
  }

}
