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

package org.mrgeo.services.wms;

import com.meterware.httpunit.WebRequest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.junit.IntegrationTest;
import org.mrgeo.test.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("static-method")
public class MissingStatsTest extends WmsGeneratorTestAbstract
{
  @SuppressWarnings("unused")
  private static final Logger log = LoggerFactory.getLogger(MissingStatsTest.class);

  @BeforeClass
  public static void setUp()
  {
    try
    {
      baselineInput = TestUtils.composeInputDir(MissingStatsTest.class);
      WmsGeneratorTestAbstract.setUp();
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }

  /*
   * If no stats have been calculated on an image, a default range of 0.0 to 1.0 is used for the
   * extrema during color scale application.
   */

  @Test
  @Category(IntegrationTest.class)
  public void testGetMapPngNoStats() throws Exception
  {
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmap");
      request.setParameter("LAYERS", "IslandsElevation-v2-no-stats");
      request.setParameter("FORMAT", "image/png");
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE);
      request.setParameter("WIDTH", "512");
      request.setParameter("HEIGHT", "512");

      processImageResponse(webClient.getResponse(request), "png");
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMapJpgNoStats() throws Exception
  {
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmap");
      request.setParameter("LAYERS", "IslandsElevation-v2-no-stats");
      request.setParameter("FORMAT", "image/jpeg");
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE);
      request.setParameter("WIDTH", "512");
      request.setParameter("HEIGHT", "512");

      processImageResponse(webClient.getResponse(request), "jpg");
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMapTifNoStats() throws Exception
  {
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmap");
      request.setParameter("LAYERS", "IslandsElevation-v2-no-stats");
      request.setParameter("FORMAT", "image/tiff");
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE);
      request.setParameter("WIDTH", "512");
      request.setParameter("HEIGHT", "512");

      processImageResponse(webClient.getResponse(request), "tif");
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }

  /*
   * If no stats have been calculated on an image, a default range of 0.0 to 1.0 is used for the
   * extrema during color scale application.
   */

  @Test
  @Category(IntegrationTest.class)
  public void testGetMosaicPngNoStats() throws Exception
  {
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmosaic");
      request.setParameter("LAYERS", "IslandsElevation-v2-no-stats");
      request.setParameter("FORMAT", "image/png");
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE);

      processImageResponse(webClient.getResponse(request), "png");
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMosaicJpgNoStats() throws Exception
  {
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmosaic");
      request.setParameter("LAYERS", "IslandsElevation-v2-no-stats");
      request.setParameter("FORMAT", "image/jpeg");
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE);

      processImageResponse(webClient.getResponse(request), "jpg");
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetMosaicTifNoStats() throws Exception
  {
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "getmosaic");
      request.setParameter("LAYERS", "IslandsElevation-v2-no-stats");
      request.setParameter("FORMAT", "image/tiff");
      request.setParameter("BBOX", ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE);

      processImageResponse(webClient.getResponse(request), "tif");
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }

  /*
   * If no stats have been calculated on an image, a default range of 0.0 to 1.0 is used for the
   * extrema during color scale application.
   */

  @Test
  @Category(IntegrationTest.class)
  public void testGetTilePngNoStats() throws Exception
  {
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "gettile");
      request.setParameter("LAYER", "IslandsElevation-v2-no-stats");
      request.setParameter("FORMAT", "image/png");
      request.setParameter("TILEROW", "56");
      request.setParameter("TILECOL", "242");
      request.setParameter("SCALE", "0.0027465820"); // zoom level 8

      processImageResponse(webClient.getResponse(request), "png");
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetTileJpgNoStats() throws Exception
  {
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "gettile");
      request.setParameter("LAYER", "IslandsElevation-v2-no-stats");
      request.setParameter("FORMAT", "image/jpeg");
      request.setParameter("TILEROW", "56");
      request.setParameter("TILECOL", "242");
      request.setParameter("SCALE", "0.0027465820"); // zoom level 8

      processImageResponse(webClient.getResponse(request), "jpg");
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void testGetTileTifNoStats() throws Exception
  {
    try
    {
      WebRequest request = createRequest();
      request.setParameter("REQUEST", "gettile");
      request.setParameter("LAYER", "IslandsElevation-v2-no-stats");
      request.setParameter("FORMAT", "image/tiff");
      request.setParameter("TILEROW", "56");
      request.setParameter("TILECOL", "242");
      request.setParameter("SCALE", "0.0027465820"); // zoom level 8

      processImageResponse(webClient.getResponse(request), "tif");
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }
}
