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

package org.mrgeo.resources.tms;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import junit.framework.Assert;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.image.MrsPyramid;
import org.mrgeo.image.MrsPyramidMetadata;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.services.tms.TmsService;
import org.mrgeo.services.utils.ImageTestUtils;
import org.mrgeo.test.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.io.IOException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.mockito.Mockito.*;

@SuppressWarnings("all") // Test code, not included in production
public class TileMapServiceResourceIntegrationTest extends JerseyTest
{
// only set this to true to generate new baseline images after correcting tests; image comparison
// tests won't be run when is set to true
public final static boolean GEN_BASELINE_DATA_ONLY = false;
private static final Logger log = LoggerFactory.getLogger(TileMapServiceResourceIntegrationTest.class);
private static final String rgbsmall_nopyramids = "rgbsmall-nopyramids";
private static final String astersmall_nopyramids = "astersmall-nopyramids";
private static String input = TestUtils.composeInputDir(TileMapServiceResourceIntegrationTest.class);
private static final String rgbsmall_nopyramids_abs = /*"file://" +*/ input + rgbsmall_nopyramids;
private static final String astersmall_nopyramids_abs = /*"file://" +*/ input + astersmall_nopyramids;
protected String baselineInput;
private TmsService service;
private HttpServletRequest request;

@Override
@SuppressFBWarnings(value = "ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD", justification = "Test setup")
public void setUp() throws Exception
{
  super.setUp();
  Mockito.reset(service, request);
  Properties props = MrGeoProperties.getInstance();
  props.put(MrGeoConstants.MRGEO_HDFS_IMAGE, input);
  props.put(MrGeoConstants.MRGEO_HDFS_COLORSCALE, input + "color-scales");

  baselineInput = TestUtils.composeInputDir(TileMapServiceResourceIntegrationTest.class);

  TileMapServiceResource.props = props;
}

@Test
@Category(UnitTest.class)
public void testGetRootResource() throws Exception
{
  Mockito.when(request.getRequestURL())
      .thenReturn(new StringBuffer("http://localhost:9998/tms"));

  Response response = target("tms")
      .request().get();

  Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus());

  verify(request, times(1)).getRequestURL();
  verifyNoMoreInteractions(request, service);
}

@Test
@Category(UnitTest.class)
public void testGetTileMapService() throws Exception
{
  String version = "1.0.0";

  List<String> retval = new ArrayList<String>();
  retval.add("foo");
  retval.add("foo2");

  Mockito.when(request.getRequestURL())
      .thenReturn(new StringBuffer("http://localhost:9998/tms" + version));
  Mockito.when(service.listImages())
      .thenReturn(retval);

  Response response = target("tms/" + version)
      .request().get();

  Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus());

  verify(request, times(1)).getRequestURL();
  verify(service, times(1));
  service.listImages();
  verifyNoMoreInteractions(request, service);
}

@Test
@Category(UnitTest.class)
public void testGetTileMap() throws Exception
{
  String version = "1.0.0";

  Mockito.when(request.getRequestURL())
      .thenReturn(new StringBuffer("http://localhost:9998/tms/1.0.0/" + rgbsmall_nopyramids_abs + "/global-geodetic"));
  Mockito.when(service.getMetadata(rgbsmall_nopyramids_abs))
      .thenReturn(getMetadata(rgbsmall_nopyramids_abs));

  Response response = target("tms" + "/" + version + "/" +
      URLEncoder.encode(rgbsmall_nopyramids_abs, "UTF-8") + "/global-geodetic")
      .request().get();

  Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus());

  verify(request, times(1)).getRequestURL();
  verify(service, times(1)).getMetadata(rgbsmall_nopyramids_abs);
  verify(service, times(0));
  service.listImages();
  verifyNoMoreInteractions(request, service);
}

@Test
@Category(UnitTest.class)
public void testGetTileMapMercator() throws Exception
{
  String version = "1.0.0";

  when(request.getRequestURL())
      .thenReturn(new StringBuffer("http://localhost:9998/tms/1.0.0/" +
          rgbsmall_nopyramids_abs + "/global-mercator"));
  when(service.getMetadata(rgbsmall_nopyramids_abs))
      .thenReturn(getMetadata(rgbsmall_nopyramids_abs));

  Response response = target("tms" + "/" + version + "/" +
      URLEncoder.encode(rgbsmall_nopyramids_abs, "UTF-8") + "/global-mercator")
      .request().get();

  Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus());

  verify(request, times(1)).getRequestURL();
  verify(service, times(1)).getMetadata(rgbsmall_nopyramids_abs);
  verify(service, times(0));
  service.listImages();
  verifyNoMoreInteractions(request, service);
}

@Test
@Category(UnitTest.class)
public void testGetTileBadFormat() throws Exception
{
  String version = "1.0.0";
  String raster = "elevation";
  int x = 710;
  int y = 350;
  int z = 10;
  String format = "gif";

  Response response = target("tms" + "/" + version + "/" +
      URLEncoder.encode(raster, "UTF-8") + "/global-geodetic/" + z + "/" + x + "/" + y + "." + format)
      .request().get();

  Assert.assertEquals(400, response.getStatus());
  Assert.assertEquals("Unsupported image format - gif", response.readEntity(String.class));
}

@Test
@Category(UnitTest.class)
public void testGetTileMissingRasterTileMap() throws Exception
{
  String version = "1.0.0";
  String raster = "nonexistantpyramid";

  ExecutionException exception = mock(ExecutionException.class);
  when(request.getRequestURL()).thenReturn(
      new StringBuffer("http://localhost:9998/tms/1.0.0/nonexistantpyramid/global-geodetic"));
  when(service.getMetadata("nonexistantpyramid")).thenThrow(exception);

  Response response = target("tms" + "/" + version + "/" +
      URLEncoder.encode(raster, "UTF-8") + "/global-geodetic")
      .request().get();

  Assert.assertEquals(404, response.getStatus());
  Assert.assertEquals("Tile map not found - " + raster, response.readEntity(String.class));

  verify(request, times(1)).getRequestURL();
  verify(service, times(1)).getMetadata("nonexistantpyramid");
  verifyNoMoreInteractions(request, service);
}

@Test
@Category(UnitTest.class)
public void testGetTileMissingRasterTile() throws Exception
{
  String version = "1.0.0";
  String raster = new String("nonexistantpyramid");
  int x = 710;
  int y = 350;
  int z = 10;
  String format = "jpg";

  ExecutionException exception = mock(ExecutionException.class);
  when(request.getRequestURL()).thenReturn(
      new StringBuffer("http://localhost:9998/tms/1.0.0/nonexistantpyramid/global-geodetic"));
  when(service.getMetadata("nonexistantpyramid")).thenThrow(exception);

  Response response = target("tms" + "/" + version + "/" +
      URLEncoder.encode(raster, "UTF-8") + "/global-geodetic/" + z + "/" + x + "/" + y + "." + format)
      .request().get();

  Assert.assertEquals(404, response.getStatus());
  Assert.assertEquals("Tile map not found - " + raster, response.readEntity(String.class));

  verify(request, times(0)).getRequestURL();
  verifyNoMoreInteractions(request, service);
}

@Test
@Category(UnitTest.class)
public void testGetTileOutOfBounds() throws Exception
{
  String version = "1.0.0";
  String raster = astersmall_nopyramids_abs;
  int x = 846;
  int y = 411;
  int z = 12;
  String format = "png";

  when(service.getMetadata(raster)).thenReturn(getMetadata(raster));
  when(service.getPyramid(raster)).thenReturn(getPyramid(raster));

  Response response = target("tms" + "/" + version + "/" +
      URLEncoder.encode(raster, "UTF-8") + "/global-geodetic/" + z + "/" + x + "/" + y + "." + format)
      .request().get();

  processImageResponse(response, format);

  verify(service, times(1)).getMetadata(raster);
  verifyNoMoreInteractions(request, service);
}

@Test
@Category(UnitTest.class)
public void testGetTileNonexistanPyramidLevel() throws Exception
{
  String version = "1.0.0";
  String raster = astersmall_nopyramids_abs;
  int x = 710;
  int y = 350;
  int z = 10;
  String format = "jpg";

  when(service.getMetadata(raster)).thenReturn(getMetadata(raster));

  Response response = target("tms" + "/" + version + "/" +
      URLEncoder.encode(raster, "UTF-8") + "/global-geodetic/" + z + "/" + x + "/" + y + "." + format)
      .request().get();

  Assert.assertEquals(404, response.getStatus());
  Assert.assertEquals("Tile map not found - " + raster, response.readEntity(String.class));

  verifyNoMoreInteractions(request, service);
}

@Test
@Category(UnitTest.class)
public void testGetTileRgbPng() throws Exception
{
  String version = "1.0.0";
  String raster = rgbsmall_nopyramids_abs;
  int x = 11346;
  int y = 5667;
  int z = 14;
  String format = "png";

  when(service.getMetadata(raster)).thenReturn(getMetadata(raster));
  when(service.getPyramid(raster)).thenReturn(getPyramid(raster));

  Response response = target("tms" + "/" + version + "/" +
      URLEncoder.encode(raster, "UTF-8") + "/global-geodetic/" + z + "/" + x + "/" + y + "." + format)
      .request().get();

  processImageResponse(response, format);

  verifyNoMoreInteractions(request, service);
}

@Test
@Category(UnitTest.class)
public void testGetTileRgbPngMercator() throws Exception
{
  String version = "1.0.0";
  String raster = rgbsmall_nopyramids_abs;
  int x = 11346;
  int y = 11517; // 11334; // 5667;
  int z = 15; // 14;
  String format = "png";

  when(service.getMetadata(raster)).thenReturn(getMetadata(raster));
  when(service.getPyramid(raster)).thenReturn(getPyramid(raster));

  Response response = target("tms" + "/" + version + "/" +
      URLEncoder.encode(raster, "UTF-8") + "/global-mercator/" + z + "/" + x + "/" + y + "." + format)
      .request().get();

  processImageResponse(response, format);
}

@Test
@Category(UnitTest.class)
public void testGetTileRgbJpg() throws Exception
{
  String version = "1.0.0";
  String raster = rgbsmall_nopyramids_abs;
  int x = 11346;
  int y = 5667;
  int z = 14;
  String format = "jpg";

  when(service.getMetadata(raster)).thenReturn(getMetadata(raster));
  when(service.getPyramid(raster)).thenReturn(getPyramid(raster));

  Response response = target("tms" + "/" + version + "/" +
      URLEncoder.encode(raster, "UTF-8") + "/global-geodetic/" + z + "/" + x + "/" + y + "." + format)
      .request().get();

  processImageResponse(response, format);
}

@Test
@Category(UnitTest.class)
public void testGetTileRgbTif() throws Exception
{
  String version = "1.0.0";
  String raster = rgbsmall_nopyramids_abs;
  int x = 11346;
  int y = 5667;
  int z = 14;
  String format = "tif";

  when(service.getMetadata(raster)).thenReturn(getMetadata(raster));
  when(service.getPyramid(raster)).thenReturn(getPyramid(raster));

  Response response = target("tms" + "/" + version + "/" +
      URLEncoder.encode(raster, "UTF-8") + "/global-geodetic/" + z + "/" + x + "/" + y + "." + format)
      .request().get();

  processImageResponse(response, format);
}

@Test
@Category(UnitTest.class)
public void testGetTileRgbTifMercator() throws Exception
{
  String version = "1.0.0";
  String raster = rgbsmall_nopyramids_abs;
  int x = 11346;
  int y = 11517; // 11334; // 5667;
  int z = 15; // 14;
  String format = "tif";

  when(service.getMetadata(raster)).thenReturn(getMetadata(raster));
  when(service.getPyramid(raster)).thenReturn(getPyramid(raster));

  Response response = target("tms" + "/" + version + "/" +
      URLEncoder.encode(raster, "UTF-8") + "/global-mercator/" + z + "/" + x + "/" + y + "." + format)
      .request().get();

  processImageResponse(response, format);
}

@Test
@Category(UnitTest.class)
public void testGetTileDefColorScaleTif() throws Exception
{
  String version = "1.0.0";
  String raster = astersmall_nopyramids_abs;
  int x = 2846;
  int y = 1411;
  int z = 12;
  String format = "tif";

  when(service.getMetadata(raster)).thenReturn(getMetadata(raster));
  when(service.getPyramid(raster)).thenReturn(getPyramid(raster));

  Response response = target("tms" + "/" + version + "/" +
      URLEncoder.encode(raster, "UTF-8") + "/global-geodetic/" + z + "/" + x + "/" + y + "." + format)
      .request().get();

  processImageResponse(response, format);
}

@Test
@Category(UnitTest.class)
public void testGetTileJsonColorScalePng() throws Exception
{
  String version = "1.0.0";
  String raster = astersmall_nopyramids_abs;
  int x = 2846;
  int y = 1411;
  int z = 12;
  String format = "png";
  String json =
      "{\"Interpolate\":\"true\",\"ForceValuesIntoRange\":\"false\",\"ReliefShading\":\"false\",\"NullColor\":{\"color\":\"0,0,0\",\"opacity\":\"0\"},\"Colors\":[{\"color\":\"255,0,0\",\"value\":\"0.0\"},{\"color\":\"255,255,0\",\"value\":\"0.25\"},{\"color\":\"0,255,255\",\"value\":\"0.75\"},{\"color\":\"255,255,255\",\"value\":\"1.0\"}],\"Scaling\":\"MinMax\"}";

  when(service.getMetadata(raster)).thenReturn(getMetadata(raster));
  when(service.getPyramid(raster)).thenReturn(getPyramid(raster));

  Response response = target("tms" + "/" + version + "/" +
      URLEncoder.encode(raster, "UTF-8") + "/global-geodetic/" + z + "/" + x + "/" + y + "." + format)
      .queryParam("color-scale", URLEncoder.encode(json, "UTF-8"))
      .request().get();

  processImageResponse(response, format);

}

@Test
@Category(UnitTest.class)
public void testGetTileBadJsonColorScalePng() throws Exception
{
  String version = "1.0.0";
  String raster = astersmall_nopyramids_abs;
  int x = 2846;
  int y = 1411;
  int z = 12;
  String format = "png";
  String json = "{\"foo\":\"bar\"}";


  Response response = target("tms" + "/" + version + "/" +
      URLEncoder.encode(raster, "UTF-8") + "/global-geodetic/" + z + "/" + x + "/" + y + "." + format)
      .queryParam("color-scale", URLEncoder.encode(json, "UTF-8"))
      .request().get();

  Assert.assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  Assert.assertEquals("Unable to parse color scale JSON", response.readEntity(String.class));
}

@Test
@Category(UnitTest.class)
public void testGetTileJsonColorScaleMinMaxPng() throws Exception
{
  String version = "1.0.0";
  String raster = astersmall_nopyramids_abs;
  int x = 2846;
  int y = 1411;
  int z = 12;
  String format = "png";
  String json =
      "{\"Interpolate\":\"true\",\"ForceValuesIntoRange\":\"false\",\"ReliefShading\":\"false\",\"NullColor\":{\"color\":\"0,0,0\",\"opacity\":\"0\"},\"Colors\":[{\"color\":\"255,0,0\",\"value\":\"0.0\"},{\"color\":\"255,255,0\",\"value\":\"0.25\"},{\"color\":\"0,255,255\",\"value\":\"0.75\"},{\"color\":\"255,255,255\",\"value\":\"1.0\"}],\"Scaling\":\"MinMax\"}";
  int min = 3000;
  int max = 4000;

  when(service.getMetadata(raster)).thenReturn(getMetadata(raster));
  when(service.getPyramid(raster)).thenReturn(getPyramid(raster));

  Response response = target("tms" + "/" + version + "/" +
      URLEncoder.encode(raster, "UTF-8") + "/global-geodetic/" + z + "/" + x + "/" + y + "." + format)
      .queryParam("color-scale", URLEncoder.encode(json, "UTF-8"))
      .queryParam("min", String.valueOf(min))
      .queryParam("max", String.valueOf(max))
      .request().get();

  processImageResponse(response, format);

}

@Test
@Category(UnitTest.class)
public void testGetTileColorScaleNamePng() throws Exception
{
  String version = "1.0.0";
  String raster = astersmall_nopyramids_abs;
  int x = 2846;
  int y = 1411;
  int z = 12;
  String format = "png";
  String colorScaleName = "Default";

  when(service.getMetadata(raster)).thenReturn(getMetadata(raster));
  when(service.getPyramid(raster)).thenReturn(getPyramid(raster));

  Response response = target("tms" + "/" + version + "/" +
      URLEncoder.encode(raster, "UTF-8") + "/global-geodetic/" + z + "/" + x + "/" + y + "." + format)
      .queryParam("color-scale-name", colorScaleName)
      .request().get();

  processImageResponse(response, format);
}

@Test
@Category(UnitTest.class)
public void testGetTileColorScaleNameNonExistent() throws Exception
{
  String version = "1.0.0";
  String raster = astersmall_nopyramids_abs;
  int x = 2846;
  int y = 1411;
  int z = 12;
  String format = "png";
  String colorScaleName = "Missing";

  Response response = target("tms" + "/" + version + "/" +
      URLEncoder.encode(raster, "UTF-8") + "/global-geodetic/" + z + "/" + x + "/" + y + "." + format)
      .queryParam("color-scale-name", colorScaleName)
      .request().get();

  Assert.assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  Assert.assertEquals("Unable to open color scale file", response.readEntity(String.class));
}

@Override
protected Application configure()
{
  service = Mockito.mock(TmsService.class);
  request = Mockito.mock(HttpServletRequest.class);

  ResourceConfig config = new ResourceConfig();
  config.register(TileMapServiceResource.class);
  config.register(new AbstractBinder()
  {
    @Override
    protected void configure()
    {
      bind(service).to(TmsService.class);
    }
  });
  config.register(new AbstractBinder()
  {
    @Override
    protected void configure()
    {
      bind(request).to(HttpServletRequest.class);
    }
  });
  return config;
}

private MrsPyramidMetadata getMetadata(String pyramid) throws IOException
{
  return getPyramid(pyramid).getMetadata();
}

private MrsPyramid getPyramid(String pyramid) throws IOException
{
  return MrsPyramid.open(pyramid, (ProviderProperties) null);
}

private void processImageResponse(final Response response, final String extension)
    throws IOException
{
  try
  {
    Assert.assertEquals("Bad response code", Status.OK.getStatusCode(), response.getStatus());

    if (GEN_BASELINE_DATA_ONLY)
    {
      final String outputPath =
          baselineInput + Thread.currentThread().getStackTrace()[2].getMethodName() + "." +
              extension;
      log.info("Generating baseline image: " + outputPath);
      ImageTestUtils.writeBaselineImage(response, outputPath);
    }
    else
    {
      final String baselineImageFile =
          baselineInput + Thread.currentThread().getStackTrace()[2].getMethodName() + "." +
              extension;
      log.info("Comparing result to baseline image " + baselineImageFile + " ...");
      ImageTestUtils.outputImageMatchesBaseline(response, baselineImageFile);
    }
  }
  finally
  {
    if (response != null)
    {
      response.close();
    }
  }
}
}
