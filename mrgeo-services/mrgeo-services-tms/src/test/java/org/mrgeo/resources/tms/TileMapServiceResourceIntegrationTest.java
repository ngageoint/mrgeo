/*
 * Copyright 2009-2015 DigitalGlobe, Inc.
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

package org.mrgeo.resources.tms;

import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.core.DefaultResourceConfig;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import com.sun.jersey.spi.inject.SingletonTypeInjectableProvider;
import com.sun.jersey.test.framework.JerseyTest;
import com.sun.jersey.test.framework.LowLevelAppDescriptor;
import com.sun.jersey.test.framework.spi.container.TestContainerException;
import com.sun.jersey.test.framework.spi.container.TestContainerFactory;
import junit.framework.Assert;
import org.codehaus.jackson.jaxrs.JacksonJsonProvider;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.mrgeo.FilteringInMemoryTestContainerFactory;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.image.MrsPyramid;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.image.MrsPyramidMetadata;
import org.mrgeo.services.tms.TmsService;
import org.mrgeo.services.utils.ImageTestUtils;
import org.mrgeo.test.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response.Status;
import java.io.IOException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.mockito.Mockito.*;

public class TileMapServiceResourceIntegrationTest extends JerseyTest
{
  private static final Logger log = LoggerFactory.getLogger(TileMapServiceResourceIntegrationTest.class);

  // only set this to true to generate new baseline images after correcting tests; image comparison
  // tests won't be run when is set to true
  public final static boolean GEN_BASELINE_DATA_ONLY = false;
  
  protected static String baselineInput;

  private static String input = TestUtils.composeInputDir(TileMapServiceResourceIntegrationTest.class);
  private static final String rgbsmall_nopyramids = "rgbsmall-nopyramids";
  private static final String rgbsmall_nopyramids_abs = /*"file://" +*/ input + rgbsmall_nopyramids;
  private static final String astersmall_nopyramids = "astersmall-nopyramids";
  private static final String astersmall_nopyramids_abs = /*"file://" +*/ input + astersmall_nopyramids;

  TmsService service;
  private HttpServletRequest request;


  @Override
  protected LowLevelAppDescriptor configure()
  {
      DefaultResourceConfig resourceConfig = new DefaultResourceConfig();
      service = mock(TmsService.class);
      request = mock(HttpServletRequest.class);

      resourceConfig.getSingletons().add( new SingletonTypeInjectableProvider<Context, TmsService>(TmsService.class, service) {});
      resourceConfig.getSingletons().add( new SingletonTypeInjectableProvider<Context, HttpServletRequest>(HttpServletRequest.class, request) {});

      resourceConfig.getClasses().add(TileMapServiceResource.class);
      resourceConfig.getClasses().add(JacksonJsonProvider.class);

      return new LowLevelAppDescriptor.Builder( resourceConfig ).build();
  }

  @Override
  protected TestContainerFactory getTestContainerFactory() throws TestContainerException {
      return new FilteringInMemoryTestContainerFactory();
  }

  @Override
  public void setUp() throws Exception {
      super.setUp();
      Mockito.reset(request, service);
      Properties props = MrGeoProperties.getInstance();
      props.put(MrGeoConstants.MRGEO_HDFS_IMAGE, input);
      props.put(MrGeoConstants.MRGEO_HDFS_COLORSCALE, input + "color-scales");
      
      baselineInput = TestUtils.composeInputDir(TileMapServiceResourceIntegrationTest.class);

      TileMapServiceResource.props = props;
  }

  @Test
  @Category(UnitTest.class)
  public void testGetRootResource() throws Exception {
    when(request.getRequestURL()).thenReturn(new StringBuffer("http://localhost:9998/tms"));

    WebResource webResource = resource();
    WebResource wr =  webResource.path("tms");
    ClientResponse resp = wr.get(ClientResponse.class);

    Assert.assertEquals(Status.OK.getStatusCode(), resp.getStatus());

    verify(request, times(1)).getRequestURL();
    verifyNoMoreInteractions(request, service);
  }

  @Test
  @Category(UnitTest.class)
  public void testGetTileMapService() throws Exception {
    String version = "1.0.0";

    List<String> retval = new ArrayList<String>();
    retval.add("foo");
    retval.add("foo2");
    when(request.getRequestURL()).thenReturn(new StringBuffer("http://localhost:9998/tms"));
    when(service.listImages()).thenReturn(retval);

    WebResource webResource = resource();
    WebResource wr =  webResource.path("tms" + "/" + version);
    ClientResponse resp = wr.get(ClientResponse.class);

    Assert.assertEquals(Status.OK.getStatusCode(), resp.getStatus());

    verify(request, times(1)).getRequestURL();
    verify(service, times(1));
    service.listImages();
    verifyNoMoreInteractions(request, service);
  }

  private MrsPyramidMetadata getMetadata(String pyramid) throws IOException {
      return getPyramid(pyramid).getMetadata();
  }

  private MrsPyramid getPyramid(String pyramid) throws IOException {
    return MrsPyramid.open(pyramid, (ProviderProperties)null);
  }

  @Test
  @Category(UnitTest.class)
  public void testGetTileMap() throws Exception {
    String version = "1.0.0";

    when(request.getRequestURL()).thenReturn(new StringBuffer("http://localhost:9998/tms/1.0.0/" + rgbsmall_nopyramids_abs));
    when(service.getMetadata(rgbsmall_nopyramids_abs)).thenReturn( getMetadata(rgbsmall_nopyramids_abs) );
    WebResource webResource = resource();
    WebResource wr =  webResource.path("tms" + "/" + version + "/" + URLEncoder.encode(rgbsmall_nopyramids_abs, "UTF-8"));
    ClientResponse resp = wr.get(ClientResponse.class);

    Assert.assertEquals(Status.OK.getStatusCode(), resp.getStatus());

    verify(request, times(1)).getRequestURL();
    verify(service, times(1)).getMetadata(rgbsmall_nopyramids_abs);
    verify(service, times(0));
    service.listImages();
    verifyNoMoreInteractions(request, service);
  }

  @Test
  @Category(UnitTest.class)
  public void testGetTileBadFormat() throws Exception {
    String version = "1.0.0";
    String raster = "elevation";
    int x = 710;
    int y = 350;
    int z = 10;
    String format = "gif";

    WebResource webResource = resource();
    WebResource wr =  webResource.path("tms" + "/" + version + "/" + URLEncoder.encode(raster, "UTF-8") + "/" + z + "/" + x + "/" + y  + "." + format);
    ClientResponse resp = wr.get(ClientResponse.class);

    Assert.assertEquals(400, resp.getStatus());
    Assert.assertEquals("Unsupported image format - gif", resp.getEntity(String.class));
  }

  @Test
  @Category(UnitTest.class)
  public void testGetTileMissingRasterTileMap() throws Exception {
    String version = "1.0.0";
    String raster = new String("nonexistantpyramid");

    ExecutionException exception = mock(ExecutionException.class);
    when(request.getRequestURL()).thenReturn(new StringBuffer("http://localhost:9998/tms/1.0.0/nonexistantpyramid"));
    when(service.getMetadata("nonexistantpyramid")).thenThrow(exception);

    WebResource webResource = resource();
    WebResource wr =  webResource.path("tms" + "/" + version + "/" + URLEncoder.encode(raster, "UTF-8"));
    ClientResponse resp = wr.get(ClientResponse.class);

    Assert.assertEquals(404, resp.getStatus());
    Assert.assertEquals("Tile map not found - " + raster, resp.getEntity(String.class));

    verify(request, times(1)).getRequestURL();
    verify(service, times(1)).getMetadata("nonexistantpyramid");
    verifyNoMoreInteractions(request, service);
  }

  @Test
  @Category(UnitTest.class)
  public void testGetTileMissingRasterTile() throws Exception {
    String version = "1.0.0";
    String raster = new String("nonexistantpyramid");
    int x = 710;
    int y = 350;
    int z = 10;
    String format = "jpg";

    ExecutionException exception = mock(ExecutionException.class);
    when(request.getRequestURL()).thenReturn(new StringBuffer("http://localhost:9998/tms/1.0.0/nonexistantpyramid"));
    when(service.getMetadata("nonexistantpyramid")).thenThrow( exception );

    WebResource webResource = resource();
    WebResource wr =  webResource.path("tms" + "/" + version + "/" + URLEncoder.encode(raster, "UTF-8") + "/" + z + "/" + x + "/" + y  + "." + format);
    ClientResponse resp = wr.get(ClientResponse.class);

    Assert.assertEquals(404, resp.getStatus());
    Assert.assertEquals("Tile map not found - " + new String(raster), resp.getEntity(String.class));

    verify(request, times(0)).getRequestURL();
    verifyNoMoreInteractions(request, service);
  }

  @Test
  @Category(UnitTest.class)
  public void testGetTileOutOfBounds() throws Exception {
    String version = "1.0.0";
    String raster = astersmall_nopyramids_abs;
    int x = 846;
    int y = 411;
    int z = 12;
    String format = "png";

    when(service.getMetadata(raster)).thenReturn( getMetadata(raster) );
    when(service.getPyramid(raster)).thenReturn( getPyramid(raster) );

    WebResource webResource = resource();
    WebResource wr =  webResource.path("tms" + "/" + version + "/" + URLEncoder.encode(raster, "UTF-8") + "/" + z + "/" + x + "/" + y  + "." + format);
    ClientResponse resp = wr.get(ClientResponse.class);

    processImageResponse(resp, format);

//    Assert.assertEquals(200, resp.getStatus());
//
//    MultivaluedMap<String, String> headers = resp.getHeaders();
//    Assert.assertEquals("[image/png]", headers.get("Content-Type").toString());
//    InputStream is = resp.getEntityInputStream();
//    ImageInputStream imageInputStream = ImageIO.createImageInputStream(is);
//    ImageReader reader = ImageUtils.createImageReader("image/png");
//    reader.setInput(imageInputStream, false);
//    BufferedImage outputImg = reader.read(0);
//    BufferedImage goldenImg = ImageIO.read(new File(input + "411.png"));
//    TestUtils.compareRenderedImages(goldenImg, outputImg);
//    is.close();
//
//    verify(service, times(1)).getMetadata(raster);
//    verifyNoMoreInteractions(request, service);
  }


  @Test
  @Category(UnitTest.class)
  public void testGetTileNonexistanPyramidLevel() throws Exception {
    String version = "1.0.0";
    String raster = astersmall_nopyramids_abs;
    int x = 710;
    int y = 350;
    int z = 10;
    String format = "jpg";

    when(service.getMetadata(raster)).thenReturn( getMetadata(raster) );

    WebResource webResource = resource();
    WebResource wr =  webResource.path("tms" + "/" + version + "/" + URLEncoder.encode(raster, "UTF-8") + "/" + z + "/" + x + "/" + y  + "." + format);
    ClientResponse resp = wr.get(ClientResponse.class);

    Assert.assertEquals(404, resp.getStatus());
    Assert.assertEquals("Tile map not found - " + raster + ": 10", 
      resp.getEntity(String.class));

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

    when(service.getMetadata(raster)).thenReturn( getMetadata(raster) );
    when(service.getPyramid(raster)).thenReturn( getPyramid( raster ));

    WebResource webResource = resource();
    WebResource wr =  webResource.path("tms" + "/" + version + "/" + URLEncoder.encode(raster, "UTF-8") + "/" + z + "/" + x + "/" + y  + "." + format);
    ClientResponse resp = wr.get(ClientResponse.class);
    
    processImageResponse(resp, format);

//    MultivaluedMap<String, String> headers = resp.getHeaders();
//    Assert.assertEquals("[image/png]", headers.get("Content-Type").toString());
//    InputStream is = resp.getEntityInputStream();
//    ImageInputStream imageInputStream = ImageIO.createImageInputStream(is);
//    reader.setInput(imageInputStream, false);
//    BufferedImage outputImg = reader.read(0);
//    BufferedImage goldenImg = ImageIO.read(new File(input + "5667.png"));
//    TestUtils.compareRenderedImages(goldenImg, outputImg);
//    IOUtils.closeQuietly(is);
//
//    verify(service, times(1)).getMetadata(raster);
//    verify(service, times(1)).getColorScale(anyString(), anyString(), anyString(), (ColorScaleInfo.Builder) any());
//    verify(service, times(1)).getPyramid(raster);
//    verifyNoMoreInteractions(request, service);
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

    when(service.getMetadata(raster)).thenReturn( getMetadata(raster) );
    when(service.getPyramid(raster)).thenReturn( getPyramid( raster ));

    WebResource webResource = resource();
    WebResource wr =  webResource.path("tms" + "/" + version + "/" + URLEncoder.encode(raster, "UTF-8") + "/" + z + "/" + x + "/" + y  + "." + format);
    ClientResponse resp = wr.get(ClientResponse.class);

    processImageResponse(resp, format);

//    Assert.assertEquals(Status.OK.getStatusCode(), resp.getStatus());
//
//    MultivaluedMap<String, String> headers = resp.getHeaders();
//    Assert.assertEquals("[image/jpeg]", headers.get("Content-Type").toString());
//    InputStream is = resp.getEntityInputStream();
//    ImageInputStream imageInputStream = ImageIO.createImageInputStream(is);
//    reader.setInput(imageInputStream, false);
//    BufferedImage outputImg = reader.read(0);
//
//    BufferedImage goldenImg = ImageIO.read(new File(input + "5667.jpg"));
//    TestUtils.compareRenderedImages(goldenImg, outputImg);
//    IOUtils.closeQuietly(is);
//
//    verify(service, times(1)).getMetadata(raster);
//    verify(service, times(1)).getColorScale(anyString(), anyString(), anyString(), (ColorScaleInfo.Builder) any());
//    verify(service, times(1)).getPyramid(raster);
//    verifyNoMoreInteractions(request, service);
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

    when(service.getMetadata(raster)).thenReturn( getMetadata(raster) );
    when(service.getPyramid(raster)).thenReturn( getPyramid( raster ));

    WebResource webResource = resource();
    WebResource wr =  webResource.path("tms" + "/" + version + "/" + URLEncoder.encode(raster, "UTF-8") + "/" + z + "/" + x + "/" + y  + "." + format);
    ClientResponse resp = wr.get(ClientResponse.class);
   
    processImageResponse(resp, format);

//    Assert.assertEquals(Status.OK.getStatusCode(), resp.getStatus());
//
//    MultivaluedMap<String, String> headers = resp.getHeaders();
//    Assert.assertEquals("[image/tiff]", headers.get("Content-Type").toString());
//    InputStream is = resp.getEntityInputStream();
//    ImageInputStream imageInputStream = ImageIO.createImageInputStream(is);
//    ImageReader reader = ImageUtils.createImageReader("image/tiff");
//    reader.setInput(imageInputStream, false);
//    BufferedImage outputImg = reader.read(0);
//    BufferedImage goldenImg = ImageIO.read(new File(input + "5667.tif"));
//    TestUtils.compareRenderedImages(goldenImg, outputImg);
//    IOUtils.closeQuietly(is);
//
//    verify(service, times(1)).getMetadata(raster);
//    verify(service, times(1)).getColorScale(anyString(), anyString(), anyString(), (ColorScaleInfo.Builder) any());
//    verify(service, times(1)).getPyramid(raster);
//    verifyNoMoreInteractions(request, service);
  }

  // There are no longer any default colors scales defined in a pyramid
//  @Test
//  @Category(UnitTest.class)
//  public void testGetTileDefColorScalePng() throws Exception
//  {
//    String version = "1.0.0";
//    String raster = astersmall_nopyramids_abs;
//    int x = 2846;
//    int y = 1411;
//    int z = 12;
//    String format = "png";
//
//    when(service.getMetadata(raster)).thenReturn(getMetadata(raster));
//    when(csService.getColorScale(anyString(), anyString(), anyString(), (ColorScaleInfo.Builder) any())).thenAnswer(new Answer<ColorScale>() {
//        @Override
//        public ColorScale answer(InvocationOnMock invocationOnMock) throws Throwable {
//            Object[] args = invocationOnMock.getArguments();
//            ColorScaleInfo.Builder builder = (ColorScaleInfo.Builder) args[3];
//            return csService.getColorScale((String) args[0], (String) args[1], (String) args[2], builder);
//        }
//    });
//    when(service.getPyramid(raster)).thenReturn(getPyramid(raster));
//
//    WebResource webResource = resource();
//    WebResource wr =  webResource.path("tms" + "/" + version + "/" + URLEncoder.encode(raster, "UTF-8") + "/" + z + "/" + x + "/" + y  + "." + format);
//    ClientResponse resp = wr.get(ClientResponse.class);
//
//    processImageResponse(resp, format);
//
////    Assert.assertEquals(Status.OK.getStatusCode(), resp.getStatus());
////
////    MultivaluedMap<String, String> headers = resp.getHeaders();
////    Assert.assertEquals("[image/png]", headers.get("Content-Type").toString());
////    InputStream is = resp.getEntityInputStream();
////    ImageInputStream imageInputStream = ImageIO.createImageInputStream(is);
////    ImageReader reader = ImageUtils.createImageReader("image/png");
////    reader.setInput(imageInputStream, false);
////    BufferedImage outputImg = reader.read(0);
////    BufferedImage goldenImg = ImageIO.read(new File(input + "1411.png"));
////    TestUtils.compareRenderedImages(goldenImg, outputImg);
////    IOUtils.closeQuietly(is);
////
////    verify(service, times(1)).getMetadata(raster);
////    verify(service, times(1)).getColorScale(anyString(), anyString(), anyString(), (ColorScaleInfo.Builder) any());
////    verify(service, times(1)).getPyramid(raster);
////    verifyNoMoreInteractions(request, service);
//  }

  // There are no longer any default colors scales defined in a pyramid
//  @Test
//  @Category(UnitTest.class)
//  public void testGetTileDefColorScaleJpg() throws Exception
//  {
//    String version = "1.0.0";
//    String raster = astersmall_nopyramids_abs;
//    int x = 2846;
//    int y = 1411;
//    int z = 12;
//    String format = "jpg";
//
//    when(service.getMetadata(raster)).thenReturn(getMetadata(raster));
//    when(csService.getColorScale(anyString(), anyString(), anyString(), (ColorScaleInfo.Builder) any())).thenAnswer(new Answer<ColorScale>() {
//        @Override
//        public ColorScale answer(InvocationOnMock invocationOnMock) throws Throwable {
//            Object[] args = invocationOnMock.getArguments();
//            ColorScaleInfo.Builder builder = (ColorScaleInfo.Builder) args[3];
//            return csService.getColorScale((String) args[0], (String) args[1], (String) args[2], builder);
//        }
//    });
//    when(service.getPyramid(raster)).thenReturn( getPyramid( raster ));
//
//    WebResource webResource = resource();
//    WebResource wr =  webResource.path("tms" + "/" + version + "/" + URLEncoder.encode(raster, "UTF-8") + "/" + z + "/" + x + "/" + y  + "." + format);
//    ClientResponse resp = wr.get(ClientResponse.class);
//
//    processImageResponse(resp, format);
//
////    Assert.assertEquals(Status.OK.getStatusCode(), resp.getStatus());
////
////    MultivaluedMap<String, String> headers = resp.getHeaders();
////    Assert.assertEquals("[image/jpeg]", headers.get("Content-Type").toString());
////    InputStream is = resp.getEntityInputStream();
////    ImageInputStream imageInputStream = ImageIO.createImageInputStream(is);
////    ImageReader reader = ImageUtils.createImageReader("image/jpeg");
////    reader.setInput(imageInputStream, false);
////    BufferedImage outputImg = reader.read(0);
////    BufferedImage goldenImg = ImageIO.read(new File(input + "1411.jpg"));
////    TestUtils.compareRenderedImages(goldenImg, outputImg);
////    IOUtils.closeQuietly(is);
////
////    verify(service, times(1)).getMetadata(raster);
////    verify(service, times(1)).getColorScale(anyString(), anyString(), anyString(), (ColorScaleInfo.Builder) any());
////    verify(service, times(1)).getPyramid(raster);
////    verifyNoMoreInteractions(request, service);
//  }

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

    when(service.getMetadata(raster)).thenReturn( getMetadata(raster) );
    when(service.getPyramid(raster)).thenReturn( getPyramid( raster ));

    WebResource webResource = resource();
    WebResource wr =  webResource.path("tms" + "/" + version + "/" + URLEncoder.encode(raster, "UTF-8") + "/" + z + "/" + x + "/" + y  + "." + format);
    ClientResponse resp = wr.get(ClientResponse.class);

    processImageResponse(resp, format);

//    Assert.assertEquals(Status.OK.getStatusCode(), resp.getStatus());
//
//    MultivaluedMap<String, String> headers = resp.getHeaders();
//    Assert.assertEquals("[image/tiff]", headers.get("Content-Type").toString());
//    InputStream is = resp.getEntityInputStream();
//    ImageInputStream imageInputStream = ImageIO.createImageInputStream(is);
//    ImageReader reader = ImageUtils.createImageReader("image/tiff");
//    reader.setInput(imageInputStream, false);
//    BufferedImage outputImg = reader.read(0);
//    BufferedImage goldenImg = ImageIO.read(new File(input + "1411.tif"));
//    TestUtils.compareRenderedImages(goldenImg, outputImg);
//    IOUtils.closeQuietly(is);
//
//    verify(service, times(1)).getMetadata(raster);
//    verify(service, times(1)).getColorScale(anyString(), anyString(), anyString(), (ColorScaleInfo.Builder) any());
//    verify(service, times(1)).getPyramid(raster);
//    verifyNoMoreInteractions(request, service);
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
    String json = "{\"Interpolate\":\"true\",\"ForceValuesIntoRange\":\"false\",\"ReliefShading\":\"false\",\"NullColor\":{\"color\":\"0,0,0\",\"opacity\":\"0\"},\"Colors\":[{\"color\":\"255,0,0\",\"value\":\"0.0\"},{\"color\":\"255,255,0\",\"value\":\"0.25\"},{\"color\":\"0,255,255\",\"value\":\"0.75\"},{\"color\":\"255,255,255\",\"value\":\"1.0\"}],\"Scaling\":\"MinMax\"}";

    MultivaluedMap<String, String> params = new MultivaluedMapImpl();
    params.add("color-scale", URLEncoder.encode(json, "UTF-8"));

    when(service.getMetadata(raster)).thenReturn( getMetadata(raster) );
    when(service.getPyramid(raster)).thenReturn( getPyramid( raster ));

    WebResource webResource = resource();
    WebResource wr =  webResource.path("tms" + "/" + version + "/" + URLEncoder.encode(raster, "UTF-8") + "/" + z + "/" + x + "/" + y  + "." + format).queryParams(params);
    ClientResponse resp = wr.get(ClientResponse.class);

    processImageResponse(resp, format);

//    Assert.assertEquals(Status.OK.getStatusCode(), resp.getStatus());
//
//    MultivaluedMap<String, String> headers = resp.getHeaders();
//    Assert.assertEquals("[image/png]", headers.get("Content-Type").toString());
//    InputStream is = resp.getEntityInputStream();
//    ImageInputStream imageInputStream = ImageIO.createImageInputStream(is);
//    ImageReader reader = ImageUtils.createImageReader("image/png");
//    reader.setInput(imageInputStream, false);
//    BufferedImage outputImg = reader.read(0);
//    BufferedImage goldenImg = ImageIO.read(new File(input + "1411json.png"));
//    TestUtils.compareRenderedImages(goldenImg, outputImg);
//    IOUtils.closeQuietly(is);
//
//    verify(service, times(1)).getMetadata(raster);
//    verify(service, times(1)).getColorScale(anyString(), anyString(), anyString(), (ColorScaleInfo.Builder) any());
//    verify(service, times(1)).getPyramid(raster);
//    verifyNoMoreInteractions(request, service);
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

    MultivaluedMap<String, String> params = new MultivaluedMapImpl();
    params.add("color-scale", URLEncoder.encode(json, "UTF-8"));

    WebResource webResource = resource();
    WebResource wr =  webResource.path("tms" + "/" + version + "/" + URLEncoder.encode(raster, "UTF-8") + "/" + z + "/" + x + "/" + y  + "." + format).queryParams(params);
    ClientResponse resp = wr.get(ClientResponse.class);

    Assert.assertEquals(Status.NOT_FOUND.getStatusCode(), resp.getStatus());
    Assert.assertEquals("Unable to parse color scale JSON", resp.getEntity(String.class));
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
    String json = "{\"Interpolate\":\"true\",\"ForceValuesIntoRange\":\"false\",\"ReliefShading\":\"false\",\"NullColor\":{\"color\":\"0,0,0\",\"opacity\":\"0\"},\"Colors\":[{\"color\":\"255,0,0\",\"value\":\"0.0\"},{\"color\":\"255,255,0\",\"value\":\"0.25\"},{\"color\":\"0,255,255\",\"value\":\"0.75\"},{\"color\":\"255,255,255\",\"value\":\"1.0\"}],\"Scaling\":\"MinMax\"}";
    int min = 3000;
    int max = 4000;

    when(service.getMetadata(raster)).thenReturn( getMetadata(raster) );
    when(service.getPyramid(raster)).thenReturn( getPyramid( raster ));

    MultivaluedMap<String, String> params = new MultivaluedMapImpl();
    params.add("color-scale", URLEncoder.encode(json, "UTF-8"));
    params.add("min", String.valueOf(min));
    params.add("max", String.valueOf(max));

    WebResource webResource = resource();
    WebResource wr =  webResource.path("tms" + "/" + version + "/" + URLEncoder.encode(raster, "UTF-8") + "/" + z + "/" + x + "/" + y  + "." + format).queryParams(params);
    ClientResponse resp = wr.get(ClientResponse.class);

    processImageResponse(resp, format);

//    Assert.assertEquals(Status.OK.getStatusCode(), resp.getStatus());
//
//    MultivaluedMap<String, String> headers = resp.getHeaders();
//    Assert.assertEquals("[image/png]", headers.get("Content-Type").toString());
//    InputStream is = resp.getEntityInputStream();
//    ImageInputStream imageInputStream = ImageIO.createImageInputStream(is);
//    ImageReader reader = ImageUtils.createImageReader("image/png");
//    reader.setInput(imageInputStream, false);
//    BufferedImage outputImg = reader.read(0);
//    BufferedImage goldenImg = ImageIO.read(new File(input + "1411minmax.png"));
//    TestUtils.compareRenderedImages(goldenImg, outputImg);
//    IOUtils.closeQuietly(is);
//
//    verify(service, times(1)).getMetadata(raster);
//    verify(service, times(1)).getColorScale(anyString(), anyString(), anyString(), (ColorScaleInfo.Builder) any());
//    verify(service, times(1)).getPyramid(raster);
//    verifyNoMoreInteractions(request, service);
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

    when(service.getMetadata(raster)).thenReturn( getMetadata(raster) );
    when(service.getPyramid(raster)).thenReturn( getPyramid( raster ));

    MultivaluedMap<String, String> params = new MultivaluedMapImpl();
    params.add("color-scale-name", colorScaleName);

    WebResource webResource = resource();
    WebResource wr =  webResource.path("tms" + "/" + version + "/" + URLEncoder.encode(raster, "UTF-8") + "/" + z + "/" + x + "/" + y  + "." + format).queryParams(params);
    ClientResponse resp = wr.get(ClientResponse.class);

    processImageResponse(resp, format);

//    Assert.assertEquals(Status.OK.getStatusCode(), resp.getStatus());
//
//    MultivaluedMap<String, String> headers = resp.getHeaders();
//    Assert.assertEquals("[image/png]", headers.get("Content-Type").toString());
//    InputStream is = resp.getEntityInputStream();
//    ImageInputStream imageInputStream = ImageIO.createImageInputStream(is);
//    ImageReader reader = ImageUtils.createImageReader("image/png");
//    reader.setInput(imageInputStream, false);
//    BufferedImage outputImg = reader.read(0);
//    BufferedImage goldenImg = ImageIO.read(new File(input + "1411.png"));
//    TestUtils.compareRenderedImages(goldenImg, outputImg);
//    IOUtils.closeQuietly(is);
//
//    verify(service, times(1)).getMetadata(raster);
//    verify(service, times(1)).getColorScale(anyString(), anyString(), anyString(), (ColorScaleInfo.Builder) any());
//    verify(service, times(1)).getPyramid(raster);
//    verifyNoMoreInteractions(request, service);
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

//    when(service.getMetadata(raster)).thenReturn( getMetadata(raster) );
//    when(service.getColorScale(anyString(), anyString(), anyString(), (ColorScaleInfo.Builder) any())).thenAnswer(new Answer<ColorScale>() {
//                    @Override
//                    public ColorScale answer(InvocationOnMock invocationOnMock) throws Throwable {
//                        Object[] args = invocationOnMock.getArguments();
//                        ColorScaleInfo.Builder builder = (ColorScaleInfo.Builder)args[3];
//                        return csService.getColorScale((String)args[0], (String)args[1], (String)args[2], builder);
//                    }
//      });
//    when(service.getPyramid(raster)).thenReturn( getPyramid( raster ));

    MultivaluedMap<String, String> params = new MultivaluedMapImpl();
    params.add("color-scale-name", colorScaleName);

    WebResource webResource = resource();
    WebResource wr =  webResource.path("tms" + "/" + version + "/" + URLEncoder.encode(raster, "UTF-8") + "/" + z + "/" + x + "/" + y  + "." + format).queryParams(params);
    ClientResponse resp = wr.get(ClientResponse.class);

    Assert.assertEquals(Status.NOT_FOUND.getStatusCode(), resp.getStatus());
    Assert.assertEquals("Unable to open color scale file", resp.getEntity(String.class));

//    verify(service, times(1)).getMetadata(raster);
//    verify(service, times(1)).getColorScale(anyString(), anyString(), anyString(), (ColorScaleInfo.Builder) any());
//    verify(service, times(0)).getPyramid(raster);
//    verifyNoMoreInteractions(request, service);
  }


  protected static void processImageResponse(final ClientResponse response, final String extension)
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
