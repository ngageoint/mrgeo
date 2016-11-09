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

package org.mrgeo.resources.mrspyramid;

import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.core.DefaultResourceConfig;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import com.sun.jersey.spi.inject.SingletonTypeInjectableProvider;
import com.sun.jersey.test.framework.JerseyTest;
import com.sun.jersey.test.framework.LowLevelAppDescriptor;
import com.sun.jersey.test.framework.spi.container.TestContainerFactory;
import junit.framework.Assert;
import org.apache.commons.io.IOUtils;
import org.custommonkey.xmlunit.Diff;
import org.custommonkey.xmlunit.XMLUnit;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.mrgeo.FilteringInMemoryTestContainerFactory;
import org.mrgeo.colorscale.ColorScale;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.raster.MrGeoRaster;
import org.mrgeo.image.MrsPyramid;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.services.mrspyramid.MrsPyramidService;
import org.mrgeo.services.mrspyramid.rendering.ImageRenderer;
import org.mrgeo.services.mrspyramid.rendering.ImageResponseWriter;
import org.mrgeo.utils.tms.Bounds;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.StringWriter;

import static junit.framework.Assert.assertEquals;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RasterResourceTest extends JerseyTest
{
//private static final Logger log = LoggerFactory.getLogger(RasterResourceTest.class);

private MrsPyramidService service;
private HttpServletRequest request;

@Override
protected LowLevelAppDescriptor configure()
{
  DefaultResourceConfig resourceConfig = new DefaultResourceConfig();
  service = mock(MrsPyramidService.class);
  request = mock(HttpServletRequest.class);

  resourceConfig.getSingletons().add( new SingletonTypeInjectableProvider<Context, MrsPyramidService>(MrsPyramidService.class, service) {});
  resourceConfig.getSingletons().add( new SingletonTypeInjectableProvider<Context, HttpServletRequest>(HttpServletRequest.class, request) {});

  resourceConfig.getClasses().add(RasterResource.class);
  //resourceConfig.getClasses().add(JacksonJsonProvider.class);

  return new LowLevelAppDescriptor.Builder( resourceConfig ).build();
}

@Override
protected TestContainerFactory getTestContainerFactory()
{
  return new FilteringInMemoryTestContainerFactory();
}

@Override
public void setUp() throws Exception
{
  super.setUp();
  Mockito.reset(service, request);
}

//@Test
//@Category(UnitTest.class)
//public void testCreateJob() throws Exception
//{
//  JobManager jobManager = mock(JobManager.class);
//  when(jobManager.submitJob(anyString(), (MapAlgebraJob) any())).thenReturn(5L);
//  when(jobManager.getJob(Mockito.anyLong())).thenAnswer(new Answer() {
//    public Object answer(InvocationOnMock invocation) {
//      JobDetails details = mock(JobDetails.class);
//      when(details.getJobId()).thenReturn(5L);
//      return details;
//    }
//  });
//
//  when(service.getJobManager()).thenReturn(jobManager);
////    when(service.getOutputImageStr(anyString(), anyString())).thenReturn("/foo/bar");
//  when(request.getScheme()).thenReturn("http");
//  when(request.getHeader(anyString())).thenReturn(null);
//
//  final WebResource webResource = resource();
//  final String ma = ("a = [some points]; RasterizeVector(a, \"LAST\", 0.3, \"c\") ");
//
//  final JobInfoResponse res = webResource.path("raster/testCreateJob/mapalgebra")
//      .queryParam("basePath", "/foo/bar").type(MediaType.TEXT_PLAIN)
//      .put(JobInfoResponse.class, ma);
//  Assert.assertTrue(res.getJobId() != 0);
//  Assert.assertTrue(res.getStatusUrl().startsWith("http:"));
//  final long jobId = res.getJobId();
//  assertEquals(jobId, 5L);
//}

//@Test
//@Category(UnitTest.class)
//public void testCreateJobHttpsStatus() throws Exception
//{
//  JobManager jobManager = mock(JobManager.class);
//  when(jobManager.submitJob(anyString(), (MapAlgebraJob) any())).thenReturn(5L);
//  when(jobManager.getJob(Mockito.anyLong())).thenAnswer(new Answer() {
//    public Object answer(InvocationOnMock invocation) {
//      JobDetails details = mock(JobDetails.class);
//      when(details.getJobId()).thenReturn(5L);
//      return details;
//    }
//  });
//
//  when(service.getJobManager()).thenReturn(jobManager);
////    when(service.getOutputImageStr(anyString(), anyString())).thenReturn("/foo/bar");
//  when(request.getScheme()).thenReturn("http");
//  when(request.getHeader(anyString())).thenReturn("on");
//
//  final WebResource webResource = resource();
//  final String ma = "a = [some points]; RasterizeVector(a, \"LAST\", 0.3, \"c\") ";
//
//  final JobInfoResponse res = webResource.path("raster/testCreateJob/mapalgebra")
//      .queryParam("basePath", "/foo/bar").type(MediaType.TEXT_PLAIN)
//      .header("X-Forwarded-SSL", "on").put(JobInfoResponse.class, ma);
//  Assert.assertTrue(res.getJobId() != 0);
//  Assert.assertTrue("Status URL does not start with 'https:', was '" + res.getStatusUrl() + "'",
//      res.getStatusUrl().startsWith("https:"));
//
//  // we only care about the status URL being changed
//  // so no further assertions
//}

private String returnXml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
    "<kml xmlns=\"http://www.opengis.net/kml/2.2\">\n" +
    "<Document>\n" +
    "<Region>\n" +
    "  <LatLonAltBox>\n" +
    "    <north>-17.0</north>\n" +
    "    <south>-18.0</south>\n" +
    "    <east>142.0</east>\n" +
    "    <west>141.0</west>\n" +
    "  </LatLonAltBox>\n" +
    "  <Lod>\n" +
    "    <minLodPixels>256</minLodPixels>\n" +
    "    <maxLodPixels>-1</maxLodPixels>\n" +
    "  </Lod>\n" +
    "</Region>\n" +
    "<NetworkLink>\n" +
    "  <name>0</name>\n" +
    "  <Region>\n" +
    "    <LatLonAltBox>\n" +
    "      <north>-17.0</north>\n" +
    "      <south>-18.0</south>\n" +
    "      <east>142.0</east>\n" +
    "      <west>141.0</west>\n" +
    "   </LatLonAltBox>\n" +
    "   <Lod>\n" +
    "     <minLodPixels>256</minLodPixels>\n" +
    "     <maxLodPixels>-1</maxLodPixels>\n" +
    "   </Lod>\n" +
    " </Region>\n" +
    " <Link>\n" +
    "<href>{BASE.URL}KmlGenerator?LAYERS=/mrgeo/test-files/org.mrgeo.resources.mrspyramid/RasterResourceTest/all-ones&amp;SERVICE=kml&amp;REQUEST=getkmlnode&amp;WMSHOST={WMSHOST}&amp;BBOX=&amp;LEVELS=10&amp;RESOLUTION=512&amp;NODEID=0,0</href>\n" +
    "   <viewRefreshMode>onRegion</viewRefreshMode>\n" +
    " </Link>\n" +
    "</NetworkLink>\n" +
    "</Document>\n" +
    "</kml>\n";

@Test
@Category(UnitTest.class)
public void testGetImageAsKML() throws Exception
{
  MrsPyramid pyramidMock = mock(MrsPyramid.class);
  when(service.getPyramid(anyString(), (ProviderProperties) any())).thenReturn(pyramidMock);
  when(service.renderKml(anyString(), (Bounds) any(), anyInt(), anyInt(), (ColorScale) any(), anyInt(), (ProviderProperties)anyObject()))
      .thenReturn(Response.ok().entity(returnXml).type("application/vnd.google-earth.kml+xml").build());

  final WebResource webResource = resource();
  // retrieve kml
  final MultivaluedMap<String, String> params = new MultivaluedMapImpl();
  params.add("format", "kml");
  params.add("bbox", "141, -18, 142, -17");
  final ClientResponse resp = webResource.path("raster/some/raster/path")
      .queryParams(params)
      .get(ClientResponse.class);
  final MultivaluedMap<String, String> headers = resp.getHeaders();
  assertEquals("[application/vnd.google-earth.kml+xml]", headers.get("Content-Type")
      .toString());
  final InputStream is = resp.getEntityInputStream();
  final StringWriter writer = new StringWriter();
  IOUtils.copy(is, writer);
  XMLUnit.setIgnoreWhitespace(true);
  Diff xmlDiff = new Diff(writer.toString(), returnXml);
  Assert.assertTrue("XML return is not similiar to source", xmlDiff.similar());
}

@Test
@Category(UnitTest.class)
public void testGetImageBadColorScaleName() throws Exception
{
  MrsPyramid pyramidMock = mock(MrsPyramid.class);
  when(service.getPyramid(anyString(), (ProviderProperties) any())).thenReturn(pyramidMock);
  when(service.getColorScaleFromName(anyString())).thenAnswer(new Answer() {
    public Object answer(InvocationOnMock invocation) throws FileNotFoundException {
      String builder = "File does not exist: " + "/mrgeo/color-scales/" + invocation.getArguments()[0] + ".xml";
      throw new FileNotFoundException(builder);
    }
  });
  when(service.renderKml(anyString(), (Bounds) any(), anyInt(), anyInt(), (ColorScale) any(), anyInt(), (ProviderProperties)anyObject()))
      .thenReturn(Response.ok().entity(returnXml).type("application/vnd.google-earth.kml+xml").build());

  final WebResource webResource = resource();

  // retrieve jpg
  final MultivaluedMap<String, String> params = new MultivaluedMapImpl();
  params.add("format", "png");
  params.add("width", "400");
  params.add("height", "200");
  params.add("bbox", "142,-18, 143,-17");
  params.add("color-scale-name", "bad-color-scale");

  final WebResource wr = webResource.path("raster/some/raster/path").queryParams(params);
  final ClientResponse resp = wr.get(ClientResponse.class);
  assertEquals(400, resp.getStatus());
  assertEquals("File does not exist: /mrgeo/color-scales/bad-color-scale.xml",
      resp.getEntity(String.class));
}

@Test
@Category(UnitTest.class)
public void testGetImageBadFormat() throws Exception
{
  MrsPyramid pyramidMock = mock(MrsPyramid.class);
  when(pyramidMock.getBounds()).thenReturn(Bounds.fromCommaString("142,-18, 143,-17"));
  when(service.getPyramid((String) any(), (ProviderProperties) any())).thenReturn(pyramidMock);
  when(service.getImageRenderer(anyString())).thenThrow(new IllegalArgumentException("INVALID FORMAT"));
  final WebResource webResource = resource();

  // retrieve jpg
  final MultivaluedMap<String, String> params = new MultivaluedMapImpl();
  params.add("format", "png-invalid");
  params.add("width", "400");
  params.add("height", "200");
  params.add("bbox", "142,-18, 143,-17");

  final WebResource wr = webResource.path("raster/foo/bar").queryParams(params);
  final ClientResponse resp = wr.get(ClientResponse.class);
  assertEquals(400, resp.getStatus());
  assertEquals("Unsupported image format - png-invalid", resp.getEntity(String.class));
}

@Test
@Category(UnitTest.class)
public void testGetImageNotExist() throws Exception
{
  when(service.renderKml(anyString(), (Bounds) any(), anyInt(), anyInt(), (ColorScale) any(), anyInt(), (ProviderProperties)anyObject()))
      .thenReturn(Response.ok().entity(returnXml).type("application/vnd.google-earth.kml+xml").build());
  final WebResource webResource = resource();
  final String resultImage = "badpathtest";
  // deleteMrsPyramid(resultImage);

  final MultivaluedMap<String, String> params = new MultivaluedMapImpl();
  params.add("format", "png");
  final ClientResponse resp = webResource.path("raster/" + resultImage).queryParams(params)
      .get(ClientResponse.class);
  assertEquals(404, resp.getStatus());
  assertEquals(resultImage + " not found", resp.getEntity(String.class));
}

@Test
@Category(UnitTest.class)
public void testGetImagePng() throws Exception
{
  String typ = "image/png";
  MrsPyramid pyramidMock = mock(MrsPyramid.class);
  when(pyramidMock.getBounds()).thenReturn(Bounds.fromCommaString("142,-18, 143,-17"));
  when(service.getPyramid((String) any(), (ProviderProperties) any())).thenReturn(pyramidMock);
  when(service.getColorScaleFromName(anyString())).thenReturn(null);
  ImageRenderer renderer = mock(ImageRenderer.class);
  // TODO: Source a legit PNG
  //Mockito.when(renderer.getMimeType()).thenReturn( typ );
  when(renderer.renderImage(anyString(), (Bounds) any(), anyInt(), anyInt(), (ProviderProperties)anyObject(), anyString()))
      .thenReturn(null); // Nothing to return, we aren't validating response

  when(service.getImageRenderer(anyString())).thenReturn(renderer);

  ImageResponseWriter writer = mock(ImageResponseWriter.class);
  //Mockito.when(writer.getMimeType()).thenReturn( typ );
  when(writer.write((MrGeoRaster) any(), anyString(), (Bounds) any())).thenReturn(Response.ok().type( typ ));

  when(service.getImageResponseWriter(anyString())).thenReturn(writer);
  final WebResource webResource = resource();

  // retrieve jpg
  final MultivaluedMap<String, String> params = new MultivaluedMapImpl();
  params.add("format", "png");
  params.add("width", "400");
  params.add("height", "200");
  params.add("bbox", "142,-18, 143,-17");

  final WebResource wr = webResource.path("raster/foo/bar").queryParams(params);

  final ClientResponse resp = wr.get(ClientResponse.class);
  final MultivaluedMap<String, String> headers = resp.getHeaders();
  assertEquals("[image/png]", headers.get("Content-Type").toString());
//    final InputStream is = resp.getEntityInputStream();
//    final BufferedImage image = readImageFromStream(is, ImageUtils.createImageReader("image/png"));
//    is.close();
//
//    // note the color scale will translate the ones to 255 (white)
//    TestUtils.compareRenderedImageToConstant(image, 255, 0);
}

@Test
@Category(UnitTest.class)
public void testGetImageTiff() throws Exception
{
  String typ = "image/tiff";
  MrsPyramid pyramidMock = mock(MrsPyramid.class);
  when(pyramidMock.getBounds()).thenReturn(Bounds.fromCommaString("142,-18, 143,-17"));
  when(service.getPyramid((String) any(), (ProviderProperties) any())).thenReturn(pyramidMock);
  when(service.getColorScaleFromName(anyString())).thenReturn(null);
  ImageRenderer renderer = mock(ImageRenderer.class);
  // TODO: Source a legit TIFF
  //Mockito.when(renderer.getMimeType()).thenReturn( typ );
  when(renderer.renderImage(anyString(), (Bounds) any(), anyInt(), anyInt(), (ProviderProperties)anyObject(), anyString()))
      .thenReturn(null); // Nothing to return, we aren't validating response

  when(service.getImageRenderer(anyString())).thenReturn(renderer);

  ImageResponseWriter writer = mock(ImageResponseWriter.class);
  //Mockito.when(writer.getMimeType()).thenReturn( typ );
  when(writer.write((MrGeoRaster) any(), anyString(), (Bounds) any())).thenReturn(Response.ok().type( typ ));

  when(service.getImageResponseWriter(anyString())).thenReturn(writer);
  final WebResource webResource = resource();

  // retrieve jpg
  final MultivaluedMap<String, String> params = new MultivaluedMapImpl();
  params.add("format", "tiff");
  params.add("width", "400");
  params.add("height", "200");
  params.add("bbox", "142,-18, 143,-17");

  final WebResource wr = webResource.path("raster/foo/bar").queryParams(params);

  final ClientResponse resp = wr.get(ClientResponse.class);
  final MultivaluedMap<String, String> headers = resp.getHeaders();
  assertEquals("[image/tiff]", headers.get("Content-Type").toString());
}
}
