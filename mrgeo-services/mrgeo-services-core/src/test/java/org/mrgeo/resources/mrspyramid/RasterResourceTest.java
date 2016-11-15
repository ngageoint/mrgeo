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

import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import junit.framework.Assert;
import org.apache.commons.io.IOUtils;
import org.custommonkey.xmlunit.Diff;
import org.custommonkey.xmlunit.XMLUnit;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.spi.TestContainerFactory;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
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
import javax.ws.rs.core.Application;
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
protected Application configure()
{
  service = Mockito.mock(MrsPyramidService.class);
  request = Mockito.mock(HttpServletRequest.class);

  ResourceConfig config = new ResourceConfig();
  config.register(RasterResource.class);
  config.register(new AbstractBinder()
  {
    @Override
    protected void configure()
    {
      bind(service).to(MrsPyramidService.class);
    }
  });
  return config;
}


@Override
public void setUp() throws Exception
{
  super.setUp();
  Mockito.reset(service, request);
}

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

  Response response = target("raster/some/raster/path")
      .queryParam("format", "kml")
      .queryParam("bbox", "141, -18, 142, -17")
      .request().get();

  Assert.assertEquals("application/vnd.google-earth.kml+xml", response.getHeaderString("Content-Type"));
  ;

  final InputStream is = response.readEntity(InputStream.class);

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

  Response response = target("raster/some/raster/path")
      .queryParam("format", "png")
      .queryParam("width", "400")
      .queryParam("height", "200")
      .queryParam("bbox", "142,-18, 143,-17")
      .queryParam("color-scale-name", "bad-color-scale")
      .request().get();

  Assert.assertEquals(400, response.getStatus());
  Assert.assertEquals("File does not exist: /mrgeo/color-scales/bad-color-scale.xml",
      response.readEntity(String.class));
}

@Test
@Category(UnitTest.class)
public void testGetImageBadFormat() throws Exception
{
  MrsPyramid pyramidMock = mock(MrsPyramid.class);
  when(pyramidMock.getBounds()).thenReturn(Bounds.fromCommaString("142,-18, 143,-17"));
  when(service.getPyramid((String) any(), (ProviderProperties) any())).thenReturn(pyramidMock);
  when(service.getImageRenderer(anyString())).thenThrow(new IllegalArgumentException("INVALID FORMAT"));

  Response response = target("raster/some/raster/path")
      .queryParam("format", "png-invalid")
      .queryParam("width", "400")
      .queryParam("height", "200")
      .queryParam("bbox", "142,-18, 143,-17")
      .request().get();

  Assert.assertEquals(400, response.getStatus());
  assertEquals("Unsupported image format - png-invalid", response.readEntity(String.class));
}

@Test
@Category(UnitTest.class)
public void testGetImageNotExist() throws Exception
{
  when(service.renderKml(anyString(), (Bounds) any(), anyInt(), anyInt(), (ColorScale) any(), anyInt(), (ProviderProperties)anyObject()))
      .thenReturn(Response.ok().entity(returnXml).type("application/vnd.google-earth.kml+xml").build());
  final String resultImage = "badpathtest";

  Response response = target("raster/" + resultImage)
      .queryParam("format", "png-invalid")
      .request().get();

  assertEquals(404, response.getStatus());
  assertEquals(resultImage + " not found", response.readEntity(String.class));
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

  when(renderer.renderImage(anyString(), (Bounds) any(), anyInt(), anyInt(), (ProviderProperties)anyObject(), anyString()))
      .thenReturn(null); // Nothing to return, we aren't validating response

  when(service.getImageRenderer(anyString())).thenReturn(renderer);

  ImageResponseWriter writer = mock(ImageResponseWriter.class);
  //Mockito.when(writer.getMimeType()).thenReturn( typ );
  when(writer.write((MrGeoRaster) any(), anyString(), (Bounds) any())).thenReturn(Response.ok().type( typ ));

  when(service.getImageResponseWriter(anyString())).thenReturn(writer);

  Response response = target("raster/foo/bar")
      .queryParam("format", "png")
      .queryParam("width", "400")
      .queryParam("height", "200")
      .queryParam("bbox", "142,-18, 143,-17")
      .request().get();

  assertEquals(typ, response.getHeaderString("Content-Type"));

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

  when(renderer.renderImage(anyString(), (Bounds) any(), anyInt(), anyInt(), (ProviderProperties)anyObject(), anyString()))
      .thenReturn(null); // Nothing to return, we aren't validating response

  when(service.getImageRenderer(anyString())).thenReturn(renderer);

  ImageResponseWriter writer = mock(ImageResponseWriter.class);
  //Mockito.when(writer.getMimeType()).thenReturn( typ );
  when(writer.write((MrGeoRaster) any(), anyString(), (Bounds) any())).thenReturn(Response.ok().type( typ ));

  when(service.getImageResponseWriter(anyString())).thenReturn(writer);

  Response response = target("raster/foo/bar")
      .queryParam("format", "tiff")
      .queryParam("width", "400")
      .queryParam("height", "200")
      .queryParam("bbox", "142,-18, 143,-17")
      .request().get();

  assertEquals(typ, response.getHeaderString("Content-Type"));
}
}
