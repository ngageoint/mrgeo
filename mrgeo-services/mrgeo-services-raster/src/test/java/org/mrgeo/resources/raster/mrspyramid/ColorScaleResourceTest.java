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

package org.mrgeo.resources.raster.mrspyramid;

import junit.framework.Assert;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.mrgeo.colorscale.ColorScale;
import org.mrgeo.colorscale.ColorScale.ColorScaleException;
import org.mrgeo.data.raster.MrGeoRaster;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.services.mrspyramid.MrsPyramidService;
import org.mrgeo.services.mrspyramid.MrsPyramidServiceException;
import org.mrgeo.services.mrspyramid.rendering.ImageResponseWriter;
import org.mrgeo.services.mrspyramid.rendering.PngImageResponseWriter;
import org.mrgeo.test.TestUtils;
import org.mrgeo.utils.GDALUtils;
import org.w3c.dom.Document;

import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("all") // Test code, not included in production
public class ColorScaleResourceTest extends JerseyTest
{
private MrsPyramidService service;
private String input = TestUtils.composeInputDir(ColorScaleResourceTest.class);

@Override
public void setUp() throws Exception
{
  super.setUp();
  Mockito.reset(service);
}

@Test
@Category(UnitTest.class)
public void testGetAll() throws Exception
{
  List<String> retvals = new ArrayList<>();
  retvals.add("/mrgeo/test-files/output/org.mrgeo.resources.raster.mrspyramid/ColorScaleResourceTest/rainbow-transparent.xml");
  retvals.add("/mrgeo/test-files/output/org.mrgeo.resources.raster.mrspyramid/ColorScaleResourceTest/rainbow.xml");
  retvals
      .add("/mrgeo/test-files/output/org.mrgeo.resources.raster.mrspyramid/ColorScaleResourceTest/dir1/brewer-green-log.xml");
  retvals.add("/mrgeo/test-files/output/org.mrgeo.resources.raster.mrspyramid/ColorScaleResourceTest/dir1/Default.xml");
  retvals.add("/mrgeo/test-files/output/org.mrgeo.resources.raster.mrspyramid/ColorScaleResourceTest/dir1/rainbow.xml");

  Mockito.when(service.getColorScales()).thenReturn(retvals);

  ColorScaleList response = target("/colorscales").request().get(ColorScaleList.class);

  Assert.assertNotNull(response);

  List<String> fileNames = response.getFilePaths();

  assertEquals(fileNames.size(), 5);
  assertTrue(fileNames.contains(
      "/mrgeo/test-files/output/org.mrgeo.resources.raster.mrspyramid/ColorScaleResourceTest/rainbow-transparent.xml"));
  assertTrue(fileNames.contains(
      "/mrgeo/test-files/output/org.mrgeo.resources.raster.mrspyramid/ColorScaleResourceTest/rainbow.xml"));
  assertTrue(fileNames.contains(
      "/mrgeo/test-files/output/org.mrgeo.resources.raster.mrspyramid/ColorScaleResourceTest/dir1/brewer-green-log.xml"));
  assertTrue(fileNames.contains(
      "/mrgeo/test-files/output/org.mrgeo.resources.raster.mrspyramid/ColorScaleResourceTest/dir1/Default.xml"));
  assertTrue(fileNames.contains(
      "/mrgeo/test-files/output/org.mrgeo.resources.raster.mrspyramid/ColorScaleResourceTest/dir1/rainbow.xml"));
}

@Test
@Category(UnitTest.class)
public void testGetColorScaleLegendVertical() throws Exception
{
  Mockito.when(service.getColorScaleFromName(Mockito.anyString())).thenReturn(createRainbowColorScale());

  Response response = target(
      "colorscales/legend/mrgeo/test-files/output/org.mrgeo.resources.raster.mrspyramid/ColorScaleResourceTest/rainbow.xml")
      .queryParam("max", "1000")
      .request().get();

  Assert.assertEquals("text/html", response.getHeaderString("Content-Type"));

  DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
  DocumentBuilder db = dbf.newDocumentBuilder();
  Document doc = db.parse(new ByteArrayInputStream(response.readEntity(String.class).getBytes()));

  Assert.assertEquals(1, doc.getElementsByTagName("img").getLength());
  Assert.assertTrue(doc.getElementsByTagName("div").getLength() > 1);
  Assert.assertEquals(
      "mrgeo/test-files/output/org.mrgeo.resources.raster.mrspyramid/ColorScaleResourceTest/rainbow.xml?width=20&height=200",
      doc.getElementsByTagName("img").item(0).getAttributes().getNamedItem("src").getTextContent());
}

@Test
@Category(UnitTest.class)
public void testGetColorScaleLegendVerticalLikelihood() throws Exception
{
  Mockito.when(service.getColorScaleFromName(Mockito.anyString())).thenReturn(createRainbowColorScale());

  Response response = target(
      "colorscales/legend//mrgeo/test-files/output/org.mrgeo.resources.raster.mrspyramid/ColorScaleResourceTest/rainbow.xml")
      .queryParam("width", "15")
      .queryParam("height", "180")
      .queryParam("max", "0.08")
      .queryParam("units", "likelihood")
      .request().get();

  Assert.assertEquals("text/html", response.getHeaderString("Content-Type"));

  DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
  DocumentBuilder db = dbf.newDocumentBuilder();
  Document doc = db.parse(new ByteArrayInputStream(response.readEntity(String.class).getBytes()));

  Assert.assertEquals(doc.getElementsByTagName("img").getLength(), 1);
  Assert.assertTrue(doc.getElementsByTagName("div").getLength() > 1);
  Assert.assertEquals(
      "/mrgeo/test-files/output/org.mrgeo.resources.raster.mrspyramid/ColorScaleResourceTest/rainbow.xml?width=15&height=180",
      doc.getElementsByTagName("img").item(0).getAttributes().getNamedItem("src").getTextContent());
}

@Test
@Category(UnitTest.class)
public void testGetColorScaleLegendHorizontal() throws Exception
{
  Mockito.when(service.getColorScaleFromName(Mockito.anyString())).thenReturn(createRainbowColorScale());

  Response response = target(
      "colorscales/legend/mrgeo/test-files/output/org.mrgeo.resources.raster.mrspyramid/ColorScaleResourceTest/rainbow.xml")
      .queryParam("min", "0")
      .queryParam("max", "1000")
      .queryParam("units", "meters")
      .queryParam("width", "250")
      .queryParam("height", "10")
      .request().get();

  Assert.assertEquals("text/html", response.getHeaderString("Content-Type"));

  DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
  DocumentBuilder db = dbf.newDocumentBuilder();
  Document doc = db.parse(new ByteArrayInputStream(response.readEntity(String.class).getBytes()));

  Assert.assertEquals(doc.getElementsByTagName("img").getLength(), 1);
  Assert.assertTrue(doc.getElementsByTagName("div").getLength() > 1);
}

@Test
@Category(UnitTest.class)
public void testGetColorScaleLegendHorizontalLikelihood() throws Exception
{
  Mockito.when(service.getColorScaleFromName(Mockito.anyString())).thenReturn(createRainbowColorScale());

  Response response = target(
      "colorscales/legend/mrgeo/test-files/output/org.mrgeo.resources.raster.mrspyramid/ColorScaleResourceTest/rainbow.xml")
      .queryParam("min", "0")
      .queryParam("max", "1.0")
      .queryParam("units", "likelihood")
      .queryParam("width", "200")
      .queryParam("height", "10")
      .request().get();

  Assert.assertEquals("text/html", response.getHeaderString("Content-Type"));

  DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
  DocumentBuilder db = dbf.newDocumentBuilder();
  Document doc = db.parse(new ByteArrayInputStream(response.readEntity(String.class).getBytes()));

  Assert.assertEquals(doc.getElementsByTagName("img").getLength(), 1);
  Assert.assertTrue(doc.getElementsByTagName("div").getLength() > 1);
}

@Test
@Category(UnitTest.class)
public void testGetColorScaleSwatch() throws Exception
{
  Mockito.when(service.createColorScaleSwatch(Mockito.anyString(), Mockito.anyString(),
      Mockito.anyInt(), Mockito.anyInt())).thenReturn(getRaster(input, "colorswatch.png"));

  ImageResponseWriter writer = new PngImageResponseWriter();
  Mockito.when(service.getImageResponseWriter(Mockito.anyString())).thenReturn(writer);

  Response response =
      target("colorscales/mrgeo/test-files/output/org.mrgeo.resources.raster.mrspyramid/ColorScaleResourceTest/rainbow.xml")
          .queryParam("width", "100")
          .queryParam("height", "10")
          .request().get();

  Assert.assertEquals("image/png", response.getHeaderString("Content-Type"));


  MrGeoRaster raster = MrGeoRaster.fromDataset(GDALUtils.open(response.readEntity(InputStream.class)));
  MrGeoRaster golden = getRaster(input, "colorswatch.png");
  TestUtils.compareRasters(golden, raster);

  Assert.assertEquals(100, raster.width());
  Assert.assertEquals(10, raster.height());
}

@Test
@Category(UnitTest.class)
public void testGetColorScaleSwatchMissingColorScale() throws Exception
{
  Mockito.when(
      service.createColorScaleSwatch(Mockito.anyString(), Mockito.anyString(), Mockito.anyInt(), Mockito.anyInt()))
      .thenAnswer(new Answer()
      {
        public Object answer(InvocationOnMock invocation) throws MrsPyramidServiceException
        {
          throw new MrsPyramidServiceException("Base path \"" + invocation.getArguments()[0] + "\" does not exist.");
        }
      });

  ImageResponseWriter writer = new PngImageResponseWriter();
  Mockito.when(service.getImageResponseWriter(Mockito.anyString())).thenReturn(writer);

  String path = "/mrgeo/test-files/output/org.mrgeo.resources.raster.mrspyramid/ColorScaleResourceTest/missing.xml";

  Response response = target("colorscales/" + path)
      .queryParam("width", "100")
      .queryParam("height", "10")
      .request().get();

  Assert.assertNotSame("image/png", response.getHeaderString("Content-Type"));
  Assert.assertEquals(400, response.getStatus());
  Assert.assertEquals("Color scale file not found: " + path, response.readEntity(String.class));
}

@Override
protected Application configure()
{
  service = Mockito.mock(MrsPyramidService.class);

  ResourceConfig config = new ResourceConfig();
  config.register(ColorScaleResource.class);
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

private ColorScale createRainbowColorScale() throws ColorScaleException
{
  String colorScaleXml = "<ColorMap name=\"Rainbow\">\n" +
      "  <Scaling>MinMax</Scaling> <!-- Could also be Absolute -->\n" +
      "  <ReliefShading>0</ReliefShading>\n" +
      "  <Interpolate>1</Interpolate>\n" +
      "  <NullColor color=\"0,0,0\" opacity=\"0\"/>\n" +
      "  <Color value=\"0.0\" color=\"0,0,127\" opacity=\"255\"/>\n" +
      "  <Color value=\"0.2\" color=\"0,0,255\"/> <!-- if not specified an opacity defaults to 255 -->\n" +
      "  <Color value=\"0.4\" color=\"0,255,255\"/>\n" +
      "  <Color value=\"0.6\" color=\"0,255,0\"/>\n" +
      "  <Color value=\"0.8\" color=\"255,255,0\"/>\n" +
      "  <Color value=\"1.0\" color=\"255,0,0\"/>\n" +
      "</ColorMap>";
  return ColorScale.loadFromXML(new ByteArrayInputStream(colorScaleXml.getBytes()));
}

private MrGeoRaster getRaster(String dir, String file) throws IOException
{
  return MrGeoRaster.fromDataset(GDALUtils.open(new File(dir, file).getCanonicalPath()));
}
}
