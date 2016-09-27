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

import com.sun.jersey.api.NotFoundException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.core.DefaultResourceConfig;
import com.sun.jersey.spi.inject.SingletonTypeInjectableProvider;
import com.sun.jersey.test.framework.JerseyTest;
import com.sun.jersey.test.framework.LowLevelAppDescriptor;
import com.sun.jersey.test.framework.spi.container.TestContainerFactory;
import junit.framework.Assert;
import org.codehaus.jackson.jaxrs.JacksonJsonProvider;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.mrgeo.FilteringInMemoryTestContainerFactory;
import org.mrgeo.colorscale.ColorScale;
import org.mrgeo.colorscale.ColorScale.ColorScaleException;
import org.mrgeo.data.raster.MrGeoRaster;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.services.mrspyramid.MrsPyramidService;
import org.mrgeo.services.mrspyramid.rendering.ImageResponseWriter;
import org.mrgeo.services.mrspyramid.rendering.PngImageResponseWriter;
import org.mrgeo.test.TestUtils;
import org.mrgeo.utils.GDALUtils;
import org.w3c.dom.Document;

import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class ColorScaleResourceTest extends JerseyTest
{
//  private Path outputHdfs;
//  private String input;
//
//  public ColorScaleResourceTest() throws IOException
//  {
//    super("org.mrgeo.resources");
//
//    outputHdfs = TestUtils.composeOutputHdfs(ColorScaleResourceTest.class);
//    input = TestUtils.composeInputDir(ColorScaleResourceTest.class);
//    HadoopFileUtils.copyToHdfs(input, outputHdfs, "bad-colorscale-1.xml");
//    HadoopFileUtils.copyToHdfs(input, outputHdfs, "rainbow-transparent.xml");
//    HadoopFileUtils.copyToHdfs(input, outputHdfs, "rainbow.xml");
//
//    FileSystem fileSystem = HadoopFileUtils.getFileSystem();
//    Path subdirInput = new Path(input, "dir1");
//    Path subdirOutput = new Path(outputHdfs, "dir1");
//    if (fileSystem.exists(subdirOutput))
//    {
//      fileSystem.delete(subdirOutput, true);
//    }
//    fileSystem.mkdirs(subdirOutput);
//    HadoopFileUtils.copyToHdfs(subdirInput, subdirOutput, "bad-colorscale-2.xml");
//    HadoopFileUtils.copyToHdfs(subdirInput, subdirOutput, "bad-colorscale-3.txt");
//    HadoopFileUtils.copyToHdfs(subdirInput, subdirOutput, "brewer-green-log.xml");
//    HadoopFileUtils.copyToHdfs(subdirInput, subdirOutput, "Default.xml");
//    HadoopFileUtils.copyToHdfs(subdirInput, subdirOutput, "rainbow.xml");
//  }

    private MrsPyramidService service;

    @Override
    protected LowLevelAppDescriptor configure()
    {
        DefaultResourceConfig resourceConfig = new DefaultResourceConfig();
        service = Mockito.mock(MrsPyramidService.class);

        resourceConfig.getSingletons().add( new SingletonTypeInjectableProvider<Context, MrsPyramidService>(MrsPyramidService.class, service) {});

        resourceConfig.getClasses().add(ColorScaleResource.class);
        resourceConfig.getClasses().add(JacksonJsonProvider.class);

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
        Mockito.reset(service);
    }

  @Test
  @Category(UnitTest.class)
  public void testGetAll() throws Exception {
    List<String> retvals = new ArrayList<>();
      retvals.add("/mrgeo/test-files/output/org.mrgeo.resources.mrspyramid/ColorScaleResourceTest/rainbow-transparent.xml");
      retvals.add("/mrgeo/test-files/output/org.mrgeo.resources.mrspyramid/ColorScaleResourceTest/rainbow.xml");
      retvals.add("/mrgeo/test-files/output/org.mrgeo.resources.mrspyramid/ColorScaleResourceTest/dir1/brewer-green-log.xml");
      retvals.add("/mrgeo/test-files/output/org.mrgeo.resources.mrspyramid/ColorScaleResourceTest/dir1/Default.xml");
      retvals.add("/mrgeo/test-files/output/org.mrgeo.resources.mrspyramid/ColorScaleResourceTest/dir1/rainbow.xml");

    Mockito.when(service.getColorScales()).thenReturn(retvals);
    WebResource webResource = resource();
    try
    {
      List<String> fileNames =
        webResource.path("colorscales")
        .queryParam("basePath", "/mrgeo/colorscales")
        .accept(MediaType.APPLICATION_JSON)
        .get(ColorScaleList.class)
        .getFilePaths();
      assertEquals(fileNames.size(), 5);
      assertTrue(fileNames.contains(
        "/mrgeo/test-files/output/org.mrgeo.resources.mrspyramid/ColorScaleResourceTest/rainbow-transparent.xml"));
      assertTrue(fileNames.contains(
        "/mrgeo/test-files/output/org.mrgeo.resources.mrspyramid/ColorScaleResourceTest/rainbow.xml"));
      assertTrue(fileNames.contains(
        "/mrgeo/test-files/output/org.mrgeo.resources.mrspyramid/ColorScaleResourceTest/dir1/brewer-green-log.xml"));
      assertTrue(fileNames.contains(
        "/mrgeo/test-files/output/org.mrgeo.resources.mrspyramid/ColorScaleResourceTest/dir1/Default.xml"));
      assertTrue(fileNames.contains(
        "/mrgeo/test-files/output/org.mrgeo.resources.mrspyramid/ColorScaleResourceTest/dir1/rainbow.xml"));
    }
    catch (UniformInterfaceException e)
    {
      ClientResponse r = e.getResponse();
      fail("Unexpected response " + r.getStatus() + " " + r.getEntity(String.class));
    }
  }

  private ColorScale createRainbowColorScale() throws ColorScaleException {
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

  @Test
  @Category(UnitTest.class)
  public void testGetColorScaleLegendVertical() throws Exception {
      Mockito.when(service.getColorScaleFromName(Mockito.anyString())).thenReturn( createRainbowColorScale() );
      WebResource webResource = resource();
      String path = "colorscales/legend/mrgeo/test-files/output/org.mrgeo.resources.mrspyramid/ColorScaleResourceTest/rainbow.xml";
      WebResource wr = webResource.path(path)
              .queryParam("max","1000");

      ClientResponse resp = wr.get(ClientResponse.class);
      MultivaluedMap<String, String> headers = resp.getHeaders();
      Assert.assertEquals("[text/html]", headers.get("Content-Type").toString());

      InputStream inputStream = resp.getEntityInputStream();

      DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
      DocumentBuilder db = dbf.newDocumentBuilder();
      Document doc = db.parse(inputStream);

      Assert.assertEquals(1, doc.getElementsByTagName("img").getLength());
      Assert.assertTrue(doc.getElementsByTagName("div").getLength() > 1);
      Assert.assertEquals("mrgeo/test-files/output/org.mrgeo.resources.mrspyramid/ColorScaleResourceTest/rainbow.xml?width=20&height=200",
              doc.getElementsByTagName("img").item(0).getAttributes().getNamedItem("src").getTextContent());
  }

  @Test
  @Category(UnitTest.class)
  public void testGetColorScaleLegendVerticalLikelihood() throws Exception {
      Mockito.when(service.getColorScaleFromName(Mockito.anyString())).thenReturn( createRainbowColorScale() );
      WebResource webResource = resource();
      WebResource wr = webResource.path("colorscales/legend//mrgeo/test-files/output/org.mrgeo.resources.mrspyramid/ColorScaleResourceTest/rainbow.xml")
              .queryParam("width","15")
              .queryParam("height","180")
              .queryParam("max","0.08")
              .queryParam("units","likelihood");

      ClientResponse resp = wr.get(ClientResponse.class);
      MultivaluedMap<String, String> headers = resp.getHeaders();
      Assert.assertEquals("[text/html]", headers.get("Content-Type").toString());

      InputStream inputStream = resp.getEntityInputStream();

      DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
      DocumentBuilder db = dbf.newDocumentBuilder();
      Document doc = db.parse(inputStream);

      Assert.assertEquals(doc.getElementsByTagName("img").getLength(), 1);
      Assert.assertTrue(doc.getElementsByTagName("div").getLength() > 1);
      Assert.assertEquals("/mrgeo/test-files/output/org.mrgeo.resources.mrspyramid/ColorScaleResourceTest/rainbow.xml?width=15&height=180",
              doc.getElementsByTagName("img").item(0).getAttributes().getNamedItem("src").getTextContent());
  }

  @Test
  @Category(UnitTest.class)
  public void testGetColorScaleLegendHorizontal() throws Exception {
      Mockito.when(service.getColorScaleFromName(Mockito.anyString())).thenReturn( createRainbowColorScale() );
      WebResource webResource = resource();
      WebResource wr = webResource.path("colorscales/legend/mrgeo/test-files/output/org.mrgeo.resources.mrspyramid/ColorScaleResourceTest/rainbow.xml")
              .queryParam("min","0")
              .queryParam("max","1000")
              .queryParam("units","meters")
              .queryParam("width","250")
              .queryParam("height","10");

      ClientResponse resp = wr.get(ClientResponse.class);
      MultivaluedMap<String, String> headers = resp.getHeaders();
      Assert.assertEquals("[text/html]", headers.get("Content-Type").toString());

      InputStream inputStream = resp.getEntityInputStream();

      DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
      DocumentBuilder db = dbf.newDocumentBuilder();
      Document doc = db.parse(inputStream);

      Assert.assertEquals(doc.getElementsByTagName("img").getLength(), 1);
      Assert.assertTrue(doc.getElementsByTagName("div").getLength() > 1);
  }

  @Test
  @Category(UnitTest.class)
  public void testGetColorScaleLegendHorizontalLikelihood() throws Exception {
      Mockito.when(service.getColorScaleFromName(Mockito.anyString())).thenReturn( createRainbowColorScale() );
      WebResource webResource = resource();
      WebResource wr = webResource.path("colorscales/legend/mrgeo/test-files/output/org.mrgeo.resources.mrspyramid/ColorScaleResourceTest/rainbow.xml")
              .queryParam("min","0")
              .queryParam("max","1.0")
              .queryParam("units","likelihood")
              .queryParam("width","200")
              .queryParam("height","10");

      ClientResponse resp = wr.get(ClientResponse.class);
      MultivaluedMap<String, String> headers = resp.getHeaders();
      Assert.assertEquals("[text/html]", headers.get("Content-Type").toString());

      InputStream inputStream = resp.getEntityInputStream();

      DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
      DocumentBuilder db = dbf.newDocumentBuilder();
      Document doc = db.parse(inputStream);

      Assert.assertEquals(doc.getElementsByTagName("img").getLength(), 1);
      Assert.assertTrue(doc.getElementsByTagName("div").getLength() > 1);
  }

  private MrGeoRaster getRaster(String dir, String file) throws IOException {
    return MrGeoRaster.fromDataset(GDALUtils.open(new File(dir, file).getCanonicalPath()));
  }

  private String input = TestUtils.composeInputDir(ColorScaleResourceTest.class);

  @Test
  @Category(UnitTest.class)
  public void testGetColorScaleSwatch() throws Exception
  {
    Mockito.when(service.createColorScaleSwatch(Mockito.anyString(), Mockito.anyString(),
        Mockito.anyInt(), Mockito.anyInt())).thenReturn(getRaster(input, "colorswatch.png") );

    ImageResponseWriter writer = new PngImageResponseWriter();
    Mockito.when(service.getImageResponseWriter(Mockito.anyString())).thenReturn(writer);

    WebResource webResource = resource();
    WebResource wr = webResource.path("colorscales/mrgeo/test-files/output/org.mrgeo.resources.mrspyramid/ColorScaleResourceTest/rainbow.xml")
            .queryParam("width", "100")
            .queryParam("height", "10");

    ClientResponse resp = wr.get(ClientResponse.class);
    MultivaluedMap<String, String> headers = resp.getHeaders();
    Assert.assertEquals("[image/png]", headers.get("Content-Type").toString());
    try (InputStream is = resp.getEntityInputStream())
    {

      MrGeoRaster raster = MrGeoRaster.fromDataset(GDALUtils.open(is));
      MrGeoRaster golden = getRaster(input, "colorswatch.png");
      TestUtils.compareRasters(golden, raster);

      Assert.assertEquals(100, raster.width());
      Assert.assertEquals(10, raster.height());
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testGetColorScaleSwatchMissingColorScale() throws Exception
  {
      Mockito.when(service.createColorScaleSwatch(Mockito.anyString(), Mockito.anyString(), Mockito.anyInt(), Mockito.anyInt())).thenAnswer(new Answer() {
          public Object answer(InvocationOnMock invocation) {
            throw new NotFoundException("Base path \"" + invocation.getArguments()[0] + "\" does not exist.");
          }
      });

    ImageResponseWriter writer = new PngImageResponseWriter();
    Mockito.when(service.getImageResponseWriter(Mockito.anyString())).thenReturn(writer);

    String path = "/mrgeo/test-files/output/org.mrgeo.resources.mrspyramid/ColorScaleResourceTest/missing.xml";
      WebResource webResource = resource();
    WebResource wr = webResource.path("colorscales/" + path)
            .queryParam("width", "100")
            .queryParam("height", "10");

    ClientResponse resp = wr.get(ClientResponse.class);
    MultivaluedMap<String, String> headers = resp.getHeaders();
    Assert.assertEquals("[image/png]", headers.get("Content-Type").toString());
    Assert.assertEquals(400, resp.getStatus());
    Assert.assertEquals("Color scale file not found: " + path, resp.getEntity(String.class));
  }
}
