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

import com.meterware.servletunit.ServletUnitClient;
import junit.framework.Assert;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.custommonkey.xmlunit.XMLAssert;
import org.custommonkey.xmlunit.XMLUnit;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.mrgeo.colorscale.ColorScaleManager;
import org.mrgeo.core.Defs;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.services.utils.ImageTestUtils;
import org.mrgeo.test.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.*;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

//import org.mrgeo.utils.logging.LoggingUtils;

@SuppressWarnings("all") // Test code, not included in production
public class WmsGeneratorTestAbstract extends JerseyTest
{
// bounds is completely outside of the image bounds
final static String ISLANDS_ELEVATION_V2_OUT_OF_BOUNDS =
    "160.312500,-12.656250,161.718750,-11.250000";
// bounds is within the image bounds and results in a single source tile being accessed;
// zoom level = 8
final static String ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE =
    "160.312500,-11.250000,163.125000,-8.437500";
// bounds is within the image bounds and results in multiple source tiles being accessed;
// zoom level = 9
final static String ISLANDS_ELEVATION_V2_IN_BOUNDS_MULTIPLE_SOURCE_TILES =
    "160.312500,-11.250000,161.718750,-9.843750";
// bounds is higher than the zoom level of any available image in the pyramid;
// zoom level = 11
final static String ISLANDS_ELEVATION_V2_PAST_HIGHEST_RES_ZOOM_LEVEL =
    "161.71875,-10.546875,161.89453125,-10.37109375";
private static final Logger log = LoggerFactory.getLogger(WmsGeneratorTestAbstract.class);
// only set this to true to generate new baseline images after correcting tests; image comparison
// tests won't be run when is set to true
private final static boolean GEN_BASELINE_DATA_ONLY = false;
protected static String input;
protected static String baselineInput;
protected static Path inputHdfs;
//  protected static ServletRunner servletRunner;
protected static ServletUnitClient webClient;
static String imageStretchUnqualified;
static String imageStretch2Unqualified;
static String small3bandUnqualified;
private static String imageStretch = "cost-distance";
private static String imageStretch2 = "cost-distance-shift-2";
private static String small3band = "small-3band";

@Rule
public TestName testname = new TestName();

@BeforeClass
public static void setUpForJUnit()
{
  try
  {
    DataProviderFactory.invalidateCache();
    ColorScaleManager.invalidateCache();

    // use the top level dir for input data
    input = TestUtils.composeInputDir(WmsGeneratorTestAbstract.class);
    inputHdfs = TestUtils.composeInputHdfs(WmsGeneratorTestAbstract.class, true);
    copyInputData();
//      launchServlet();
    MrGeoProperties.getInstance().setProperty(MrGeoConstants.MRGEO_HDFS_IMAGE, inputHdfs.toString());
    MrGeoProperties.getInstance().setProperty(MrGeoConstants.MRGEO_HDFS_COLORSCALE, inputHdfs.toString());

    if (GEN_BASELINE_DATA_ONLY)
    {
      log.warn("***WMS TESTS SET TO GENERATE BASELINE IMAGES ONLY***");
    }
  }
  catch (final Exception e)
  {
    e.printStackTrace();
  }
}

// TODO: all test classes are using the same set of input data for now to make things simpler...
// kind of inefficient
protected static void copyInputData() throws IOException
{
  final FileSystem fileSystem = HadoopFileUtils.getFileSystem(inputHdfs);

  Properties mrgeoProperties = MrGeoProperties.getInstance();

  mrgeoProperties.put(MrGeoConstants.MRGEO_COMMON_HOME, inputHdfs.toString());
  mrgeoProperties.put(MrGeoConstants.MRGEO_HDFS_IMAGE, inputHdfs.toString());
  mrgeoProperties.put(MrGeoConstants.MRGEO_HDFS_COLORSCALE, inputHdfs.toString());
  mrgeoProperties.put("base.path", inputHdfs.toString());

//    WmsGenerator.setBasePath(inputHdfs);
//    WmsGenerator.setColorScaleBasePath(inputHdfs);
  // regular data with pyramids built
  fileSystem.copyFromLocalFile(false, true, new Path(input, "IslandsElevation-v2"), inputHdfs);

  // set up the system color scale
  fileSystem.copyFromLocalFile(false, true, new Path(input, "rainbow.xml"),
      new Path(inputHdfs, "Default.xml"));

  // copy a custom color scale
  fileSystem.copyFromLocalFile(false, true, new Path(input, "IslandsElevation-v2"),
      new Path(inputHdfs, "IslandsElevation-v2-color-scale"));
  fileSystem.copyFromLocalFile(false, true, new Path(input, "brewer-green-log.xml"),
      new Path(inputHdfs, "IslandsElevation-v2-color-scale/ColorScale.xml"));

  // same data as above with only the highest res image; metadata file accurately represents
  // directory contents
  fileSystem.copyFromLocalFile(false, true, new Path(input, "IslandsElevation-v2-no-pyramid"),
      inputHdfs);

  // no pyramid data with a metadata file showing the pyramid contains all zoom levels, but it
  // actually only contains the highest res image
  fileSystem.copyFromLocalFile(false, true, new Path(input, "IslandsElevation-v2-no-pyramid"),
      new Path(inputHdfs, "IslandsElevation-v2-no-pyramid-extra-metadata"));
  fileSystem.copyFromLocalFile(false, true, new Path(input, "metadata-no-pyramid-extra"),
      new Path(inputHdfs, "IslandsElevation-v2-no-pyramid-extra-metadata/metadata"));

  // no statistics have been calculated for this pyramid
  fileSystem.copyFromLocalFile(false, true, new Path(input, "IslandsElevation-v2"),
      new Path(inputHdfs, "IslandsElevation-v2-no-stats"));
  fileSystem.copyFromLocalFile(false, true, new Path(input, "metadata-no-stats"),
      new Path(inputHdfs, "IslandsElevation-v2-no-stats/metadata"));

  HadoopFileUtils.copyToHdfs(new Path(Defs.INPUT), inputHdfs, imageStretch);
  imageStretchUnqualified =
      HadoopFileUtils.unqualifyPath(new Path(inputHdfs, imageStretch)).toString();

  HadoopFileUtils.copyToHdfs(new Path(Defs.INPUT), inputHdfs, imageStretch2);
  imageStretch2Unqualified =
      HadoopFileUtils.unqualifyPath(new Path(inputHdfs, imageStretch2)).toString();

  HadoopFileUtils.copyToHdfs(new Path(Defs.INPUT), inputHdfs, small3band);
  small3bandUnqualified =
      HadoopFileUtils.unqualifyPath(new Path(inputHdfs, small3band)).toString();
}

protected static void processXMLResponse(final Response response,
    final String baselineFileName) throws IOException, SAXException
{
  processXMLResponse(response, baselineFileName, Response.Status.OK);
}

//  @AfterClass
//  public static void tearDownForJUnit()
//  {
//    if (servletRunner != null)
//    {
//      servletRunner.shutDown();
//    }
//  }

//  @Override
//  protected AppDescriptor configure()
//  {
//    return new WebAppDescriptor.Builder("org.mrgeo.resources")
//            .contextPath("/")
//            .initParam("javax.ws.rs.Application", "org.mrgeo.application.Application")
//            .build();
//  }

protected static void processXMLResponse(final Response response,
    final String baselineFileName, Response.Status status)
    throws IOException, SAXException
{
  try
  {
    String content = response.readEntity(String.class);
    if (response.getStatus() != status.getStatusCode())
    {
      assertEquals("Unexpected response status " + response.getStatus() + " with content " + content,
          status.getStatusCode(), response.getStatus());
    }
    if (GEN_BASELINE_DATA_ONLY)
    {
      final String outputPath = baselineInput + baselineFileName;
      log.info("Generating baseline text: " + outputPath);
      final OutputStream outputStream = new FileOutputStream(new File(outputPath));
      try
      {
        // Turn xml string into a document
        org.w3c.dom.Document document = DocumentBuilderFactory.newInstance()
            .newDocumentBuilder()
            .parse(new InputSource(new ByteArrayInputStream(content.getBytes("utf-8"))));

        // Remove whitespaces outside tags
        XPath xPath = XPathFactory.newInstance().newXPath();
        NodeList nodeList = (NodeList) xPath.evaluate("//text()[normalize-space()='']",
            document,
            XPathConstants.NODESET);

        for (int i = 0; i < nodeList.getLength(); ++i)
        {
          Node node = nodeList.item(i);
          node.getParentNode().removeChild(node);
        }

        // Setup pretty print options
        TransformerFactory transformerFactory = TransformerFactory.newInstance();
        transformerFactory.setAttribute("indent-number", 2);
        Transformer transformer = transformerFactory.newTransformer();
        transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
        transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");

        // Return pretty print xml string
        StringWriter stringWriter = new StringWriter();
        transformer.transform(new DOMSource(document), new StreamResult(stringWriter));


        IOUtils.write(stringWriter.toString(), outputStream);
      }
      catch (TransformerConfigurationException e)
      {
        e.printStackTrace();
      }
      catch (TransformerException e)
      {
        e.printStackTrace();
      }
      catch (XPathExpressionException e)
      {
        e.printStackTrace();
      }
      catch (ParserConfigurationException e)
      {
        e.printStackTrace();
      }
      finally
      {
        IOUtils.closeQuietly(outputStream);
      }
    }
    else
    {
      final String baselineFile = baselineInput + baselineFileName;
      final InputStream inputStream = new FileInputStream(new File(baselineFile));
      try
      {
        XMLUnit.setIgnoreWhitespace(true);
        // log.debug(response.getText());
        log.info("Comparing result to baseline text in " + baselineFile + " ...");
        //Assert.assertEquals(IOUtils.toString(inputStream), content);

        XMLAssert.assertXMLEqual(IOUtils.toString(inputStream), content);
      }
      finally
      {
        IOUtils.closeQuietly(inputStream);
      }
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

//protected static WebRequest createRequest() throws MalformedURLException
//{
//  return new GetMethodWebRequest(
//      new URL(
//          MrGeoProperties.getInstance().getProperty("base.url", "http://localhost:8080")
//              .replaceAll("/mrgeo-services/", "")),
//      "/mrgeo-services/wms");
//}

@Before
public void init()
{
  DataProviderFactory.invalidateCache();
  ColorScaleManager.invalidateCache();
}

@Override
protected Application configure()
{
  ResourceConfig config = new ResourceConfig();
  config.register(WmsGenerator.class);
  return config;
}

protected void processImageResponse(final Response response, final String contentType, final String extension)
    throws IOException
{
  processImageResponse(response, contentType, extension, false);

}
  protected void processImageResponse(final Response response, final String contentType,
      final String extension, boolean hasgdal2)
    throws IOException
  {
  try
  {
    Assert.assertEquals("Bad response code", Response.Status.OK.getStatusCode(), response.getStatus());
    Assert.assertEquals(contentType, response.getHeaders().getFirst("Content-Type"));

    if (GEN_BASELINE_DATA_ONLY)
    {
      final String outputPath =
          baselineInput + testname.getMethodName() + "." +
              extension;
      log.info("Generating baseline image: " + outputPath);
      ImageTestUtils.writeBaselineImage(response, outputPath, hasgdal2);
    }
    else
    {
      final String baselineImageFile =
          baselineInput + testname.getMethodName() + "." +
              extension;
      log.info("Comparing result to baseline image " + baselineImageFile + " ...");
      ImageTestUtils.outputImageMatchesBaseline(response, baselineImageFile, hasgdal2);
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
