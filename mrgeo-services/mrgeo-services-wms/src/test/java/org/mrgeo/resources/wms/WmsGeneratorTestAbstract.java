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

package org.mrgeo.resources.wms;

import com.meterware.httpunit.GetMethodWebRequest;
import com.meterware.httpunit.WebRequest;
import com.meterware.httpunit.WebResponse;
import com.meterware.servletunit.ServletUnitClient;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.core.DefaultResourceConfig;
import com.sun.jersey.test.framework.AppDescriptor;
import com.sun.jersey.test.framework.JerseyTest;
import com.sun.jersey.test.framework.LowLevelAppDescriptor;
import com.sun.jersey.test.framework.spi.container.TestContainerException;
import com.sun.jersey.test.framework.spi.container.TestContainerFactory;
import com.sun.jersey.test.framework.spi.container.grizzly2.GrizzlyTestContainerFactory;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.BeforeClass;
import org.mrgeo.core.Defs;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.services.mrspyramid.ColorScaleManager;
import org.mrgeo.services.utils.ImageTestUtils;
import org.mrgeo.test.TestUtils;
import org.mrgeo.utils.HadoopUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

//import org.mrgeo.utils.LoggingUtils;

public class WmsGeneratorTestAbstract extends JerseyTest
{
  private static final Logger log = LoggerFactory.getLogger(WmsGeneratorTestAbstract.class);

  protected static String input;
  protected static String baselineInput;
  protected static Path inputHdfs;

//  protected static ServletRunner servletRunner;
  protected static ServletUnitClient webClient;

  // only set this to true to generate new baseline images after correcting tests; image comparison
  // tests won't be run when is set to true
  public final static boolean GEN_BASELINE_DATA_ONLY = false;
  // bounds is completely outside of the image bounds
  public final static String ISLANDS_ELEVATION_V2_OUT_OF_BOUNDS =
      "160.312500,-12.656250,161.718750,-11.250000";
  // bounds is within the image bounds and results in a single source tile being accessed;
  // zoom level = 8
  public final static String ISLANDS_ELEVATION_V2_IN_BOUNDS_SINGLE_SOURCE_TILE =
      "160.312500,-11.250000,163.125000,-8.437500";
  // bounds is within the image bounds and results in multiple source tiles being accessed;
  // zoom level = 9
  public final static String ISLANDS_ELEVATION_V2_IN_BOUNDS_MULTIPLE_SOURCE_TILES =
      "160.312500,-11.250000,161.718750,-9.843750";
  // bounds is higher than the zoom level of any available image in the pyramid;
  // zoom level = 11
  public final static String ISLANDS_ELEVATION_V2_PAST_HIGHEST_RES_ZOOM_LEVEL =
      "161.71875,-10.546875,161.89453125,-10.37109375";
  
  protected static String imageStretch = "cost-distance";
  protected static String imageStretchUnqualified;
  protected static String imageStretch2 = "cost-distance-shift-2";
  protected static String imageStretch2Unqualified;
  
  protected static String small3band = "small-3band";
  protected static String small3bandUnqualified;

  @Override
  protected AppDescriptor configure()
  {
    DefaultResourceConfig resourceConfig = new DefaultResourceConfig();
    resourceConfig.getClasses().add(WmsGenerator.class);
    return new LowLevelAppDescriptor.Builder( resourceConfig ).build();
  }

  @Override
  protected TestContainerFactory getTestContainerFactory() throws TestContainerException
  {
//    return new FilteringInMemoryTestContainerFactory();
    return new GrizzlyTestContainerFactory();
//    return new InMemoryTestContainerFactory();
  }

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
      MrGeoProperties.getInstance().setProperty(HadoopUtils.IMAGE_BASE, inputHdfs.toString());
      MrGeoProperties.getInstance().setProperty(HadoopUtils.COLOR_SCALE_BASE, inputHdfs.toString());

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

  @Before
  public void init()
  {
    DataProviderFactory.invalidateCache();
    ColorScaleManager.invalidateCache();
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

  // TODO: all test classes are using the same set of input data for now to make things simpler...
  // kind of inefficient
  protected static void copyInputData() throws IOException
  {
    final FileSystem fileSystem = HadoopFileUtils.getFileSystem(inputHdfs);
    
    Properties mrgeoProperties = MrGeoProperties.getInstance();

    mrgeoProperties.put("MRGEO_HOME", inputHdfs.toString());
    mrgeoProperties.put(HadoopUtils.IMAGE_BASE, inputHdfs.toString());
    mrgeoProperties.put(HadoopUtils.COLOR_SCALE_BASE, inputHdfs.toString());
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

  protected static WebRequest createRequest() throws MalformedURLException
  {
    return new GetMethodWebRequest(
        new URL(
            MrGeoProperties.getInstance().getProperty("base.url", "http://localhost:8080")
                .replaceAll("/mrgeo-services/", "")),
        "/mrgeo-services/wms");
  }

//  protected static void launchServlet()
//  {
//    servletRunner = new ServletRunner();
//    servletRunner.registerServlet(
//        "/mrgeo-services/WmsGenerator", WmsGenerator.class.getName());
//    webClient = servletRunner.newClient();
////    WmsGenerator.setBasePath(inputHdfs);
////    WmsGenerator.setColorScaleBasePath(inputHdfs);
//
//    MrGeoProperties.getInstance().setProperty(HadoopUtils.IMAGE_BASE, inputHdfs.toString());
//    MrGeoProperties.getInstance().setProperty(HadoopUtils.COLOR_SCALE_BASE, inputHdfs.toString());
//  }

  protected static void processImageResponse(final WebResponse response, final String extension)
      throws IOException
  {
    try
    {
      assertEquals(response.getResponseCode(), 200);
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

  protected static void processImageResponse(final ClientResponse response,
                                             final String contentType, final String extension)
          throws IOException
  {
    try
    {
      if (response.getStatus() != Response.Status.OK.getStatusCode())
      {
        // Because of how junit works, we check for the failure case before calling
        // assertEquals because it will trigger the code that computes the message (the
        // first argument), which reads the content, and then the content would not be
        // readable for the success case (to get the image bytes).
        String content = response.getEntity(String.class);
        assertEquals("Unexpected response status " + response.getStatus() + " with content " + content,
                     Response.Status.OK.getStatusCode(), response.getStatus());
      }
      assertEquals(contentType, response.getHeaders().getFirst("Content-Type"));
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

  protected static void processTextResponse(final WebResponse response,
      final String baselineFileName)
      throws IOException
  {
    try
    {
      assertEquals(response.getResponseCode(), 200);
      if (GEN_BASELINE_DATA_ONLY)
      {
        final String outputPath = baselineInput + baselineFileName;
        log.info("Generating baseline text: " + outputPath);
        final OutputStream outputStream = new FileOutputStream(new File(outputPath));
        try
        {
          // log.debug(response.getText());
          IOUtils.write(response.getText(), outputStream);
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
          // log.debug(response.getText());
          log.info("Comparing result to baseline text in " + baselineFile + " ...");
          assertEquals(IOUtils.toString(inputStream), response.getText());
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

  protected static void processTextResponse(final ClientResponse response,
                                            final String baselineFileName)
          throws IOException
  {
    try
    {
      String content = response.getEntity(String.class);
      if (response.getStatus() != Response.Status.OK.getStatusCode())
      {
        assertEquals("Unexpected response status " + response.getStatus() + " with content " + content,
                     Response.Status.OK.getStatusCode(), response.getStatus());
      }
      if (GEN_BASELINE_DATA_ONLY)
      {
        final String outputPath = baselineInput + baselineFileName;
        log.info("Generating baseline text: " + outputPath);
        final OutputStream outputStream = new FileOutputStream(new File(outputPath));
        try
        {
          // log.debug(response.getText());
          IOUtils.write(content, outputStream);
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
          // log.debug(response.getText());
          log.info("Comparing result to baseline text in " + baselineFile + " ...");
          assertEquals(IOUtils.toString(inputStream), content);
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
}
