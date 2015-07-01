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

package org.mrgeo.services.utils;

import com.google.common.io.ByteStreams;
import com.meterware.httpunit.WebResponse;
import com.sun.jersey.api.client.ClientResponse;
import org.junit.Assert;
import org.mrgeo.test.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;

import static org.junit.Assert.assertTrue;

//import javax.imageio.ImageIO;

public class ImageTestUtils
{
  private static final Logger log = LoggerFactory.getLogger(ImageTestUtils.class);

  public static void outputImageMatchesBaseline(final WebResponse response,
    final String baselineImage) throws IOException
  {
    final File baseline = new File(baselineImage);

    assertTrue(baseline.exists());

    log.debug("Response content length: " + response.getContentLength());

    final BufferedImage image = ImageIO.read(response.getInputStream());
    Assert.assertNotNull("Test Image: not loaded, could not read stream", image);

    TestUtils.compareRasters(baseline, image.getData());
  }
  
  public static void outputImageMatchesBaseline(final ClientResponse response,
    final String baselineImage) throws IOException
  {
    final File baseline = new File(baselineImage);

    assertTrue(baseline.exists());

    log.debug("Response content length: " + response.getLength());

    final BufferedImage image = ImageIO.read(response.getEntityInputStream());
    Assert.assertNotNull("Test Image: " + image + " not loaded", image);

    TestUtils.compareRasters(baseline, image.getData());
  }

  public static void writeBaselineImage(final ClientResponse response, final String path)
    throws IOException
  {
    writeBaselineImage(response.getEntityInputStream(), path);
  }

  public static void writeBaselineImage(final InputStream stream, final String path)
    throws IOException
  {
    final OutputStream outputStream = new FileOutputStream(new File(path));
    ByteStreams.copy(stream, outputStream);
    outputStream.close();

  }

  public static void writeBaselineImage(final WebResponse response, final String path)
    throws IOException
  {
    writeBaselineImage(response.getInputStream(), path);
  }
}
