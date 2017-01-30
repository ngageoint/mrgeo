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

package org.mrgeo.services.utils;

import com.google.common.io.ByteStreams;
import org.junit.Assert;
import org.mrgeo.data.raster.MrGeoRaster;
import org.mrgeo.test.TestUtils;
import org.mrgeo.utils.GDALUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import java.io.*;

import static org.junit.Assert.assertTrue;


@SuppressWarnings("all") // Test code, not included in production
public class ImageTestUtils
{
private static final Logger log = LoggerFactory.getLogger(ImageTestUtils.class);

public static void outputImageMatchesBaseline(final Response response,
    final String baselineImage) throws IOException
{
  final File baseline = new File(baselineImage);

  assertTrue(baseline.exists());

  log.debug("Response content length: " + response.getLength());

  MrGeoRaster raster = MrGeoRaster.fromDataset(GDALUtils.open(response.readEntity(InputStream.class)));
  Assert.assertNotNull("Test Image: not loaded, could not read stream", raster);

  TestUtils.compareRasters(baseline, raster);
}

//  public static void outputImageMatchesBaseline(final ClientResponse response,
//    final String baselineImage) throws IOException
//  {
//    final File baseline = new File(baselineImage);
//
//    assertTrue(baseline.exists());
//
//    log.debug("Response content length: " + response.getLength());
//    response.
//    MrGeoRaster raster = MrGeoRaster.fromDataset(GDALUtils.open(response.getEntityInputStream()));
//    Assert.assertNotNull("Test Image: not loaded, could not read stream", raster);
//
//    TestUtils.compareRasters(baseline, raster);
//  }

//  public static void writeBaselineImage(final ClientResponse response, final String path)
//    throws IOException
//  {
//    writeBaselineImage(response.getEntityInputStream(), path);
//  }

public static void writeBaselineImage(final InputStream stream, final String path)
    throws IOException
{
  final OutputStream outputStream = new FileOutputStream(new File(path));
  ByteStreams.copy(stream, outputStream);
  outputStream.close();

}

public static void writeBaselineImage(final Response response, final String path)
    throws IOException
{
  writeBaselineImage(response.readEntity(InputStream.class), path);
}
}
