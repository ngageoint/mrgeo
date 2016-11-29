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

package org.mrgeo.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gdal.gdal.Dataset;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.data.raster.MrGeoRaster;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.image.MrsImage;
import org.mrgeo.image.MrsPyramid;
import org.mrgeo.mapalgebra.MapAlgebra;
import org.mrgeo.mapalgebra.parser.ParserException;
import org.mrgeo.job.JobCancelledException;
import org.mrgeo.job.JobFailedException;
import org.mrgeo.image.MrsPyramidMetadata;
import org.mrgeo.utils.GDALJavaUtils;
import org.mrgeo.utils.GDALUtils;
import org.mrgeo.utils.tms.Bounds;
import org.mrgeo.utils.tms.TMSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

@SuppressWarnings("all") // test code, not included in production
public class MapOpTestUtils extends TestUtils
{
private static final Logger log = LoggerFactory.getLogger(MapOpTestUtils.class);

public MapOpTestUtils(final Class<?> testClass) throws IOException
{
  super(testClass);
}

public MrsPyramidMetadata getImageMetadata(final String testName)
    throws IOException
{
  MrsImageDataProvider dp =
      DataProviderFactory.getMrsImageDataProvider(new Path(outputHdfs, testName).toString(),
          DataProviderFactory.AccessMode.READ, (ProviderProperties)null);

  return dp.getMetadataReader().read();
}

public void generateBaselinePyramid(final Configuration conf, final String testName,
    final String ex) throws IOException, JobFailedException, JobCancelledException,
    ParserException
{

  runMapAlgebraExpression(conf, testName, ex);

  final Path src = new Path(outputHdfs, testName);
  final MrsPyramid pyramid = MrsPyramid.open(src.toString(), (ProviderProperties)null);
  if (pyramid != null)
  {
    final Path dst = new Path(inputLocal, testName);
    final FileSystem fs = dst.getFileSystem(conf);
    fs.copyToLocalFile(src, dst);
  }
}

public void generateBaselineTif(final Configuration conf, final String testName, final String ex)
    throws IOException, JobFailedException, JobCancelledException, ParserException
{
  generateBaselineTif(conf, testName, ex, Double.NaN);
}

public void generateBaselineTif(final Configuration conf, final String testName,
    final String ex, double nodata)
    throws IOException, JobFailedException, JobCancelledException, ParserException
{
  runMapAlgebraExpression(conf, testName, ex);

  saveBaselineTif(testName, nodata);
}

public void saveBaselineTif(String testName, double nodata) throws IOException
{
  final MrsPyramid pyramid = MrsPyramid.open(new Path(outputHdfs, testName).toString(),
                                             (ProviderProperties)null);
  MrsPyramidMetadata meta = pyramid.getMetadata();

  try (MrsImage image = pyramid.getImage(meta.getMaxZoomLevel()))
  {
    MrGeoRaster raster = image.getRaster();
    final File baselineTif = new File(new File(inputLocal), testName + ".tif");

    Bounds tilesBounds = TMSUtils.tileBounds(meta.getBounds(), image.getMaxZoomlevel(), image.getTilesize());
    GDALJavaUtils.saveRaster(raster.toDataset(meta.getBounds(), meta.getDefaultValues()), baselineTif.getCanonicalPath(), tilesBounds, nodata);
  }
}

public void
runRasterExpression(final Configuration conf, final String testName, final String ex)
    throws ParserException, IOException, JobFailedException, JobCancelledException
{
  runRasterExpression(conf, testName, null, ex);
}

public void
runRasterExpression(final Configuration conf, final String testName,
    final TestUtils.ValueTranslator testTranslator, final String ex)
    throws ParserException, IOException, JobFailedException, JobCancelledException
{
  runMapAlgebraExpression(conf, testName, ex);
  compareRasterOutput(testName, testTranslator,  null);
}

public void
runRasterExpression(final Configuration conf, final String testName,
    final TestUtils.ValueTranslator baselineTranslator, final TestUtils.ValueTranslator testTranslator, final String ex)
    throws ParserException, IOException, JobFailedException, JobCancelledException
{
  runMapAlgebraExpression(conf, testName, ex);

  compareRasterOutput(testName, baselineTranslator, testTranslator, null);
}



public void compareRasterOutput(final String testName, final TestUtils.ValueTranslator testTranslator) throws IOException
{
  compareRasterOutput(testName, null, testTranslator, null);
}

private void compareRasterOutput(final String testName, final TestUtils.ValueTranslator testTranslator,
    final ProviderProperties providerProperties) throws IOException
{
  compareRasterOutput(testName, null, testTranslator, providerProperties);
}

private void compareRasterOutput(final String testName, final TestUtils.ValueTranslator baselineTranslator,
    final TestUtils.ValueTranslator testTranslator, final ProviderProperties providerProperties) throws IOException
{
  final MrsPyramid pyramid = MrsPyramid.open(new Path(outputHdfs, testName).toString(),
                                             providerProperties);
  final MrsImage image = pyramid.getHighestResImage();

  try
  {
    // The output against which to compare could either be a tif or a MrsPyramid.
    // We check for the tif first.
    final File file = new File(inputLocal);
    final File baselineTif = new File(file, testName + ".tif");
    if (baselineTif.exists())
    {
      TestUtils.compareRasters(baselineTif, baselineTranslator, image.getRaster(), testTranslator);
    }
    else
    {
      final String inputLocalAbs = file.getCanonicalFile().toURI().toString();
      final MrsPyramid goldenPyramid = MrsPyramid.open(inputLocalAbs + "/" + testName,
                                                       providerProperties);
      final MrsImage goldenImage = goldenPyramid.getImage(image.getZoomlevel());
      try
      {
        TestUtils.compareRasters(goldenImage.getRaster(), image.getRaster());
      }
      finally
      {
        if (goldenImage != null)
        {
          goldenImage.close();
        }
      }
    }
  }
  finally
  {
    if (image != null)
    {
      image.close();
    }
  }
}
public void compareLocalRasterOutput(final String testName, final TestUtils.ValueTranslator testTranslator) throws IOException
{
  compareLocalRasterOutput(testName, null, testTranslator, null);
}
public void compareLocalRasterOutput(final String testName, final TestUtils.ValueTranslator testTranslator,
    final ProviderProperties providerProperties) throws IOException
{
  compareLocalRasterOutput(testName, null, testTranslator, providerProperties);
}

private void compareLocalRasterOutput(final String testName, final TestUtils.ValueTranslator baselineTranslator,
    final TestUtils.ValueTranslator testTranslator, final ProviderProperties providerProperties) throws IOException
{
  final File tf = new File(getOutputLocal());
  final File testFile = new File(tf, testName + ".tif");

  final MrGeoRaster testRaster;
  if (testFile.exists())
  {
    Dataset d = GDALUtils.open(testFile.getCanonicalPath());
    testRaster = MrGeoRaster.fromDataset(d);
  }
  else
  {
    final String inputLocalAbs = tf.getCanonicalFile().toURI().toString();
    final MrsPyramid testPyramid = MrsPyramid.open(inputLocalAbs + "/" + testName,
                                                   providerProperties);
    final MrsImage testImage = testPyramid.getImage(testPyramid.getMaximumLevel());
    testRaster = testImage.getRaster();
  }

  // The output against which to compare could either be a tif or a MrsPyramid.
  // We check for the tif first.
  final File file = new File(inputLocal);
  final File baselineTif = new File(file, testName + ".tif");
  if (baselineTif.exists())
  {
    TestUtils.compareRasters(baselineTif, baselineTranslator, testRaster, testTranslator);
  }
  else
  {
    final String inputLocalAbs = file.getCanonicalFile().toURI().toString();
    final MrsPyramid goldenPyramid = MrsPyramid.open(inputLocalAbs + "/" + testName,
                                                     providerProperties);
    final MrsImage goldenImage = goldenPyramid.getImage(goldenPyramid.getMaximumLevel());
    try
    {
      TestUtils.compareRasters(goldenImage.getRaster(), testRaster);
    }
    finally
    {
      if (goldenImage != null)
      {
        goldenImage.close();
      }
    }
  }
}

/**
 * Runs the map algebra expression and stores the results to outputHdfs in  a
 * subdirectory that matches the testName. No comparison against expected
 * output is done. See other methods in this class like runVectorExpression and
 * runRasterExpression for that capability.
 *
 */
public void runMapAlgebraExpression(final Configuration conf, final String testName, final String ex)
    throws IOException, JobFailedException, JobCancelledException, ParserException
{
  HadoopFileUtils.delete(new Path(outputHdfs, testName));

  log.info(ex);

  long start = System.currentTimeMillis();

  ProviderProperties pp = ProviderProperties.fromDelimitedString("");
  MapAlgebra.validateWithExceptions(ex, pp);
  MapAlgebra.mapalgebra(ex, (new Path(outputHdfs, testName)).toString(), conf, pp, null);

  log.info("Test Execution time: " + (System.currentTimeMillis() - start));
}
}
