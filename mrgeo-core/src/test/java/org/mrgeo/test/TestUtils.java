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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gdal.gdal.Dataset;
import org.junit.Assert;
import org.mrgeo.core.Defs;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.data.raster.MrGeoRaster;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.image.MrsPyramidMetadata;
import org.mrgeo.job.JobCancelledException;
import org.mrgeo.job.JobFailedException;
import org.mrgeo.mapalgebra.parser.ParserException;
import org.mrgeo.utils.FileUtils;
import org.mrgeo.utils.GDALJavaUtils;
import org.mrgeo.utils.GDALUtils;
import org.mrgeo.utils.tms.Bounds;

import java.awt.image.DataBuffer;
import java.io.*;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

public class TestUtils
{
private static final double NON_NAN_NODATA_VALUE = -32767.0;
public static TestUtils.ValueTranslator nanTranslatorToMinus9999 = new NaNTranslator(-9999.0f);
public static TestUtils.ValueTranslator anTranslatorToMinus32767 = new NaNTranslator((float) NON_NAN_NODATA_VALUE);

final String inputLocal;
final Path outputHdfs;
final private String outputLocal;
final private Path inputHdfs;
final private Path testLocal;

public TestUtils(final Class<?> testClass) throws IOException
{
  inputLocal = TestUtils.composeInputDir(testClass);
  inputHdfs = TestUtils.composeInputHdfs(testClass);
  outputLocal = TestUtils.composeOutputDir(testClass);
  outputHdfs = TestUtils.composeOutputHdfs(testClass);

  final File f = new File(Defs.INPUT);

  testLocal = new Path("file://" + f.getCanonicalPath());

  setJarLocation();

}

private static String readPath(Path p) throws IOException
{
  FileSystem fs = HadoopFileUtils.getFileSystem(p);
  try (FSDataInputStream fdis = fs.open(p))
  {
    FileStatus stat = fs.getFileStatus(p);

    byte[] baselineBuffer = new byte[(int) stat.getLen()];
    fdis.readFully(baselineBuffer);
    return new String(baselineBuffer);
  }
}

private static String readFile(File f) throws IOException
{
  byte[] baselineBuffer = new byte[(int) f.length()];
  int read = 0;
  try (FileInputStream s = new FileInputStream(f))
  {
    while (read < f.length())
    {
      read = s.read(baselineBuffer);
    }
  }
  return new String(baselineBuffer);
}

public static MrsPyramidMetadata readImageMetadata(String filename) throws
    IOException
{
  try (FileInputStream stream = new FileInputStream(filename))
  {
    return MrsPyramidMetadata.load(stream);
  }
}

public static String calculateHash(File f) throws IOException
{
  return calculateHash(readFile(f));
}

private static String calculateHash(String s)
{
  MessageDigest digest;
  try
  {
    digest = MessageDigest.getInstance("MD5");
  }
  catch (NoSuchAlgorithmException e)
  {
    e.printStackTrace();
    return "error";
  }

  digest.update(s.getBytes(), 0, s.getBytes().length);
  return new BigInteger(1, digest.digest()).toString(16);
}

public static String calculateHash(Path p) throws IOException
{
  return calculateHash(readPath(p));
}

public static String composeInputDir(Class<?> c)
{
  String pn = packageName(c);
  String cn = c.getName().replace(pn + ".", "");
  String result = Defs.INPUT + pn + "/" + cn + "/";

  File f = new File(result);
  try
  {
    return f.getCanonicalPath() + "/";
  }
  catch (IOException ignored)
  {
  }

  return result;
}

public static String composeOutputDir(Class<?> c) throws IOException
{

  String pn = packageName(c);
  String cn = c.getName().replace(pn + ".", "");
  String result = Defs.OUTPUT + pn + "/" + cn + "/";
  File dir = new File(result);
  if (dir.exists())
  {
    FileUtils.deleteDir(dir, true);
  }

  if (!dir.mkdirs())
  {
    throw new IOException("Error creating test output (filesystem) directory (" + result + ")");
  }

  return dir.getCanonicalPath();
}

public static Path composeOutputHdfs(Class<?> c) throws IOException
{
  String pn = packageName(c);
  String cn = c.getName().replace(pn + ".", "");
  Path result = new Path(Defs.OUTPUT_HDFS + "/" + pn + "/" + cn + "/");
  FileSystem fs = HadoopFileUtils.getFileSystem(result);

  if (fs.exists(result))
  {
    fs.delete(result, true);
  }

  if (!fs.mkdirs(result))
  {
    throw new IOException("Error creating test output HDFS directory (" + result + ")");
  }
  return result;
}

public static Path composeInputHdfs(Class<?> c, boolean overwrite) throws IOException
{
  String pn = packageName(c);
  String cn = c.getName().replace(pn + ".", "");
  Path result = new Path(Defs.INPUT_HDFS + "/" + pn + "/" + cn + "/");
  FileSystem fs = HadoopFileUtils.getFileSystem(result);

  if (fs.exists(result) && overwrite)
  {
    HadoopFileUtils.delete(result);
  }

  if (!fs.exists(result))
  {
    if (!fs.mkdirs(result))
    {
      throw new IOException("Error creating test input HDFS directory (" + result + ")");
    }
  }
  return result;
}

private static Path composeInputHdfs(Class<?> c) throws IOException
{
  return composeInputHdfs(c, false);
}

private static String packageName(Class<?> c)
{
  String name = c.getName();
  int li = name.lastIndexOf(".");
  return name.substring(0, li);
}

private static void compareRasters(File r1, MrGeoRaster r2) throws IOException
{
  Dataset d = GDALUtils.open(r1.getCanonicalPath());
  MrGeoRaster r = MrGeoRaster.fromDataset(d);
  compareRasters(r, null, r2, null);
}

static void compareRasters(File r1, ValueTranslator t1, MrGeoRaster r2, ValueTranslator t2) throws IOException
{
  Dataset d = GDALUtils.open(r1.getCanonicalPath());
  MrGeoRaster r = MrGeoRaster.fromDataset(d);
  compareRasters(r, t1, r2, t2);
}

public static void compareRasters(MrGeoRaster r1, MrGeoRaster r2)
{
  compareRasters(r1, null, r2, null);
}

/**
 * If no translation of values should occur for one or both rendered images,
 * pass null for that translator parameter.
 */
public static void compareRasters(MrGeoRaster r1, ValueTranslator r1Translator,
    MrGeoRaster r2, ValueTranslator r2Translator)
{
  int dataType = r1.datatype();
  boolean intish = dataType == DataBuffer.TYPE_BYTE || dataType == DataBuffer.TYPE_INT
      || dataType == DataBuffer.TYPE_SHORT;
  Assert.assertEquals("Image widths are different", r1.width(), r2.width());
  Assert.assertEquals("Image heights are different", r1.height(), r2.height());
  Assert.assertEquals("Mismatch in number of bands", r1.bands(), r2.bands());
  for (int rx = 0; rx < r1.width(); rx++)
  {
    for (int ry = 0; ry < r1.height(); ry++)
    {
      for (int band = 0; band < r1.bands(); band++)
      {
        float v1 = r1.getPixelFloat(rx, ry, band);
        if (r1Translator != null)
        {
          v1 = r1Translator.translate(v1);
        }
        float v2 = r2.getPixelFloat(rx, ry, band);
        if (r2Translator != null)
        {
          v2 = r2Translator.translate(v2);
        }

        if (Float.isNaN(v1) != Float.isNaN(v2))
        {
          Assert.assertEquals("Pixel NaN mismatch: px: " + rx + " py: " + ry
              + " b: " + band + " v1: " + v1 + " v2: " + v2, v1, v2, 0);
        }

        // make delta something reasonable relative to the data

        //NOTE: this formula is not very reliable.  An error of 2e-3f for
        //  pixel v1=1 fails, but passes for v1=2.
        float delta = intish ? 1.0001f : Math.max(Math.abs(v1 * 1e-3f), 1e-3f);
        Assert.assertEquals("Pixel value mismatch: px: " + rx + " py: " + ry
            + " b: " + band + " v1: " + v1 + " v2: " + v2, v1, v2, delta);
      }
    }
  }
}

public static void compareRasterToConstant(MrGeoRaster raster, double constant, double nodata)
{
  int dataType = raster.datatype();
  boolean intish = dataType == DataBuffer.TYPE_BYTE || dataType == DataBuffer.TYPE_INT
      || dataType == DataBuffer.TYPE_SHORT;

  boolean nodataIsNan = Double.isNaN(nodata);

  double delta = intish ? 1.0001 : Math.max(Math.abs(constant * 1e-3), 1e-3);

  for (int rx = 0; rx < raster.width(); rx++)
  {
    for (int ry = 0; ry < raster.height(); ry++)
    {
      for (int band = 0; band < raster.bands(); band++)
      {
        double v = raster.getPixelDouble(rx, ry, band);

        if ((nodataIsNan && (!Double.isNaN(v))) || (!nodataIsNan && (v != nodata)))
        {
          Assert.assertEquals("Pixel value mismatch: px: " + rx + " py: " + ry
              + " v: " + v + " const: " + constant, constant, v, delta);
        }
      }
    }
  }
}

public static MrGeoRaster createConstRaster(int width, int height, double c)
{
  MrGeoRaster raster = MrGeoRaster.createEmptyRaster(width, height, DataBuffer.TYPE_DOUBLE, 1);
  raster.fill(c);

  return raster;
}

public static MrGeoRaster createConstRasterWithNodata(int width, int height, double c, double nodata,
    int nodataFrequency)
{
  MrGeoRaster raster = MrGeoRaster.createEmptyRaster(width, height, DataBuffer.TYPE_DOUBLE, 1);

  for (int x = 0; x < width; x++)
  {
    for (int y = 0; y < height; y++)
    {
      int pixelId = getPixelId(x, y, width);
      raster.setPixel(x, y, 0, ((pixelId % nodataFrequency) == 0) ? nodata : c);
    }
  }

  return raster;
}

public static MrGeoRaster createNumberedRaster(int width, int height)
{
  MrGeoRaster raster = MrGeoRaster.createEmptyRaster(width, height, DataBuffer.TYPE_DOUBLE, 1);

  for (int x = 0; x < width; x++)
  {
    for (int y = 0; y < height; y++)
    {
      int pixelId = getPixelId(x, y, width);
      raster.setPixel(x, y, 0, (double) pixelId);
    }
  }

  return raster;
}

public static MrGeoRaster createNumberedRasterWithNodata(int width, int height, double nodata, int nodataFrequency)
{
  MrGeoRaster raster = MrGeoRaster.createEmptyRaster(width, height, DataBuffer.TYPE_DOUBLE, 1);

  for (int x = 0; x < width; x++)
  {
    for (int y = 0; y < height; y++)
    {
      int pixelId = getPixelId(x, y, width);

      raster.setPixel(x, y, 0,
          ((pixelId % nodataFrequency) == 0) ? nodata : (double) pixelId);
    }
  }

  return raster;
}

public static void compareTextFiles(File f1, File f2) throws IOException
{
  compareTextFiles(f1.getCanonicalPath(), f2.getCanonicalPath(), true);
}

public static void compareTextFiles(File f1, File f2, boolean ignorewhitespace) throws IOException
{
  compareTextFiles(f1.getCanonicalPath(), f2.getCanonicalPath(), ignorewhitespace);
}

public static void compareTextFiles(String f1, String f2) throws IOException
{
  compareTextFiles(f1, f2, true);
}

public static void compareTextFiles(String f1, String f2, boolean ignorewhitespace) throws IOException
{
  Assert.assertNotNull("File cannot be null!", f1);
  Assert.assertNotNull("File cannot be null!", f2);

  File ff1 = new File(f1);
  File ff2 = new File(f2);

  FileReader fr1 = null;
  FileReader fr2 = null;

  InputStream is1 = null;
  InputStream is2 = null;

  BufferedReader br1 = null;
  BufferedReader br2 = null;

  InputStreamReader isr = null;

  try
  {
    if (ff1.exists())
    {
      fr1 = new FileReader(ff1);

      br1 = new BufferedReader(fr1);
    }
    else if (HadoopFileUtils.exists(f1))
    {
      is1 = HadoopFileUtils.open(new Path(f1));
      isr = new InputStreamReader(is1);

      br1 = new BufferedReader(isr);
    }
    else
    {
      Assert.fail("File not found: " + f1);
    }

    if (ff2.exists())
    {
      fr2 = new FileReader(ff2);
      br2 = new BufferedReader(fr2);
    }
    else if (HadoopFileUtils.exists(f2))
    {
      is2 = HadoopFileUtils.open(new Path(f2));
      isr = new InputStreamReader(is2);

      br2 = new BufferedReader(isr);
    }
    else
    {
      Assert.fail("File not found: " + f2);
    }


    String line1;
    String line2;
    while ((line1 = br1.readLine()) != null)
    {
      line2 = br2.readLine();
      if (line2 == null)
      {
        Assert.fail("File has extra lines (" + f1 + ")");
      }

      if (ignorewhitespace)
      {
        line1 = line1.trim().replaceAll(" +", " ");
        line2 = line2.trim().replaceAll(" +", " ");
      }

      Assert.assertEquals("Lines are not equal", line1, line2);
    }

    line2 = br2.readLine();
    while (line2 != null)
    {
      if (ignorewhitespace)
      {
        line2 = line2.trim();
      }


      if (line2.length() != 0)
      {
        Assert.fail("File has extra lines (" + f2 + ")");
      }
    }
  }
  finally
  {
    if (br1 != null)
    {
      br1.close();
    }
    if (br2 != null)
    {
      br2.close();
    }

    if (isr != null)
    {
      isr.close();
    }

    if (fr1 != null)
    {
      fr1.close();
    }
    if (fr2 != null)
    {
      fr2.close();
    }
    if (is1 != null)
    {
      is1.close();
    }
    if (is2 != null)
    {
      is2.close();
    }
  }

}

static void setJarLocation() throws IOException
{
  File working = new File(Defs.CWD);
  String name = working.getName();

  File jar = new File("target/" + name + "-full.jar");

  if (jar.exists())
  {
    MrGeoProperties.getInstance().setProperty(MrGeoConstants.MRGEO_JAR,
        jar.getCanonicalPath());
  }

}

private static int getPixelId(int x, int y, int width)
{
  return x + (y * width);
}

public Path getInputHdfs()
{
  return inputHdfs;
}

public Path getInputHdfsFor(final String fileName)
{
  return new Path(inputHdfs, fileName);
}

public String getInputLocal()
{
  return inputLocal;
}

public String getInputLocalFor(final String filename)
{
  return new File(inputLocal, filename).toString();
}

public Path getOutputHdfs()
{
  return outputHdfs;
}

public Path getOutputHdfsFor(final String fileName)
{
  return new Path(outputHdfs, fileName);
}

String getOutputLocal()
{
  return outputLocal;
}

public String getOutputLocalFor(final String filename)
{
  return new File(outputLocal, filename).toString();
}

public Path getTestLocal()
{
  return testLocal;
}

public void compareRasters(String testName, MrGeoRaster r2) throws IOException
{
  final File baselineTif = new File(new File(inputLocal), testName + ".tif");
  compareRasters(baselineTif, r2);
}

public void compareRasters(String testName, ValueTranslator t1, MrGeoRaster r2, ValueTranslator t2) throws IOException
{
  final File baselineTif = new File(new File(inputLocal), testName + ".tif");
  compareRasters(baselineTif, t1, r2, t2);
}

public void generateBaselineTif(final String testName,
    final MrGeoRaster raster, Bounds bounds, double nodata)
    throws IOException, JobFailedException, JobCancelledException, ParserException
{
  double[] nodatas = new double[raster.bands()];
  Arrays.fill(nodatas, nodata);

  final File baselineTif = new File(new File(inputLocal), testName + ".tif");
  GDALJavaUtils.saveRaster(raster.toDataset(bounds, nodatas), baselineTif.getCanonicalPath(), nodata);
}

public interface ValueTranslator
{
  float translate(float value);
}

public static class NaNTranslator implements TestUtils.ValueTranslator
{

  private float translateTo;

  public NaNTranslator(float translateTo)
  {
    this.translateTo = translateTo;
  }

  @Override
  public float translate(float value)
  {
    float result = value;
    if (Float.isNaN(value))
    {
      result = translateTo;
    }
    return result;
  }
}


}
