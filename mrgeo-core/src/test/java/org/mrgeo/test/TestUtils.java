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
package org.mrgeo.test;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.junit.Assert;
import org.mrgeo.core.Defs;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.data.raster.RasterUtils;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.image.MrsImage;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.mrgeo.image.RasterTileMerger;
import org.mrgeo.mapalgebra.parser.ParserException;
import org.mrgeo.mapreduce.job.JobCancelledException;
import org.mrgeo.mapreduce.job.JobFailedException;
import org.mrgeo.rasterops.GeoTiffExporter;
import org.mrgeo.rasterops.OpImageRegistrar;
import org.mrgeo.utils.Bounds;
import org.mrgeo.utils.FileUtils;
import org.mrgeo.utils.TMSUtils;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.*;
import java.io.*;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class TestUtils
{
  public interface ValueTranslator
  {
    public float translate(float value);
  }

  public static final class DiffStats
  {
    // count, not including nan
    public long count = 0;
    public double maxDelta = 0.0;
    public double maxPercentDelta = 0.0;
    public double meanDelta = 0.0;
    public double meanPercentDelta = 0.0;
    // The number of pixels that aren't reasonably close.
    public long diffCount = 0;
    public long nanMismatch = 0;
    public double sumDelta = 0.0;
    public double sumPercentDelta = 0.0;
    public double pixelPercentError = 0.0;
    public long width = 0;
    public long height = 0;
    public double[] maxValue = {-Double.MAX_VALUE,-Double.MAX_VALUE,-Double.MAX_VALUE,
        -Double.MAX_VALUE};
    public double[] minValue = {Double.MAX_VALUE, Double.MIN_VALUE, Double.MIN_VALUE,
        Double.MIN_VALUE};
    final long noDataValue = -9999;
    int numBands = 0;

    public void calculateExtremes(RenderedImage i1, RenderedImage i2, int tileSize)
    {
      int imgWidth = i1.getWidth();
      int imgHeight = i1.getHeight();
      int tileWidth = tileSize;
      int tileHeight = tileSize;
      int x, y, rx, ry, dx, dy;
      int band;
      double v1;
      numBands = (i1.getData(new Rectangle(0, 0, 1, 1))).getNumBands();
      assert(numBands < 5);

      for (x = 0; x < imgWidth; x += tileSize)
      {
        for (y = 0; y < imgHeight; y += tileSize)
        {
          if (x + tileWidth > imgWidth) { tileWidth = imgWidth - x; }
          if (y + tileHeight > imgHeight) { tileHeight = imgHeight - y; }
          Rectangle rect = new Rectangle(x, y, tileWidth, tileHeight);
          Raster r1 = i1.getData(rect);
          for (rx = 0; rx < tileWidth; rx++)
          {
            for (ry = 0; ry < tileHeight; ry++)
            {
              dx = rx + x;
              dy = ry + y;
              for (band = 0; band < numBands; band++)
              {
                v1 = r1.getSampleDouble(dx, dy, band);
                if((long)v1 != noDataValue){
                  minValue[band] = Math.min(minValue[band], v1);
                  maxValue[band] = Math.max(maxValue[band], v1);
                }
              }
            }
          }
        }
      }
    }

    public void addToStats(double v1, double v2, double tolerance, int band)
    {
      if (Double.isNaN(v1) != Double.isNaN(v2))
      {
        nanMismatch++;
      }
      else if (Double.isNaN(v1) == false)
      {
        count++;
        double delta = Math.abs(v1 - v2);
        double percentDelta = delta / (maxValue[band] - minValue[band]);
        if (percentDelta > tolerance) diffCount++;
        maxDelta = Math.max(delta, maxDelta);
        maxPercentDelta = Math.max(percentDelta, maxPercentDelta);
        sumDelta += delta;
        sumPercentDelta += percentDelta;
        meanDelta = sumDelta / count;
        meanPercentDelta = sumPercentDelta / count;
      }
    }

    public void calculateDiffStats(Raster r1, Raster r2, double tolerance)
    {
      // compare params for base and test to be sure they are the same
      Assert.assertEquals(r1.getMinX(), r2.getMinX());
      Assert.assertEquals (r1.getMinY(),  r2.getMinY());
      Assert.assertEquals (r1.getWidth(),  r2.getWidth());
      Assert.assertEquals (r1.getHeight(),  r2.getHeight());
      Assert.assertEquals(r1.getNumBands(), r2.getNumBands());

      // var initialization, potential for improving code optimization by initializing upfront
      int rx, ry;
      int dx, dy;
      double v1, v2;
      int band;

      for (rx = 0; rx < r1.getWidth(); rx++)
      {
        for (ry = 0; ry < r1.getHeight(); ry++)
        {
          dx = rx + r1.getMinX();
          dy = ry + r1.getMinY();
          for (band = 0; band < r1.getNumBands(); band++)
          {
            v1 = r1.getSampleDouble(dx, dy, band);
            v2 = r2.getSampleDouble(dx, dy, band);
            addToStats(v1, v2, tolerance, band);
          }
        }
      }
    }

    @Override
    public String toString()
    {
      String result = String.format("\nWidth = %d\n", width);
      result += String.format("Height = %d\n", height);
      result += String.format("Max Delta: %.10f\n", maxDelta);
      result += String.format("Max %% Delta: %.10f\n", maxPercentDelta);
      result += String.format("Mean Delta: %.10f\n", meanDelta);
      result += String.format("Mean %% Delta: %.10f\n", meanPercentDelta);
      result += String.format("NaN Mismatch: %d\n", nanMismatch);
      result += String.format("Diff Count: %d\n", diffCount);
      result += String.format("Non-NaN Count: %d\n", count);
      result += String.format("Pixel %% error: %.10f\n", pixelPercentError);
      result += String.format("Number of bands: %d\n", numBands);
      for(int b=0;b<numBands;b++){
        result += String.format("Min value[%d]: %.10f\n", b, minValue[b]);
        result += String.format("Max Value[%d]: %.10f\n", b, maxValue[b]);
      }
      return result;
    }
  }

  protected final String outputLocal;
  protected final String inputLocal;
  protected final Path outputHdfs;
  protected final Path inputHdfs;
  protected final Path testLocal;

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

  public static void setJarLocation() throws IOException
  {
    File working = new File(Defs.CWD);
    String name = working.getName();

    File target = new File("target");
    File jar = new File("target/" + name + "-full.jar");

    if (jar.exists())
    {
      MrGeoProperties.getInstance().setProperty(MrGeoConstants.MRGEO_JAR,
          jar.getCanonicalPath());
    }

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

  public Path getOutputHdfs()
  {
    return outputHdfs;
  }

  public Path getOutputHdfsFor(final String fileName)
  {
    return new Path(outputHdfs, fileName);
  }

  public String getOutputLocal()
  {
    return outputLocal;
  }

  public Path getTestLocal()
  {
    return testLocal;
  }

  public static DiffStats calculateDiffStats(MrsImage baseMImg, MrsImage testMImg, double tolerance)
      throws IOException
  {
    final int tileSize = baseMImg.getTilesize();

    return calculateDiffStats(baseMImg.getRenderedImage(), testMImg.getRenderedImage(),
        tolerance, tileSize);
  }

  public static DiffStats calculateDiffStats(RenderedImage i1, RenderedImage i2, double tolerance)
  {
    final int tileSize=MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT_INT;
    return calculateDiffStats(i1, i2, tolerance, tileSize);
  }

  public static DiffStats calculateDiffStats(RenderedImage i1, RenderedImage i2, double tolerance,
      int tileSize)
  {
    // compare base and test params to be sure equivalent
    Assert.assertEquals (i1.getWidth() , i2.getWidth());
    Assert.assertEquals (i1.getHeight() , i2.getHeight());
    Assert.assertEquals (i1.getMinTileX() , i2.getMinTileX());
    Assert.assertEquals (i1.getMinTileY() , i2.getMinTileY());

    // initializations upfront to hopefully improve code optimization
    int width, height;
    int x, y;

    DiffStats result = new DiffStats();
    result.calculateExtremes(i1, i2, tileSize);

    for (x = 0; x < i1.getWidth(); x += tileSize)
    {
      for (y = 0; y < i1.getHeight(); y += tileSize)
      {
        width = tileSize;
        height = tileSize;
        if (x + width > i1.getWidth()) width = i1.getWidth() - x;
        if (y + height > i1.getHeight()) height = i1.getHeight() - y;

        Rectangle rect = new Rectangle(x, y, width, height);
        Raster r1 = i1.getData(rect);
        Raster r2 = i2.getData(rect);
        result.calculateDiffStats(r1, r2, tolerance);
      }
    }
    result.pixelPercentError = (double)result.diffCount / (double)(result.count * result.numBands);
    result.width = i1.getWidth();
    result.height = i1.getHeight();

    return result;
  }

  public static DiffStats calculateDiffStats(RenderedImage base, RenderedImage test)
  {
    // default tile size = 512
    final int tileSize = MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT_INT;
    // Default tolerance is 1e-5 for pixel comparisons
    final double tolerance = 1e-5;

    return calculateDiffStats(base, test, tolerance, tileSize);
  }

  public static String readPath(Path p) throws FileNotFoundException, IOException
  {
    FileSystem fs = HadoopFileUtils.getFileSystem(p);
    FSDataInputStream fdis = fs.open(p);
    try
    {
      FileStatus stat = fs.getFileStatus(p);

      byte[] baselineBuffer = new byte[(int)stat.getLen()];
      fdis.read(baselineBuffer);
      return new String(baselineBuffer);
    }
    finally
    {
      fdis.close();
    }
  }

  public static String readFile(File f) throws FileNotFoundException, IOException
  {
    byte[] baselineBuffer = new byte[(int)f.length()];
    FileInputStream s = new FileInputStream(f);
    s.read(baselineBuffer);
    s.close();
    return new String(baselineBuffer);
  }

  public static MrsImagePyramidMetadata readImageMetadata(String filename) throws JsonGenerationException, JsonMappingException,
      IOException
  {
    FileInputStream stream = new FileInputStream(filename);
    try
    {
      return MrsImagePyramidMetadata.load(stream);
    }
    finally
    {
      stream.close();
    }
  }

  public static String calculateHash(File f) throws IOException
  {
    return calculateHash(readFile(f));
  }

  public static String calculateHash(String s)
  {
    MessageDigest digest;
    try
    {
      digest = MessageDigest.getInstance("MD5");
    }
    catch (NoSuchAlgorithmException e)
    {
      e.printStackTrace();
      assert (false);
      return "error";
    }

    digest.update(s.getBytes(), 0, s.getBytes().length);
    return new BigInteger(1, digest.digest()).toString(16);
  }

  public static String calculateHash(Path p) throws IOException
  {
    MessageDigest digest;
    try
    {
      digest = MessageDigest.getInstance("MD5");
    }
    catch (NoSuchAlgorithmException e)
    {
      e.printStackTrace();
      assert (false);
      return "error";
    }

    FileSystem fs = HadoopFileUtils.getFileSystem(p);
    byte[] buffer = new byte[(int) fs.getFileStatus(p).getLen()];

    for (int i = 0; i < buffer.length; i++)
    {
      buffer[i] = 0;
    }

    digest.update(buffer, 0, buffer.length);
    FSDataInputStream fdis = fs.open(p);
    fdis.read(buffer);
    fdis.close();
    return new BigInteger(1, digest.digest()).toString(16);
  }

  public static String composeInputDir(Class<?> c)
  {
    String pn = packageName(c);
    String cn = c.getName().replace(pn + ".", "");
    String result = Defs.INPUT + pn + "/" + cn + "/";

    File f = new File (result);
    try
    {
      return f.getCanonicalPath() + "/";
    }
    catch (IOException e)
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
      FileUtils.deleteDir(dir);
    }

    if (dir.mkdirs() == false)
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

    if (fs.mkdirs(result) == false)
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
      if (fs.mkdirs(result) == false)
      {
        throw new IOException("Error creating test input HDFS directory (" + result + ")");
      }
    }
    return result;
  }

  public static Path composeInputHdfs(Class<?> c) throws IOException
  {
    return composeInputHdfs(c, false);
  }

  public static String packageName(Class<?> c)
  {
    String name = c.getName();
    int li = name.lastIndexOf(".");
    return name.substring(0, li);
  }
  public void compareRasters(String testName, Raster r2) throws IOException
  {
    final File baselineTif = new File(new File(inputLocal), testName + ".tif");
    compareRasters(baselineTif, r2);
  }

  public static void compareRasters(File r1, Raster r2) throws IOException
  {
    BufferedImage bi = ImageIO.read(r1);
    compareRasters(bi.getData(), null, r2, null);
  }

  public void compareRasters(String testName, ValueTranslator t1, Raster r2, ValueTranslator t2) throws IOException
  {
    final File baselineTif = new File(new File(inputLocal), testName + ".tif");
    compareRasters(baselineTif, t1, r2, t2);
  }

  public static void compareRasters(File r1, ValueTranslator t1, Raster r2, ValueTranslator t2) throws IOException
  {
    BufferedImage bi = ImageIO.read(r1);
    compareRasters(bi.getData(), t1, r2, t2);
  }

  /**
   * @param r1
   * @param r2
   */
  public static void compareRasters(Raster r1, Raster r2)
  {
    compareRasters(r1, null, r2, null);
  }

  /**
   * If no translation of values should occur for one or both rendered images,
   * pass null for that translator parameter.
   *
   * @param r1
   * @param r1Translator
   * @param r2
   * @param r2Translator
   */
  public static void compareRasters(Raster r1, ValueTranslator r1Translator,
      Raster r2, ValueTranslator r2Translator)
  {
    int dataType = r1.getDataBuffer().getDataType();
    boolean intish = dataType == DataBuffer.TYPE_BYTE || dataType == DataBuffer.TYPE_INT
        || dataType == DataBuffer.TYPE_SHORT;
    Assert.assertEquals("Image start Xs are different", r1.getMinX(), r2.getMinX());
    Assert.assertEquals("Image start Ys are different", r2.getMinY(), r2.getMinY());
    Assert.assertEquals("Image widths are different", r1.getWidth(), r2.getWidth());
    Assert.assertEquals("Image heights are different", r1.getHeight(),  r2.getHeight());
    Assert.assertEquals("Mismatch in number of bands", r1.getNumBands(), r2.getNumBands());
    for (int rx = 0; rx < r1.getWidth(); rx++)
    {
      for (int ry = 0; ry < r1.getHeight(); ry++)
      {
        int dx = rx + r1.getMinX();
        int dy = ry + r2.getMinY();
        for (int band = 0; band < r1.getNumBands(); band++)
        {
          float v1 = r1.getSampleFloat(dx, dy, band);
          if (r1Translator != null)
          {
            v1 = r1Translator.translate(v1);
          }
          float v2 = r2.getSampleFloat(dx, dy, band);
          if (r2Translator != null)
          {
            v2 = r2Translator.translate(v2);
          }

          if (Float.isNaN(v1) != Float.isNaN(v2))
          {
            Assert.assertEquals("Pixel NaN mismatch: px: " + dx + " py: " +  dy
                + " b: " + band + " v1: " + v1 + " v2: " + v2,  v1, v2, 0);
          }

          // make delta something reasonable relative to the data

          //NOTE: this formula is not very reliable.  An error of 2e-3f for
          //  pixel v1=1 fails, but passes for v1=2.
          float delta = intish ? 1.0001f : Math.max(Math.abs(v1 * 1e-3f), 1e-3f);
          Assert.assertEquals("Pixel value mismatch: px: " + dx + " py: " +  dy
              + " b: " + band + " v1: " + v1 + " v2: " + v2,  v1, v2, delta);
        }
      }
    }
  }

  public static void compareRasterToConstant(Raster raster, double constant, double nodata)
  {
    int dataType = raster.getDataBuffer().getDataType();
    boolean intish = dataType == DataBuffer.TYPE_BYTE || dataType == DataBuffer.TYPE_INT
        || dataType == DataBuffer.TYPE_SHORT;

    boolean nodataIsNan = Double.isNaN(nodata);

    double delta = intish ? 1.0001 : Math.max(Math.abs(constant * 1e-3), 1e-3);

    for (int rx = 0; rx <raster.getWidth(); rx++)
    {
      for (int ry = 0; ry < raster.getHeight(); ry++)
      {
        int dx = rx + raster.getMinX();
        int dy = ry + raster.getMinY();
        for (int band = 0; band < raster.getNumBands(); band++)
        {
          double v = raster.getSampleDouble(dx, dy, band);

          if ((nodataIsNan && (!Double.isNaN(v))) || (!nodataIsNan && (v != nodata)))
          {
            Assert.assertEquals("Pixel value mismatch: px: " + dx + " py: " +  dy
                + " v: " + v + " const: " + constant,  constant, v, delta);
          }
        }
      }
    }
  }

  public void compareRenderedImages(String testName, RenderedImage r2) throws IOException
  {
    final File baselineTif = new File(new File(inputLocal), testName + ".tif");
    compareRenderedImages(baselineTif, r2);
  }

  public static void compareRenderedImages(File f1, RenderedImage r2) throws IOException
  {
    BufferedImage bi = ImageIO.read(f1);
    compareRenderedImages(bi, r2);
  }

  public static void compareRenderedImages(RenderedImage i1, RenderedImage i2)
  {
    compareRenderedImages(i1, null, i2, null);
  }

  /**
   * If no translation of values should occur for one or both rendered images,
   * pass null for that translator parameter.
   *
   * @param i1
   * @param i1Translator
   * @param i2
   * @param i2Translator
   */
  public static void compareRenderedImages(RenderedImage i1, ValueTranslator i1Translator,
      RenderedImage i2, ValueTranslator i2Translator)
  {
    OpImageRegistrar.registerMrGeoOps();

    Assert.assertEquals("Widths are different", i1.getWidth(), i2.getWidth());
    Assert.assertEquals("Heights are different", i1.getHeight(), i2.getHeight());
    Assert.assertEquals("MinTileX is different", i1.getMinTileX(), i2.getMinTileX());
    Assert.assertEquals("MinTileY is different", i1.getMinTileY(),  i2.getMinTileY());

    final int TILE_SIZE = 2048;

    for (int x = 0; x < i1.getWidth(); x += TILE_SIZE)
    {
      for (int y = 0; y < i1.getHeight(); y += TILE_SIZE)
      {
        int width = TILE_SIZE;
        int height = TILE_SIZE;
        if (x + width > i1.getWidth())
        {
          width = i1.getWidth() - x;
        }
        if (y + height > i1.getHeight())
        {
          height = i1.getHeight() - y;
        }

        Rectangle rect = new Rectangle(x, y, width, height);
        TestUtils.compareRasters(i1.getData(rect), i1Translator,
            i2.getData(rect), i2Translator);
      }
    }
  }
  public static void compareMrsImageToConstant(MrsImage i, double constant)
  {
    OpImageRegistrar.registerMrGeoOps();

    Raster raster = RasterTileMerger.mergeTiles(i);

    Double nodata = i.getMetadata().getDefaultValue(0);

    TestUtils.compareRasterToConstant(raster, constant, nodata);
  }

  public static void compareRenderedImageToConstant(RenderedImage i, double constant, double nodata)
  {
    OpImageRegistrar.registerMrGeoOps();

    TestUtils.compareRasterToConstant(i.getData(), constant, nodata);
  }


  public static void saveRaster(Raster raster, String type, String filename) throws IOException
  {
    BufferedImage img = RasterUtils.makeBufferedImage(raster);
    ImageIO.write(img, type.toUpperCase(), new File(filename));
  }

  private static int getPixelId(int x, int y, int width)
  {
    return x + (y * width);
  }


  public static WritableRaster createConstRaster(int width, int height, double c)
  {
    SampleModel sm = new BandedSampleModel(DataBuffer.TYPE_DOUBLE, width, height, 1);
    WritableRaster raster = Raster.createWritableRaster(sm, new Point(0, 0));

    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        raster.setSample(x, y, 0, c);
      }
    }

    return raster;
  }

  public static WritableRaster createConstRasterWithNodata(int width, int height, double c, double nodata, int nodataFrequency)
  {
    SampleModel sm = new BandedSampleModel(DataBuffer.TYPE_DOUBLE, width, height, 1);
    WritableRaster raster = Raster.createWritableRaster(sm, new Point(0, 0));

    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        int pixelId = getPixelId(x, y, width);

        raster.setSample(x, y, 0,
            ((pixelId % nodataFrequency) == 0) ? nodata : c);
      }
    }

    return raster;
  }

  public static WritableRaster createNumberedRaster(int width, int height)
  {
    SampleModel sm = new BandedSampleModel(DataBuffer.TYPE_DOUBLE, width, height, 1);
    WritableRaster raster = Raster.createWritableRaster(sm, new Point(0, 0));

    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        int pixelId = getPixelId(x, y, width);
        raster.setSample(x, y, 0, (double)pixelId);
      }
    }

    return raster;
  }

  public static WritableRaster createNumberedRasterWithNodata(int width, int height, double nodata, int nodataFrequency)
  {
    SampleModel sm = new BandedSampleModel(DataBuffer.TYPE_DOUBLE, width, height, 1);
    WritableRaster raster = Raster.createWritableRaster(sm, new Point(0, 0));

    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        int pixelId = getPixelId(x, y, width);

        raster.setSample(x, y, 0,
            ((pixelId % nodataFrequency) == 0) ? nodata : (double)pixelId);
      }
    }

    return raster;
  }

  public void generateBaselineTif(final String testName,
      final Raster raster, Bounds bounds)
      throws IOException, JobFailedException, JobCancelledException, ParserException
  {
    BufferedImage image = RasterUtils.makeBufferedImage(raster);
    generateBaselineTif(testName, image, bounds, Double.NaN);
  }

  public void generateBaselineTif(final String testName, final Raster raster)
      throws IOException, JobFailedException, JobCancelledException, ParserException
  {
    BufferedImage image = RasterUtils.makeBufferedImage(raster);
    generateBaselineTif( testName, image, Bounds.world, Double.NaN);
  }
  public void generateBaselineTif(final String testName,
      final BufferedImage image, Bounds bounds)
      throws IOException, JobFailedException, JobCancelledException, ParserException
  {
    generateBaselineTif(testName, image, bounds, Double.NaN);
  }

  public void generateBaselineTif(final String testName, final RenderedImage image)
      throws IOException, JobFailedException, JobCancelledException, ParserException
  {
    generateBaselineTif(testName, image, Bounds.world, Double.NaN);
  }


  public void generateBaselineTif(final String testName,
      final RenderedImage image, Bounds bounds, double nodata)
      throws IOException, JobFailedException, JobCancelledException, ParserException
  {

    double pixelsize = bounds.getWidth() / image.getWidth();
    int zoom = TMSUtils.zoomForPixelSize(pixelsize, image.getWidth());

    TMSUtils.Bounds tb = new TMSUtils.Bounds(bounds.getMinX(), bounds.getMinY(), bounds.getMaxX(),
        bounds.getMaxY());
    tb = TMSUtils.tileBounds(tb, zoom, image.getWidth());

    final File baselineTif = new File(new File(inputLocal), testName + ".tif");

    GeoTiffExporter.export(image, tb.asBounds(), baselineTif, false, nodata);
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
        InputStreamReader isr = new InputStreamReader(is1);

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
        InputStreamReader isr = new InputStreamReader(is2);

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



}
