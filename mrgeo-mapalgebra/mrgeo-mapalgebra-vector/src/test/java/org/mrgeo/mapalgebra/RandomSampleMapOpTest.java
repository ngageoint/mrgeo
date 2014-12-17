package org.mrgeo.mapalgebra;

import com.vividsolutions.jts.io.WKTReader;
import junit.framework.Assert;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.geometry.GeometryFactory;
import org.mrgeo.geometry.Point;
import org.mrgeo.image.MrsImagePyramid;
import org.mrgeo.mapreduce.job.JobFailedException;
import org.mrgeo.progress.ProgressHierarchy;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.test.LocalRunnerTest;
import org.mrgeo.test.MapOpTestUtils;
import org.mrgeo.utils.Bounds;
import org.mrgeo.utils.TMSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class RandomSampleMapOpTest extends LocalRunnerTest
{
  private static final Logger log = LoggerFactory.getLogger(RandomSampleMapOpTest.class);
  private static MapOpTestUtils testUtils;


  @BeforeClass
  public static void init() throws IOException
  {
      testUtils = new MapOpTestUtils(RandomSampleMapOpTest.class);

  }

  @Test
  @Category(UnitTest.class)
  public void testBasicFromRaster() throws Exception
  {
    int numSamples = 200;
    String greecePath = new java.io.File("testFiles/greece").getAbsolutePath();
    String greeceImageUri = "file://" + greecePath;
    String ex = "RandomSample([" + greeceImageUri + "], " + numSamples + ");";
    String baseName = "randomTestBasicFromRaster";
    String testName = baseName + ".tsv";
    log.debug(ex);
    // Load the source raster so we can get its geographic translator.
    // from which we can get the lat/lon bounds of the image. We'll use
    // this to test the random sample points. The following few lines
    // were lifted from MapAlgebraParser._loadRasterFile.
    MrsImagePyramid pyramid = MrsImagePyramid.open(greeceImageUri, getProviderProperties());
    Bounds imageBounds = pyramid.getBounds();
    double resolution = TMSUtils.resolution(pyramid.getMaximumLevel(),
        pyramid.getMetadata().getTilesize());

    MapAlgebraParser uut = new MapAlgebraParser(this.conf, "", getProviderProperties());
    HadoopFileUtils.delete(new Path(testUtils.getOutputHdfs(), testName));
    MapAlgebraExecutioner mae = new MapAlgebraExecutioner();
    MapOp mo = uut.parse(ex);

    mae.setRoot(mo);
    mae.setOutputName(new Path(testUtils.getOutputHdfs(), testName).toString());
    mae.execute(this.conf, new ProgressHierarchy());

    // read in the output file
    Path p = new Path(testUtils.getOutputHdfs(), testName);
    FileSystem fs = HadoopFileUtils.getFileSystem(this.conf, p);
    // Check that the columns file exists
    Assert.assertTrue(fs.exists(new Path(testUtils.getOutputHdfs(), baseName + ".tsv.columns")));
    // Check that the output file exists and is not empty
    Assert.assertTrue(fs.exists(p));
    long l = fs.getFileStatus(p).getLen();
    assert(l > 0);
    // Read each of the geometries in the output file and make sure they fall
    // within the bounds of the original image file.
    FSDataInputStream is = fs.open(p);
    try
    {
      java.io.BufferedReader r = new java.io.BufferedReader(new java.io.InputStreamReader(is));
      String line = "";
      WKTReader wktReader = new WKTReader();
      int count = 0;
      // Note that the following parsing code also validates that there is no added
      // column included.
      while ((line = r.readLine()) != null) {
        count++;
        Geometry geom = GeometryFactory.fromJTS(wktReader.read(line));
        if (geom != null && geom instanceof Point) {
          Point pt = (Point) geom;
          // The original lat/lon bounds can fall anywhere within a pixel. So if the far left,
          // right, top or bottom pixel is one of the random points generated, the computed
          // lat/lon is the top, left coordinate of the pixel. And that could be within one
          // pixel resolution outside of the original bounds.
          Assert.assertTrue("Y coordinate " + pt.getY() + " is too large", (pt.getY() <= imageBounds.getMaxY() + resolution));
          Assert.assertTrue("Y coordinate " + pt.getY() + " is too small", (pt.getY() >= imageBounds.getMinY() - resolution));
          Assert.assertTrue("X coordinate " + pt.getX() + " is too large", (pt.getX() <= imageBounds.getMaxX() + resolution));
          Assert.assertTrue("X coordinate " + pt.getX() + " is too small", (pt.getX() >= imageBounds.getMinX() - resolution));
        }
      }

      Assert.assertEquals(numSamples, count);
    }
    finally
    {
      is.close();
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testBasicFromBounds() throws Exception
  {
    int numSamples = 200;
    int zoomLevel = 12;
    int tileSize = 512;
    double minX = -180.0;
    double minY = -90.0;
    double maxX = -178.0;
    double maxY = -87.0;
    double resolution = TMSUtils.resolution(zoomLevel, tileSize);
    String ex = "RandomSample(\"" + minX + "\",\"" + minY + "\",\"" + maxX + "\",\"" + maxY + "\"," + zoomLevel + "," + numSamples + ");";
    String baseName = "randomTestBasicFromBounds";
    String testName = baseName + ".tsv";
    log.debug(ex);

    MapAlgebraParser uut = new MapAlgebraParser(this.conf, "", getProviderProperties());
    HadoopFileUtils.delete(new Path(testUtils.getOutputHdfs(), testName));
    MapAlgebraExecutioner mae = new MapAlgebraExecutioner();
    MapOp mo = uut.parse(ex);

    mae.setRoot(mo);
    mae.setOutputName(new Path(testUtils.getOutputHdfs(), testName).toString());
    mae.execute(this.conf, new ProgressHierarchy());

    // read in the output file
    Path p = new Path(testUtils.getOutputHdfs(), testName);
    FileSystem fs = HadoopFileUtils.getFileSystem(this.conf, p);
    // Check that the columns file exists
    Assert.assertTrue(fs.exists(new Path(testUtils.getOutputHdfs(), baseName + ".tsv.columns")));
    // Check that the output file exists and is not empty
    Assert.assertTrue(fs.exists(p));
    long l = fs.getFileStatus(p).getLen();
    assert(l > 0);
    // Read each of the geometries in the output file and make sure they fall
    // within the bounds of the original image file.
    FSDataInputStream is = fs.open(p);
    try
    {
      java.io.BufferedReader r = new java.io.BufferedReader(new java.io.InputStreamReader(is));
      String line = "";
      WKTReader wktReader = new WKTReader();
      int count = 0;
      // Note that the following parsing code also validates that there is no added
      // column included.
      while ((line = r.readLine()) != null) {
        count++;
        Geometry geom = GeometryFactory.fromJTS(wktReader.read(line));
        if (geom != null && geom instanceof Point) {
          Point pt = (Point) geom;
          // The original lat/lon bounds can fall anywhere within a pixel. So if the far left,
          // right, top or bottom pixel is one of the random points generated, the computed
          // lat/lon is the top, left coordinate of the pixel. And that could be within one
          // pixel resolution outside of the original bounds.
          Assert.assertTrue("Y coordinate " + pt.getY() + " is too large", (pt.getY() <= maxY + resolution));
          Assert.assertTrue("Y coordinate " + pt.getY() + " is too small", (pt.getY() >= minY - resolution));
          Assert.assertTrue("X coordinate " + pt.getX() + " is too large", (pt.getX() <= maxX + resolution));
          Assert.assertTrue("X coordinate " + pt.getX() + " is too small", (pt.getX() >= minX - resolution));
        }
      }

      Assert.assertEquals(numSamples, count);
    }
    finally
    {
      is.close();
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testExclusionsFromBounds() throws Exception
  {
    int zoomLevel = 5;
    int tileSize = 512;
    double minX = -180.0;
    double minY = -90.0;
    double maxX = -178.0;
    double maxY = -87.0;
    double resolution = TMSUtils.resolution(zoomLevel, tileSize);
    // Force the exclusion case to make sure it works.
    long numSamples = ((long)(((maxY - minY) / resolution) + 1) * (long)(((maxX - minX) / resolution) - 1));
    String ex = "RandomSample(\"" + minX + "\",\"" + minY + "\",\"" + maxX + "\",\"" + maxY + "\"," + zoomLevel + "," + numSamples + ");";
    String baseName = "randomTestExclusionsFromBounds";
    String testName = baseName + ".tsv";
    log.debug(ex);

    MapAlgebraParser uut = new MapAlgebraParser(this.conf, "", getProviderProperties());
    HadoopFileUtils.delete(new Path(testUtils.getOutputHdfs(), testName));
    MapAlgebraExecutioner mae = new MapAlgebraExecutioner();
    MapOp mo = uut.parse(ex);

    mae.setRoot(mo);
    mae.setOutputName(new Path(testUtils.getOutputHdfs(), testName).toString());
    mae.execute(this.conf, new ProgressHierarchy());

    // read in the output file
    Path p = new Path(testUtils.getOutputHdfs(), testName);
    FileSystem fs = HadoopFileUtils.getFileSystem(this.conf, p);
    // Check that the columns file exists
    Assert.assertTrue(fs.exists(new Path(testUtils.getOutputHdfs(), baseName + ".tsv.columns")));
    // Check that the output file exists and is not empty
    Assert.assertTrue(fs.exists(p));
    long l = fs.getFileStatus(p).getLen();
    assert(l > 0);
    // Read each of the geometries in the output file and make sure they fall
    // within the bounds of the original image file.
    FSDataInputStream is = fs.open(p);
    try
    {
      java.io.BufferedReader r = new java.io.BufferedReader(new java.io.InputStreamReader(is));
      String line = "";
      WKTReader wktReader = new WKTReader();
      int count = 0;
      // Note that the following parsing code also validates that there is no added
      // column included.
      while ((line = r.readLine()) != null) {
        count++;
        Geometry geom = GeometryFactory.fromJTS(wktReader.read(line));
        if (geom != null && geom instanceof Point) {
          Point pt = (Point) geom;
          // The original lat/lon bounds can fall anywhere within a pixel. So if the far left,
          // right, top or bottom pixel is one of the random points generated, the computed
          // lat/lon is the top, left coordinate of the pixel. And that could be within one
          // pixel resolution outside of the original bounds.
          Assert.assertTrue("Y coordinate " + pt.getY() + " is too large", (pt.getY() <= maxY + resolution));
          Assert.assertTrue("Y coordinate " + pt.getY() + " is too small", (pt.getY() >= minY - resolution));
          Assert.assertTrue("X coordinate " + pt.getX() + " is too large", (pt.getX() <= maxX + resolution));
          Assert.assertTrue("X coordinate " + pt.getX() + " is too small", (pt.getX() >= minX - resolution));
        }
      }

      Assert.assertEquals(numSamples, count);
    }
    finally
    {
      is.close();
    }
  }

  @Test
  @Category(UnitTest.class)  
  public void testTooManySamples() throws Exception
  {
    String greecePath = new java.io.File("testFiles/greece").getAbsolutePath();
    String greeceImageUri = "file://" + greecePath;
    // Load the source raster so we can get its geographic translator.
    // from which we can get the lat/lon bounds of the image. We'll use
    // this to test the random sample points. The following few lines
    // were lifted from MapAlgebraParser._loadRasterFile.
    MrsImagePyramid pyramid = MrsImagePyramid.open(greeceImageUri, getProviderProperties());
    Bounds imageBounds = pyramid.getBounds();
    double resolution = TMSUtils.resolution(pyramid.getMaximumLevel(),
        pyramid.getMetadata().getTilesize());

    // Force the number of samples to be one larger than the image pixels
    long numSamples = ((long)((imageBounds.getHeight() / resolution) + 2) * (long)((imageBounds.getWidth() / resolution) + 1));
    String ex = "RandomSample([" + greeceImageUri + "], " + numSamples + ");";
    String baseName = "randomFail";
    String testName = baseName + ".tsv";
    log.debug(ex);
    // Load the source raster so we can get its pixel height/width.

    MapAlgebraParser uut = new MapAlgebraParser(this.conf, "", getProviderProperties());
    HadoopFileUtils.delete(new Path(testUtils.getOutputHdfs(), testName));
    MapAlgebraExecutioner mae = new MapAlgebraExecutioner();
    MapOp mo = uut.parse(ex);

    mae.setRoot(mo);
    mae.setOutputName(new Path(testUtils.getOutputHdfs(), testName).toString());
    try {
      mae.execute(this.conf, new ProgressHierarchy());
      Assert.fail("Expected an IllegalArgument exception for sample count too large");
    }
    catch(JobFailedException e) {
      Assert.assertTrue("Exception message (\"" + e.getMessage() + "\") does not match what we expected",
          e.getMessage().startsWith("RandomSampler cannot generate more sample points"));
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testAddedColumnEmptyColumnName() throws Exception
  {
    int numSamples = 200;
    String greecePath = new java.io.File("testFiles/greece").getAbsolutePath();
    String greeceImageUri = "file://" + greecePath;
    String ex = "RandomSample([" + greeceImageUri + "], " + numSamples + ", \"\", \"positive\");";
    String baseName = "randomAddedColumnEmptyColumnName";
    String testName = baseName + ".tsv";
    log.debug(ex);

    MapAlgebraParser uut = new MapAlgebraParser(this.conf, "", getProviderProperties());
    HadoopFileUtils.delete(new Path(testUtils.getOutputHdfs(), testName));
    //MapAlgebraExecutioner mae = new MapAlgebraExecutioner();
    try {
      @SuppressWarnings("unused")
      MapOp mo = uut.parse(ex);
      Assert.fail("Expected an IllegalArgumentException");
    } catch(IllegalArgumentException e) {
      Assert.assertTrue("Incorrect error message received: " + e.getMessage(),
          e.getMessage().startsWith("The new column name cannot be blank."));
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testAddedColumnMissingValue() throws Exception
  {
    int numSamples = 200;
    String greecePath = new java.io.File("testFiles/greece").getAbsolutePath();
    String greeceImageUri = "file://" + greecePath;
    String ex = "RandomSample([" + greeceImageUri + "], " + numSamples + ", \"class\");";
    String baseName = "randomAddedColumnMissingVlaue";
    String testName = baseName + ".tsv";
    log.debug(ex);

    MapAlgebraParser uut = new MapAlgebraParser(this.conf, "", getProviderProperties());
    HadoopFileUtils.delete(new Path(testUtils.getOutputHdfs(), testName));
    //MapAlgebraExecutioner mae = new MapAlgebraExecutioner();
    try {
      @SuppressWarnings("unused")
      MapOp mo = uut.parse(ex);
      Assert.fail("Expected an IllegalArgumentException");
    } catch(IllegalArgumentException e) {
      Assert.assertTrue("Incorrect error message received: " + e.getMessage(),
          e.getMessage().startsWith("RandomSample usage:"));
    }
  }
  @Test
  @Category(UnitTest.class)
  public void testAddedColumn() throws Exception
  {
    int numSamples = 200;
    String greecePath = new java.io.File("testFiles/greece").getAbsolutePath();
    String greeceImageUri = "file://" + greecePath;
    String ex = "RandomSample([" + greeceImageUri + "], " + numSamples + ", \"class\", \"positive\");";
    String baseName = "randomAddedColumn";
    String testName = baseName + ".tsv";
    log.debug(ex);

    // Load the source raster so we can get its geographic translator.
    // from which we can get the lat/lon bounds of the image. We'll use
    // this to test the random sample points. The following few lines
    // were lifted from MapAlgebraParser._loadRasterFile.
    MrsImagePyramid pyramid = MrsImagePyramid.open(greeceImageUri, getProviderProperties());
    Bounds imageBounds = pyramid.getBounds();
    double resolution = TMSUtils.resolution(pyramid.getMaximumLevel(),
        pyramid.getMetadata().getTilesize());

    MapAlgebraParser uut = new MapAlgebraParser(this.conf, "", getProviderProperties());
    HadoopFileUtils.delete(new Path(testUtils.getOutputHdfs(), testName));
    MapAlgebraExecutioner mae = new MapAlgebraExecutioner();
    MapOp mo = uut.parse(ex);

    mae.setRoot(mo);
    mae.setOutputName(new Path(testUtils.getOutputHdfs(), testName).toString());
    mae.execute(this.conf, new ProgressHierarchy());

    // read in the output file
    Path p = new Path(testUtils.getOutputHdfs(), testName);
    FileSystem fs = HadoopFileUtils.getFileSystem(this.conf, p);
    // Check that the columns file exists
    Assert.assertTrue(fs.exists(new Path(testUtils.getOutputHdfs(), baseName + ".tsv.columns")));
    // Check that the output file exists and is not empty
    Assert.assertTrue(fs.exists(p));
    long l = fs.getFileStatus(p).getLen();
    assert(l > 0);
    // Read each of the geometries in the output file and make sure they fall
    // within the bounds of the original image file.
    FSDataInputStream is = fs.open(p);
    try
    {
      java.io.BufferedReader r = new java.io.BufferedReader(new java.io.InputStreamReader(is));
      String line = "";
      WKTReader wktReader = new WKTReader();
      int count = 0;
      while ((line = r.readLine()) != null) {
        count++;
        // Parse the line
        int lastDelim = line.lastIndexOf('\t');
        String addedColumnValue = line.substring(lastDelim+1).trim();
        Assert.assertEquals("positive", addedColumnValue);
        String strWkt = line.substring(0, lastDelim);
        Geometry geom = GeometryFactory.fromJTS(wktReader.read(line));
        if (geom != null && geom instanceof Point) {
          Point pt = (Point) geom;
          Assert.assertTrue("Y coordinate " + pt.getY() + " is too large", (pt.getY() <= imageBounds.getMaxY() + resolution));
          Assert.assertTrue("Y coordinate " + pt.getY() + " is too small", (pt.getY() >= imageBounds.getMinY() - resolution));
          Assert.assertTrue("X coordinate " + pt.getX() + " is too large", (pt.getX() <= imageBounds.getMaxX() + resolution));
          Assert.assertTrue("X coordinate " + pt.getX() + " is too small", (pt.getX() >= imageBounds.getMinX() - resolution));
        }
      }
      Assert.assertEquals(numSamples, count);
    }
    finally
    {
      is.close();
    }
  }
}
