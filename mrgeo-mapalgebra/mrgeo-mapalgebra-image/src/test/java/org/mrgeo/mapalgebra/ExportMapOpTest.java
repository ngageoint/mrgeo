package org.mrgeo.mapalgebra;

import junit.framework.Assert;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.gdal.gdal.Dataset;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.data.raster.MrGeoRaster;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.mapalgebra.raster.MrsPyramidMapOp;
import org.mrgeo.mapalgebra.raster.RasterMapOp;
import org.mrgeo.test.LocalRunnerTest;
import org.mrgeo.test.TestUtils;
import org.mrgeo.utils.GDALUtils;

import java.io.File;
import java.io.IOException;

@SuppressWarnings("all") // Test code, not included in production
public class ExportMapOpTest extends LocalRunnerTest
{
private static final String onetile = "onetile";
private static TestUtils testUtils;
private static Path onetilePath;
@Rule
public TestName testname = new TestName();
private SparkContext localContext;

@BeforeClass
public static void init() throws IOException
{
  testUtils = new TestUtils(ExportMapOpTest.class);

  HadoopFileUtils.copyToHdfs(testUtils.getInputLocal(), testUtils.getInputHdfs(), onetile, true);
  onetilePath = new Path(testUtils.getInputHdfs(), onetile);

}

@Before
public void setup() throws IOException
{
  SparkConf sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("MrGeo Local Mapalgebra Test")
      .set("spark.ui.enabled", "false");

  localContext = new SparkContext(sparkConf);
}

@Test
@Category(UnitTest.class)
public void exportOneTile() throws IOException
{
  String output = testUtils.getOutputLocalFor(onetile);

  MrsImageDataProvider dp = DataProviderFactory.getMrsImageDataProvider(onetilePath.toString(),
      DataProviderFactory.AccessMode.READ, new ProviderProperties());

  RasterMapOp mapop = MrsPyramidMapOp.apply(dp);
  mapop.context(localContext);
//  def create(raster:RasterMapOp, name:String, singleFile:Boolean = false, zoom:Int = -1, numTiles:Int = -1,
//    mosaic:Int = -1, format:String = "tif", randomTiles:Boolean = false,
//    tms:Boolean = false, colorscale:String = "", tileids:String = "",
//    bounds:String = "", allLevels:Boolean = false, overridenodata:Double = Double.NegativeInfinity):MapOp = {

  MapOp exportmapop = ExportMapOp.create(mapop, output,
      false, -1, -1, -1, "tif", false, false, "", "", "", false,
      Double.NEGATIVE_INFINITY);  // this line is all defaults.

  exportmapop.execute(localContext);

  File parent = new File(output).getParentFile();
  File[] files = parent.listFiles();
  Assert.assertNotNull("No files exported", files);
  Assert.assertEquals("Wrong number of files exported", 1, files.length);

  Dataset gdal = GDALUtils.open(files[0].getCanonicalPath());
  MrGeoRaster exported = MrGeoRaster.fromDataset(gdal);

  testUtils.compareRasters(testname.getMethodName(), exported);
}
}
