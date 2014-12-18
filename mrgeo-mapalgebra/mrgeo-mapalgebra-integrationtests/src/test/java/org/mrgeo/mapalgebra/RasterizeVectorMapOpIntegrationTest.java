package org.mrgeo.mapalgebra;

import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.junit.IntegrationTest;
import org.mrgeo.test.TestUtils;
import org.mrgeo.utils.HadoopUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

@SuppressWarnings("static-method")
public class RasterizeVectorMapOpIntegrationTest
{

  private static final Logger _log = LoggerFactory.getLogger(RasterizeVectorMapOpIntegrationTest.class);
  private static Path inputHdfs;
  private static String input;
  private static String shapefile = "major_road_intersections_exploded.shp";
  private static String hdfsShapefile;
  private Properties props;


  @BeforeClass
  public static void init() throws IOException
  {
    input = TestUtils.composeInputDir(RasterizeVectorMapOpIntegrationTest.class);
    inputHdfs = TestUtils.composeInputHdfs(RasterizeVectorMapOpIntegrationTest.class);
    Path hdfsShapePath = new Path(inputHdfs, shapefile);
    HadoopFileUtils.delete(hdfsShapePath);
    HadoopFileUtils.copyToHdfs(new Path(input), inputHdfs, shapefile);
    hdfsShapefile = hdfsShapePath.toString();
  }
  
  @AfterClass
  public static void teardown() throws IOException
  {
    HadoopFileUtils.delete(new Path(hdfsShapefile));
  }

  @Before
  public void setup()
  {
    props = null;
  }

  @Test
  @Category(IntegrationTest.class)
  public void testProcessChildrenMaskWithBounds()
  {
    String exp = "RasterizeVector([" + hdfsShapefile + "], \"MASK\", 0.0001716614, 68.85, 34.25, 69.35, 34.75)";
    MapAlgebraParser parser = new MapAlgebraParser(HadoopUtils.createConfiguration(), "", props);
    try {
      parser.parse(exp);
    } catch (Exception e) {
      _log.error("Failed to parse map algebra expression", e);
      Assert.fail("Failed to parse map algebra expression");
      
    }
    
  }

  @Test
  @Category(IntegrationTest.class)
  public void testProcessChildrenMaskWithQuotedNegativeBounds()
  {
    String exp = "RasterizeVector([" + hdfsShapefile + "], \"MASK\", 0.0001716614, \"-68.85\", 34.25, \"-69.35\", 34.75)";
    MapAlgebraParser parser = new MapAlgebraParser(HadoopUtils.createConfiguration(), "", props);
    try {
      parser.parse(exp);
    } catch (Exception e) {
      _log.error("Failed to parse map algebra expression", e);
      Assert.fail("Failed to parse map algebra expression");
      
    }
    
  }

  @Test
  @Category(IntegrationTest.class)
  public void testProcessChildrenMaskWithoutBounds()
  {
    String exp = "RasterizeVector([" + hdfsShapefile + "], \"MASK\", 0.0001716614)";
    MapAlgebraParser parser = new MapAlgebraParser(HadoopUtils.createConfiguration(), "", props);
    try {
      parser.parse(exp);
    } catch (Exception e) {
      _log.error("Failed to parse map algebra expression", e);
      Assert.fail("Failed to parse map algebra expression");
      
    }
    
  }

  @Test
  @Category(IntegrationTest.class)
  public void testProcessChildrenSumWithoutBounds()
  {
    String exp = "RasterizeVector([" + hdfsShapefile + "], \"SUM\", 0.0001716614)";
    MapAlgebraParser parser = new MapAlgebraParser(HadoopUtils.createConfiguration(), "", props);
    try {
      parser.parse(exp);
    } catch (Exception e) {
      _log.error("Failed to parse map algebra expression", e);
      Assert.fail("Failed to parse map algebra expression");
      
    }
    
  }

  @Test
  @Category(IntegrationTest.class)
  public void testProcessChildrenSumWithColumnWithoutBounds()
  {
    String exp = "RasterizeVector([" + hdfsShapefile + "], \"SUM\", 0.0001716614, \"column\")";
    MapAlgebraParser parser = new MapAlgebraParser(HadoopUtils.createConfiguration(), "", props);
    try {
      parser.parse(exp);
    } catch (Exception e) {
      _log.error("Failed to parse map algebra expression", e);
      Assert.fail("Failed to parse map algebra expression");
      
    }
    
  }

  @Test
  @Category(IntegrationTest.class)
  public void testProcessChildrenLastWithBounds()
  {
    String exp = "RasterizeVector([" + hdfsShapefile + "], \"LAST\", 0.0001716614, \"column\", 68.85, 34.25, 69.35, 34.75)";
    MapAlgebraParser parser = new MapAlgebraParser(HadoopUtils.createConfiguration(), "", props);
    try {
      parser.parse(exp);
    } catch (Exception e) {
      _log.error("Failed to parse map algebra expression", e);
      Assert.fail("Failed to parse map algebra expression");
      
    }
    
  }

  @Test
  @Category(IntegrationTest.class)
  public void testProcessChildrenSumWithBounds()
  {
    String exp = "RasterizeVector([" + hdfsShapefile + "], \"SUM\", 0.0001716614, \"column\", 68.85, 34.25, 69.35, 34.75)";
    MapAlgebraParser parser = new MapAlgebraParser(HadoopUtils.createConfiguration(), "", props);
    try {
      parser.parse(exp);
    } catch (Exception e) {
      _log.error("Failed to parse map algebra expression", e);
      Assert.fail("Failed to parse map algebra expression");
      
    }
    
  }

  @Test
  @Category(IntegrationTest.class)
  public void testProcessChildrenLastWithoutBounds()
  {
    String exp = "RasterizeVector([" + hdfsShapefile + "], \"LAST\", 0.0001716614, \"column\")";
    MapAlgebraParser parser = new MapAlgebraParser(HadoopUtils.createConfiguration(), "", props);
    try {
      parser.parse(exp);
    } catch (Exception e) {
      _log.error("Failed to parse map algebra expression", e);
      Assert.fail("Failed to parse map algebra expression");
      
    }
    
  }

  @Test(expected=IllegalArgumentException.class)
  @Category(IntegrationTest.class)
  public void testProcessChildrenLastWithoutColumn() throws Exception
  {
    String exp = "RasterizeVector([" + hdfsShapefile + "], \"LAST\", 0.0001716614)";
    MapAlgebraParser parser = new MapAlgebraParser(HadoopUtils.createConfiguration(), "", props);
    parser.parse(exp);
  }

  @Test(expected=IllegalArgumentException.class)
  @Category(IntegrationTest.class)
  public void testProcessChildrenMaskWithColumn() throws Exception
  {
    String exp = "RasterizeVector([" + hdfsShapefile + "], \"MASK\", 0.0001716614, \"column\")";
    MapAlgebraParser parser = new MapAlgebraParser(HadoopUtils.createConfiguration(), "", props);
    parser.parse(exp);
  }

  @Test
  @Category(IntegrationTest.class)
  public void testProcessChildrenSumWithoutColumnWithBounds()
  {
    String exp = "RasterizeVector([" + hdfsShapefile + "], \"SUM\", 0.0001716614, 68.85, 34.25, 69.35, 34.75)";
    MapAlgebraParser parser = new MapAlgebraParser(HadoopUtils.createConfiguration(), "", props);
    try {
      parser.parse(exp);
    } catch (Exception e) {
      _log.error("Failed to parse map algebra expression", e);
      Assert.fail("Failed to parse map algebra expression");
      
    }
    
  }

  @Test(expected=IllegalArgumentException.class)
  @Category(IntegrationTest.class)
  public void testProcessChildrenSumWithBadBounds3() throws Exception
  {
    String exp = "RasterizeVector([" + hdfsShapefile + "], \"SUM\", 0.0001716614, \"column\", 68.85, 34.25, 69.35)";
    MapAlgebraParser parser = new MapAlgebraParser(HadoopUtils.createConfiguration(), "", props);
    parser.parse(exp);
  }

  @Test(expected=IllegalArgumentException.class)
  @Category(IntegrationTest.class)
  public void testProcessChildrenSumWithBadBounds2() throws Exception
  {
    String exp = "RasterizeVector([" + hdfsShapefile + "], \"SUM\", 0.0001716614, \"column\", 34.25, 69.35)";
    MapAlgebraParser parser = new MapAlgebraParser(HadoopUtils.createConfiguration(), "", props);
    parser.parse(exp);
  }

  @Test(expected=IllegalArgumentException.class)
  @Category(IntegrationTest.class)
  public void testProcessChildrenSumWithBadBounds1() throws Exception
  {
    String exp = "RasterizeVector([" + hdfsShapefile + "], \"SUM\", 0.0001716614, \"column\", 68.85)";
    MapAlgebraParser parser = new MapAlgebraParser(HadoopUtils.createConfiguration(), "", props);
    parser.parse(exp);
  }

  @Test(expected=IllegalArgumentException.class)
  @Category(IntegrationTest.class)
  public void testProcessChildrenBadAggregationType() throws Exception
  {
    String exp = "RasterizeVector([" + hdfsShapefile + "], \"BAD\", 0.0001716614, \"column\", 68.85, -34.25, 69.35, -34.75)";
    MapAlgebraParser parser = new MapAlgebraParser(HadoopUtils.createConfiguration(), "", props);
    parser.parse(exp);
  }

  @Test(expected=IllegalArgumentException.class)
  @Category(IntegrationTest.class)
  public void testProcessChildrenMissingQuotesAggregationType() throws Exception
  {
    String exp = "RasterizeVector([" + hdfsShapefile + "], SUM, 0.0001716614, \"column\", 68.85, -34.25, 69.35, -34.75)";
    MapAlgebraParser parser = new MapAlgebraParser(HadoopUtils.createConfiguration(), "", props);
    parser.parse(exp);
  }
  
  @Test
  @Category(IntegrationTest.class)
  public void testProcessChildrenVariable()
  {
    String exp = String.format("a = [%s]; RasterizeVector(a, \"LAST\", 1, \"c\") ", hdfsShapefile);
    MapAlgebraParser parser = new MapAlgebraParser(HadoopUtils.createConfiguration(), "", props);
    try {
      parser.parse(exp);
    } catch (Exception e) {
      _log.error("Failed to parse map algebra expression", e);
      Assert.fail("Failed to parse map algebra expression");
      
    }
    
  }


}
