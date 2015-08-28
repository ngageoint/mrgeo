package org.mrgeo.mapalgebra;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.junit.IntegrationTest;
import org.mrgeo.mapalgebra.parser.ParserException;
import org.mrgeo.mapreduce.job.JobCancelledException;
import org.mrgeo.mapreduce.job.JobFailedException;
import org.mrgeo.progress.ProgressHierarchy;
import org.mrgeo.test.MapOpTestUtils;
import org.mrgeo.utils.HadoopUtils;

@SuppressWarnings("static-method")
public class LeastCostPathMapOpIntegrationTest
{
  private static MapOpTestUtils testUtils;
 
  private static final String LEAST_COST_PATH_OUTPUT = "testLeastCostPath";
  
  private static final String LEAST_COST_PATH_INPUT = "testCostDistance";
  private static final int LEAST_COST_PATH_INPUT_ZOOM = 10;
  private static final int LOWER_ZOOM_LEVEL = 9;

  private final String costSurface = testUtils.getInputHdfs()
      + "/" +  LEAST_COST_PATH_INPUT; // + "/" + LEAST_COST_PATH_INPUT_ZOOM;

  @BeforeClass
  public static void init() throws IOException
  {
      testUtils = new MapOpTestUtils(LeastCostPathMapOpIntegrationTest.class);

      HadoopFileUtils.delete(testUtils.getInputHdfs());

      HadoopFileUtils.copyToHdfs(testUtils.getInputLocal(),
          testUtils.getInputHdfs(), 
          LEAST_COST_PATH_INPUT);

      HadoopFileUtils.copyToHdfs(testUtils.getInputLocal(), 
          testUtils.getInputHdfs(), 
          LEAST_COST_PATH_OUTPUT);
  }

  @Test
  @Category(IntegrationTest.class)
  public void testLeastCostPath() throws IOException, ParserException, JobFailedException, JobCancelledException {    

    String exp = "destPts = InlineCsv(\"GEOMETRY\", \"'POINT(66.65408 32.13850)'\");\n"
        + "cost = [" + costSurface + "];\n"
        + "result = LeastCostPath(cost, destPts);";    

    LeastCostPathMapOpIntegrationTest.runLeastCostPath(HadoopUtils.createConfiguration(),
        testUtils.getOutputHdfs(),
        "testLeastCostPath",
        exp,
        true,
        48064f,
        62920.9f,
        0.9d,
        2.0d,
        1.3d
      );
  }

  @Test
  @Category(IntegrationTest.class)
  public void testLeastCostPathWithZoom() throws IOException, ParserException, JobFailedException, JobCancelledException {    

    String exp = "destPts = InlineCsv(\"GEOMETRY\", \"'POINT(66.65408 32.13850)'\");\n"
        + "cost = [" + costSurface + "];\n"
        + "result = LeastCostPath(" + LOWER_ZOOM_LEVEL + ", cost, destPts);";    

    try
    {
      LeastCostPathMapOpIntegrationTest.runLeastCostPath(HadoopUtils.createConfiguration(),
          testUtils.getOutputHdfs(),
          "testLeastCostPathWithZoom",
          exp,
          true,
          48064f,
          62920.9f,
          0.9d,
          2.0d,
          1.3d
        );
    }
    catch(JobFailedException e)
    {
      Assert.assertTrue(e.getMessage().indexOf("does not have an image at zoom level") >= 0);
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void testDestPtOutsideCostSurface() throws IOException, ParserException, JobCancelledException {    

    String exp = "destPts = InlineCsv(\"GEOMETRY\", \"'POINT(66.5896 32.2005)'\");\n"
        + "cost = [" + costSurface + "];\n"
        + "result = LeastCostPath(cost, destPts);";    

    try {
      LeastCostPathMapOpIntegrationTest.runLeastCostPath(HadoopUtils.createConfiguration(),
          testUtils.getOutputHdfs(),
          "testDestPtOutsideCostSurface",
          exp,
          true,
          50000f,
          0f,
          0d,
          0d,
          0d
      );
    } catch(JobFailedException e) {
      // we don't just use the Expected annotation because MapAlgebraExecutioner throws the generic
      // JobFailedException in the case of an IllegalStateException(ugh!) and expecting a generic 
      // JobFailedException may give us true negatives (test passes but code has a bug)
      boolean ret = e.getMessage().contains("falls outside cost surface");
      Assert.assertEquals(true, ret);
    }
  }
  
  public static void runLeastCostPath(Configuration conf,          
      Path outputHdfs, 
      String testName, 
      String expression,
      boolean checkPath, 
      float expectedCost,
      float expectedDistance,
      double expectedMinSpeed,
      double expectedMaxSpeed,
      double expectedAvgSpeed
      ) 
          throws IOException, ParserException, JobFailedException, 
          JobCancelledException {
    MapAlgebraParser uut = new MapAlgebraParser(conf, "", null);

    Path testOutputPath = new Path(outputHdfs, testName);
    HadoopFileUtils.delete(testOutputPath);

    MapAlgebraExecutioner mae = new MapAlgebraExecutioner();

    MapOpHadoop mo = uut.parse(expression);

    mae.setRoot(mo);
    mae.setOutputName(testOutputPath.toString());
    mae.execute(conf, new ProgressHierarchy());

    // get the path and cost of the result
    PathCost gotPathCost = getPathCost(testOutputPath, conf);

    Assert.assertEquals(expectedCost, gotPathCost.cost, 0.05);  
    Assert.assertEquals(expectedDistance, gotPathCost.distance, 0.05);  
    Assert.assertEquals(expectedMinSpeed, gotPathCost.minSpeed, 0.05);  
    Assert.assertEquals(expectedMaxSpeed, gotPathCost.maxSpeed, 0.05);  
    Assert.assertEquals(expectedAvgSpeed, gotPathCost.avgSpeed, 0.05);  

    // Make sure that the expected path is equal to path gotten above - we check for set comparison
    // and not path equality - it is extremely unlikely that the two paths have the same points 
    // but in a different order
    if(checkPath) {
      Path expectedTsvDir = new Path(testUtils.getInputHdfs(), testName);
      PathCost expectedPathCost = getPathCost(expectedTsvDir, conf);
      Assert.assertEquals(true, gotPathCost.path.equals(expectedPathCost.path));
    }

  }
  private static PathCost getPathCost(Path tsvDir, Configuration conf) throws IOException {
    Path outputFilePath = new Path(tsvDir, "leastcostpaths.tsv"); 
    FSDataInputStream fdis = outputFilePath.getFileSystem(conf).open(outputFilePath);
    BufferedReader br = new BufferedReader(new InputStreamReader(fdis));
    String strLine = br.readLine();
    String[] splits = strLine.split("\t");   
    String lineString = splits[0];
    String lineStringSub = lineString.substring(lineString.indexOf('(')+1, lineString.indexOf(')'));
    String[] gotPixelsStrs = lineStringSub.split(",");
    Set<String> gotPixels = new HashSet<String>();    
    for(String pixel : gotPixelsStrs) {
      gotPixels.add(pixel);
    }
    br.close();
    return new PathCost(gotPixels, Float.parseFloat(splits[1]), 
            Float.parseFloat(splits[2]),
            Double.parseDouble(splits[3]),
            Double.parseDouble(splits[4]),
            Double.parseDouble(splits[5])
      );
  }
  
  private static class PathCost {
    Set<String> path;
    float cost;
    float distance;
    double minSpeed;
    double maxSpeed;
    double avgSpeed;
    public PathCost(Set<String> path, float cost, float distance, double minSpeed, double maxSpeed, double avgSpeed)
    {
      super();
      this.path = path;
      this.cost = cost;
      this.distance = distance;
      this.minSpeed = minSpeed;
      this.maxSpeed = maxSpeed;
      this.avgSpeed = avgSpeed;
    }
  }

}
