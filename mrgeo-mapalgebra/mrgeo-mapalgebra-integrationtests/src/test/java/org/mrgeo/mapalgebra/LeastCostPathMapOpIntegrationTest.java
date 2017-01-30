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

package org.mrgeo.mapalgebra;

import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.vector.VectorDataProvider;
import org.mrgeo.data.vector.VectorReader;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.hdfs.vector.DelimitedVectorReader;
import org.mrgeo.job.JobCancelledException;
import org.mrgeo.job.JobFailedException;
import org.mrgeo.junit.IntegrationTest;
import org.mrgeo.mapalgebra.parser.ParserException;
import org.mrgeo.test.LocalRunnerTest;
import org.mrgeo.test.MapOpTestUtils;

import java.io.IOException;

@SuppressWarnings("all") // Test code, not included in production
public class LeastCostPathMapOpIntegrationTest extends LocalRunnerTest
{
private static final String LEAST_COST_PATH_OUTPUT = "testLeastCostPath";
private static final String LEAST_COST_PATH_INPUT = "testCostDistance";
private static final int LEAST_COST_PATH_INPUT_ZOOM = 10;
private static final int LOWER_ZOOM_LEVEL = 9;
private static MapOpTestUtils testUtils;
private final String costSurface = testUtils.getInputHdfs()
    + "/" + LEAST_COST_PATH_INPUT; // + "/" + LEAST_COST_PATH_INPUT_ZOOM;

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
    JobCancelledException
{

  String tsvFileName = testName + ".tsv";
  Path testOutputPath = new Path(outputHdfs, tsvFileName);
  HadoopFileUtils.delete(testOutputPath);

  MapAlgebra.mapalgebra(expression, testOutputPath.toString(),
      conf, ProviderProperties.fromDelimitedString(""), null);

  // get the path and cost of the result
//    PathCost gotPathCost = getPathCost(testOutputPath, conf);

//    Assert.assertEquals(expectedCost, gotPathCost.cost, 0.05);
//    Assert.assertEquals(expectedDistance, gotPathCost.distance, 0.05);
//    Assert.assertEquals(expectedMinSpeed, gotPathCost.minSpeed, 0.05);
//    Assert.assertEquals(expectedMaxSpeed, gotPathCost.maxSpeed, 0.05);
//    Assert.assertEquals(expectedAvgSpeed, gotPathCost.avgSpeed, 0.05);

  VectorDataProvider vdp = DataProviderFactory.getVectorDataProvider(
      testOutputPath.toString(),
      DataProviderFactory.AccessMode.READ,
      conf);
  Assert.assertNotNull(vdp);
  VectorReader reader = vdp.getVectorReader();
  Assert.assertNotNull(reader);
  Assert.assertTrue(reader instanceof DelimitedVectorReader);
  Assert.assertEquals(1, reader.count());
  Geometry geom = reader.get().next();
  Assert.assertNotNull(geom);

  Assert.assertEquals(expectedCost, Double.parseDouble(geom.getAttribute("COST_S")), 0.05);
  Assert.assertEquals(expectedDistance, Double.parseDouble(geom.getAttribute("DISTANCE_M")), 0.05);
  Assert.assertEquals(expectedMinSpeed, Double.parseDouble(geom.getAttribute("MINSPEED_MPS")), 0.05);
  Assert.assertEquals(expectedMaxSpeed, Double.parseDouble(geom.getAttribute("MAXSPEED_MPS")), 0.05);
  Assert.assertEquals(expectedAvgSpeed, Double.parseDouble(geom.getAttribute("AVGSPEED_MPS")), 0.05);

  // Make sure that the expected path is equal to path gotten above - we check for set comparison
  // and not path equality - it is extremely unlikely that the two paths have the same points
  // but in a different order
//    if(checkPath) {
//      Path expectedTsvDir = new Path(testUtils.getInputHdfs(), tsvFileName);
//      PathCost expectedPathCost = getPathCost(expectedTsvDir, conf);
//      Assert.assertEquals(true, gotPathCost.path.equals(expectedPathCost.path));
//    }

}

@Test
@Category(IntegrationTest.class)
public void testLeastCostPath() throws IOException, ParserException, JobFailedException, JobCancelledException
{

  //LoggingUtils.setLogLevel("org.mrgeo.mapalgebra", LoggingUtils.DEBUG);
  long start = System.currentTimeMillis();
  try
  {
    String exp = "destPts = InlineCsv(\"GEOMETRY\", \"'POINT(66.65408 32.13850)'\");\n"
        + "cost = [" + costSurface + "];\n"
        + "result = LeastCostPath(cost, destPts);";

    LeastCostPathMapOpIntegrationTest.runLeastCostPath(conf,
        testUtils.getOutputHdfs(),
        "testLeastCostPath",
        exp,
        true,
        48064f,
        70771.1f,
        0.9d,
        2.2d,
        1.5d
    );
  }
  finally
  {
    System.out.println("test took " + (System.currentTimeMillis() - start));
  }
}

@Test
@Category(IntegrationTest.class)
public void testDestPtOutsideCostSurface()
    throws IOException, ParserException, JobCancelledException, JobFailedException
{
  String exp = "destPts = InlineCsv(\"GEOMETRY\", \"'POINT(66.5896 32.2005)'\");\n"
      + "cost = [" + costSurface + "];\n"
      + "result = LeastCostPath(cost, destPts);";

  try
  {
    LeastCostPathMapOpIntegrationTest.runLeastCostPath(conf,
        testUtils.getOutputHdfs(),
        "testDestPtOutsideCostSurface",
        exp,
        true,
        0f,
        0f,
        0d,
        0d,
        0d
    );
  }
  catch (IllegalStateException e)
  {
    // we don't just use the Expected annotation because MapAlgebraExecutioner throws the generic
    // JobFailedException in the case of an IllegalStateException(ugh!) and expecting a generic
    // JobFailedException may give us true negatives (test passes but code has a bug)
    boolean ret = e.getMessage().contains("falls outside cost surface");
    Assert.assertEquals(true, ret);
  }
}
//private static PathCost getPathCost(Path outputFilePath, Configuration conf) throws IOException {
//  FSDataInputStream fdis = outputFilePath.getFileSystem(conf).open(outputFilePath);
//  BufferedReader br = new BufferedReader(new InputStreamReader(fdis));
//  // Skip the header line
//  String strLine = br.readLine();
//  strLine = br.readLine();
//  String[] splits = strLine.split("\t");
//  String lineString = splits[0];
//  String lineStringSub = lineString.substring(lineString.indexOf('(')+1, lineString.indexOf(')'));
//  String[] gotPixelsStrs = lineStringSub.split(",");
//  Set<String> gotPixels = new HashSet<String>();
//  for(String pixel : gotPixelsStrs) {
//    gotPixels.add(pixel);
//  }
//  br.close();
//  return new PathCost(gotPixels, Float.parseFloat(splits[1]),
//      Float.parseFloat(splits[2]),
//      Double.parseDouble(splits[3]),
//      Double.parseDouble(splits[4]),
//      Double.parseDouble(splits[5])
//  );
//}
//
//private static class PathCost {
//  Set<String> path;
//  float cost;
//  float distance;
//  double minSpeed;
//  double maxSpeed;
//  double avgSpeed;
//  public PathCost(Set<String> path, float cost, float distance, double minSpeed, double maxSpeed, double avgSpeed)
//  {
//    super();
//    this.path = path;
//    this.cost = cost;
//    this.distance = distance;
//    this.minSpeed = minSpeed;
//    this.maxSpeed = maxSpeed;
//    this.avgSpeed = avgSpeed;
//  }
//}

}
