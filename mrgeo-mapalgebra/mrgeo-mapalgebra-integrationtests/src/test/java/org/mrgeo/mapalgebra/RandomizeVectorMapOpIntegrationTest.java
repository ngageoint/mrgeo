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

package org.mrgeo.mapalgebra;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.junit.IntegrationTest;
import org.mrgeo.test.LocalRunnerTest;
import org.mrgeo.test.MapOpTestVectorUtils;
import org.mrgeo.test.TestUtils;

public class RandomizeVectorMapOpIntegrationTest extends LocalRunnerTest
{
  private static MapOpTestVectorUtils testUtils;
  private static String _inputSingleTsv;
  private static String _inputMultiTsv;

  @BeforeClass
  public static void init() throws IOException
  {
      // Forces it to wipeout the input directory (since we copy files to it).
      TestUtils.composeInputHdfs(RandomizeVectorMapOpIntegrationTest.class, true);
      testUtils = new MapOpTestVectorUtils(RandomizeVectorMapOpIntegrationTest.class);
      
      HadoopFileUtils.copyToHdfs(
        new Path(testUtils.getInputLocal()), testUtils.getInputHdfs(), "inputSingle.tsv");
      HadoopFileUtils.copyToHdfs(
        new Path(testUtils.getInputLocal()), testUtils.getInputHdfs(), 
        "inputSingle.tsv.columns");
      _inputSingleTsv = new Path(testUtils.getInputHdfs(), "inputSingle.tsv").toString();

      HadoopFileUtils.copyToHdfs(
          new Path(testUtils.getInputLocal()), testUtils.getInputHdfs(), "inputMulti.tsv");
        HadoopFileUtils.copyToHdfs(
          new Path(testUtils.getInputLocal()), testUtils.getInputHdfs(), 
          "inputMulti.tsv.columns");
      _inputMultiTsv = new Path(testUtils.getInputHdfs(), "inputMulti.tsv").toString();
  }

  @Test
  @Category(IntegrationTest.class)
  public void testRandomizeFromTsvSingleFile() throws Exception
  {
    String expression = 
      String.format("RandomizeVector([%s])",
        _inputSingleTsv).replace("'", "\"");
    String testName = "testRandomizeFromTsvSingleFile";
    testUtils.runMapAlgebraExpression(conf, testName + ".tsv", expression);
    List actualOutput = testUtils.readVectorOutputAsText(conf, new Path(testUtils.getOutputHdfs(), testName + ".tsv"));
    List input = testUtils.readVectorOutputAsText(conf, new Path(testUtils.getInputHdfs(), "inputSingle.tsv"));
    Assert.assertEquals(input.size(), actualOutput.size());
    Assert.assertFalse("Expected randomized output to differ from input", actualOutput.equals(input));

    // columns may be different orders, not sure what to do here...
//    Collections.sort(actualOutput);
//    Collections.sort(input);
//    Assert.assertTrue("Lists should match after sorting both", actualOutput.equals(input));
  }

  @Test
  @Category(IntegrationTest.class)
  public void testRandomizeFromTsvPartFiles() throws Exception
  {
    String expression = 
      String.format("RandomizeVector([%s])",
        _inputMultiTsv).replace("'", "\"");
    String testName = "testRandomizeFromTsvPartFiles";
    testUtils.runMapAlgebraExpression(conf, testName + ".tsv", expression);
    List actualOutput = testUtils.readVectorOutputAsText(conf, new Path(testUtils.getOutputHdfs(), testName + ".tsv"));
    List input = testUtils.readVectorOutputAsText(conf, new Path(testUtils.getInputHdfs(), "inputMulti.tsv"));
    Assert.assertEquals(input.size(), actualOutput.size());
    Assert.assertFalse("Expected randomized output to differ from input", actualOutput.equals(input));

    // columns may be different orders, not sure what to do here...
//    Collections.sort(actualOutput);
//    Collections.sort(input);
//    Assert.assertTrue("Lists should match after sorting both", actualOutput.equals(input));
  }
}
