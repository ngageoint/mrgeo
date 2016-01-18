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

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.core.Defs;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.image.MrsImage;
import org.mrgeo.image.MrsPyramid;
import org.mrgeo.junit.IntegrationTest;
import org.mrgeo.pyramid.MrsPyramidMetadata;
import org.mrgeo.pyramid.MrsPyramidMetadata.Classification;
import org.mrgeo.test.LocalRunnerTest;
import org.mrgeo.test.MapOpTestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ChangeClassificationMapOpIntegrationTest extends LocalRunnerTest
{
  @SuppressWarnings("unused")
  private static final Logger log = LoggerFactory.getLogger(ChangeClassificationMapOpIntegrationTest.class);


  private static MapOpTestUtils testUtils;

  private static String smallElevation = "small-elevation";
  private static Path smallElevationPath;
  private static String smallElevationCategorical = "small-elevation-categorical";
  private static Path smallElevationCategoricalPath;
  private static String smallElevationNoPyramids = "small-elevation-nopyramids";
  private static Path[] smallElevationNoPyramidsPath = new Path[7];

  @BeforeClass
  public static void init() throws IOException
  {
    testUtils = new MapOpTestUtils(ChangeClassificationMapOpIntegrationTest.class);

    HadoopFileUtils.copyToHdfs(Defs.INPUT, testUtils.getInputHdfs(), smallElevation, true);
    smallElevationPath = HadoopFileUtils.unqualifyPath(new Path(testUtils.getInputHdfs(), smallElevation));

    HadoopFileUtils.copyToHdfs(Defs.INPUT, testUtils.getInputHdfs(), smallElevationCategorical, true);
    smallElevationCategoricalPath = HadoopFileUtils.unqualifyPath(new Path(testUtils.getInputHdfs(), smallElevationCategorical));


    for (int i = 0; i < smallElevationNoPyramidsPath.length; i++)
    {
      Path p = new Path(testUtils.getInputHdfs(), Integer.toString(i));
      HadoopFileUtils.copyToHdfs(Defs.INPUT, p, smallElevationNoPyramids, true);
      smallElevationNoPyramidsPath[i] = 
          HadoopFileUtils.unqualifyPath(new Path(p, smallElevationNoPyramids));
    }

  }

  @Test
  @Category(IntegrationTest.class)
  public void changeToCategorical() throws Exception
  {
    String exp = String.format("changeClassification([%s], \"categorical\")", smallElevationPath);

    testUtils.runMapAlgebraExpression(conf, testname.getMethodName(), exp);

    // check the in-place pyramid
    MrsPyramid pyramid = MrsPyramid.open(smallElevationPath.toString(),
                                         getProviderProperties());
    Assert.assertNotNull("Can't load pyramid", pyramid);

    MrsPyramidMetadata metadata = pyramid.getMetadata();
    Assert.assertNotNull("Can't load metadata", metadata);

    Assert.assertEquals("Bad classification", Classification.Categorical, metadata.getClassification());
    Assert.assertEquals("Bad resampling method", "MEAN", metadata.getResamplingMethod());

    for (int level = metadata.getMaxZoomLevel(); level >= 1; level--)
    {
      MrsImage image = pyramid.getImage(level);
      Assert.assertNotNull("MrsImage image missing for level " + level, image);
      image.close();
    }
  }

@Test
@Category(IntegrationTest.class)
public void changeToContinuous() throws Exception
{
  String exp = String.format("changeClassification([%s], \"continuous\")", smallElevationCategoricalPath);

  testUtils.runMapAlgebraExpression(conf, testname.getMethodName(), exp);

  // check the in-place pyramid
  MrsPyramid pyramid = MrsPyramid.open(smallElevationCategoricalPath.toString(),
                                       getProviderProperties());
  Assert.assertNotNull("Can't load pyramid", pyramid);

  MrsPyramidMetadata metadata = pyramid.getMetadata();
  Assert.assertNotNull("Can't load metadata", metadata);

  Assert.assertEquals("Bad classification", Classification.Continuous, metadata.getClassification());

  //NOTE:  We didn't change the aggregation type, so it should still be MODE
  Assert.assertEquals("Bad resampling method", "MODE", metadata.getResamplingMethod());
  for (int level = metadata.getMaxZoomLevel(); level >= 1; level--)
  {
    MrsImage image = pyramid.getImage(level);
    Assert.assertNotNull("MrsImage image missing for level " + level, image);
    image.close();
  }
}

@Test
@Category(IntegrationTest.class)
public void changeToContinuousMean() throws Exception
{
  String exp = String.format("changeClassification([%s], \"continuous\", \"mean\")", smallElevationCategoricalPath);

  testUtils.runMapAlgebraExpression(conf, testname.getMethodName(), exp);

  // check the in-place pyramid
  MrsPyramid pyramid = MrsPyramid.open(smallElevationCategoricalPath.toString(),
                                       getProviderProperties());
  Assert.assertNotNull("Can't load pyramid", pyramid);

  MrsPyramidMetadata metadata = pyramid.getMetadata();
  Assert.assertNotNull("Can't load metadata", metadata);

  Assert.assertEquals("Bad classification", Classification.Continuous, metadata.getClassification());

  //NOTE:  We didn't change the aggregation type, so it should still be MODE
  Assert.assertEquals("Bad resampling method", "MEAN", metadata.getResamplingMethod());
  for (int level = metadata.getMaxZoomLevel(); level >= 1; level--)
  {
    MrsImage image = pyramid.getImage(level);
    Assert.assertNotNull("MrsImage image missing for level " + level, image);
    image.close();
  }
}

  @Test
  @Category(IntegrationTest.class)
  public void changeToCategoricalNoPyramids() throws Exception
  {
    String exp = String.format("changeClassification([%s], \"categorical\")", smallElevationNoPyramidsPath[0]);

    testUtils.runMapAlgebraExpression(conf, testname.getMethodName(), exp);

    // check the in-place pyramid
    MrsPyramid pyramid = MrsPyramid.open(smallElevationNoPyramidsPath[0].toString(),
                                         getProviderProperties());
    Assert.assertNotNull("Can't load pyramid", pyramid);

    MrsPyramidMetadata metadata = pyramid.getMetadata();
    Assert.assertNotNull("Can't load metadata", metadata);

    Assert.assertEquals("Bad classification", Classification.Categorical, metadata.getClassification());
    Assert.assertEquals("Bad resampling method", "MEAN", metadata.getResamplingMethod());
    for (int level = metadata.getMaxZoomLevel(); level >= 1; level--)
    {
      MrsImage image = pyramid.getImage(level);
      if (level == metadata.getMaxZoomLevel())
      {
        Assert.assertNotNull("MrsImage image missing for level " + level, image);
        image.close();
      }
      else
      {
        Assert.assertNull("MrsImage image found for level " + level, image);
      }
    }

  }

  @Test
  @Category(IntegrationTest.class)
  public void changeToCategoricalMode() throws Exception
  {
    String exp = String.format("changeClassification([%s], \"categorical\", \"mode\")", smallElevationNoPyramidsPath[1]);

    testUtils.runMapAlgebraExpression(conf, testname.getMethodName(), exp);

    // check the in-place pyramid
    MrsPyramid pyramid = MrsPyramid.open(smallElevationNoPyramidsPath[1].toString(),
                                         getProviderProperties());
    Assert.assertNotNull("Can't load pyramid", pyramid);

    MrsPyramidMetadata metadata = pyramid.getMetadata();
    Assert.assertNotNull("Can't load metadata", metadata);

    Assert.assertEquals("Bad classification", Classification.Categorical, metadata.getClassification());
    Assert.assertEquals("Bad resampling method", "MODE", metadata.getResamplingMethod());

  }
  
  @Test
  @Category(IntegrationTest.class)
  public void changeToCategoricalSum() throws Exception
  {
    String exp = String.format("changeClassification([%s], \"categorical\", \"sum\")", smallElevationNoPyramidsPath[2]);

    testUtils.runMapAlgebraExpression(conf, testname.getMethodName(), exp);

    // check the in-place pyramid
    MrsPyramid pyramid = MrsPyramid.open(smallElevationNoPyramidsPath[2].toString(),
                                         getProviderProperties());
    Assert.assertNotNull("Can't load pyramid", pyramid);

    MrsPyramidMetadata metadata = pyramid.getMetadata();
    Assert.assertNotNull("Can't load metadata", metadata);

    Assert.assertEquals("Bad classification", Classification.Categorical, metadata.getClassification());
    Assert.assertEquals("Bad resampling method", "SUM", metadata.getResamplingMethod());

  }

  @Test
  @Category(IntegrationTest.class)
  public void changeToCategoricalNearest() throws Exception
  {
    String exp = String.format("changeClassification([%s], \"categorical\", \"nearest\")", smallElevationNoPyramidsPath[3]);

    testUtils.runMapAlgebraExpression(conf, testname.getMethodName(), exp);

    // check the in-place pyramid
    MrsPyramid pyramid = MrsPyramid.open(smallElevationNoPyramidsPath[3].toString(),
                                         getProviderProperties());
    Assert.assertNotNull("Can't load pyramid", pyramid);

    MrsPyramidMetadata metadata = pyramid.getMetadata();
    Assert.assertNotNull("Can't load metadata", metadata);

    Assert.assertEquals("Bad classification", Classification.Categorical, metadata.getClassification());
    Assert.assertEquals("Bad resampling method", "NEAREST", metadata.getResamplingMethod());

  }

  @Test
  @Category(IntegrationTest.class)
  public void changeToCategoricalMin() throws Exception
  {
    String exp = String.format("changeClassification([%s], \"categorical\", \"min\")", smallElevationNoPyramidsPath[4]);

    testUtils.runMapAlgebraExpression(conf, testname.getMethodName(), exp);

    // check the in-place pyramid
    MrsPyramid pyramid = MrsPyramid.open(smallElevationNoPyramidsPath[4].toString(),
                                         getProviderProperties());
    Assert.assertNotNull("Can't load pyramid", pyramid);

    MrsPyramidMetadata metadata = pyramid.getMetadata();
    Assert.assertNotNull("Can't load metadata", metadata);

    Assert.assertEquals("Bad classification", Classification.Categorical, metadata.getClassification());
    Assert.assertEquals("Bad resampling method", "MIN", metadata.getResamplingMethod());

  }

  @Test
  @Category(IntegrationTest.class)
  public void changeToCategoricalMax() throws Exception
  {
    String exp = String.format("changeClassification([%s], \"categorical\", \"max\")", smallElevationNoPyramidsPath[5]);

    testUtils.runMapAlgebraExpression(conf, testname.getMethodName(), exp);

    // check the in-place pyramid
    MrsPyramid pyramid = MrsPyramid.open(smallElevationNoPyramidsPath[5].toString(),
                                         getProviderProperties());
    Assert.assertNotNull("Can't load pyramid", pyramid);

    MrsPyramidMetadata metadata = pyramid.getMetadata();
    Assert.assertNotNull("Can't load metadata", metadata);

    Assert.assertEquals("Bad classification", Classification.Categorical, metadata.getClassification());
    Assert.assertEquals("Bad resampling method", "MAX", metadata.getResamplingMethod());

  }

  @Test
  @Category(IntegrationTest.class)
  public void changeToCategoricalMinAvgPair() throws Exception
  {
    String exp = String.format("changeClassification([%s], \"categorical\", \"minavgpair\")", smallElevationNoPyramidsPath[6]);

    testUtils.runMapAlgebraExpression(conf, testname.getMethodName(), exp);

    // check the in-place pyramid
    MrsPyramid pyramid = MrsPyramid.open(smallElevationNoPyramidsPath[6].toString(),
                                         getProviderProperties());
    Assert.assertNotNull("Can't load pyramid", pyramid);

    MrsPyramidMetadata metadata = pyramid.getMetadata();
    Assert.assertNotNull("Can't load metadata", metadata);

    Assert.assertEquals("Bad classification", Classification.Categorical, metadata.getClassification());
    Assert.assertEquals("Bad resampling method", "MINAVGPAIR", metadata.getResamplingMethod());

  }

}