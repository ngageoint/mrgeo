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
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.buildpyramid.BuildPyramid;
import org.mrgeo.core.Defs;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.image.MrsImage;
import org.mrgeo.image.MrsPyramid;
import org.mrgeo.junit.IntegrationTest;
import org.mrgeo.image.MrsPyramidMetadata;
import org.mrgeo.image.MrsPyramidMetadata.Classification;
import org.mrgeo.test.LocalRunnerTest;
import org.mrgeo.test.MapOpTestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class BuildPyramidMapOpIntegrationTest extends LocalRunnerTest
{
  @SuppressWarnings("unused")
  private static final Logger log = LoggerFactory.getLogger(BuildPyramidMapOpIntegrationTest.class);


  private static MapOpTestUtils testUtils;

  private static String smallElevation = "small-elevation";
  private static Path smallElevationPath;

  private static String smallElevationNoPyramids = "small-elevation-nopyramids";
  private static Path smallElevationNoPyramidsPath;

  private ProviderProperties providerProperties;

  @BeforeClass
  public static void init() throws IOException
  {
    testUtils = new MapOpTestUtils(BuildPyramidMapOpIntegrationTest.class);
  }

  @Before
  public void setup() throws IOException
  {
    providerProperties = null;
    Path parent = new Path(testUtils.getInputHdfs(), testname.getMethodName());
    HadoopFileUtils.copyToHdfs(Defs.INPUT, parent, smallElevationNoPyramids, true);
    smallElevationNoPyramidsPath = 
        HadoopFileUtils.unqualifyPath(new Path(parent, smallElevationNoPyramids));

    HadoopFileUtils.copyToHdfs(Defs.INPUT, testUtils.getInputHdfs(), smallElevation, true);
    smallElevationPath = HadoopFileUtils.unqualifyPath(new Path(testUtils.getInputHdfs(), smallElevation));
  }


  @Test
  @Category(IntegrationTest.class)
  public void buildPyramidExistingPyramids() throws Exception
  {
    String exp = String.format("BuildPyramid([%s])", smallElevationPath);

    testUtils.runMapAlgebraExpression(conf, testname.getMethodName(), exp);

    // check the in-place pyramid
    MrsPyramid pyramid = MrsPyramid.open(smallElevationPath.toString(), providerProperties);
    Assert.assertNotNull("Can't load pyramid", pyramid);

    MrsPyramidMetadata metadata = pyramid.getMetadata();
    Assert.assertNotNull("Can't load metadata", metadata);

    Assert.assertEquals("Bad classification", Classification.Continuous, metadata.getClassification());
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
  public void buildPyramidDefaultAggregator() throws Exception
  {
    String exp = String.format("BuildPyramid([%s])", smallElevationNoPyramidsPath);

    testUtils.runMapAlgebraExpression(conf, testname.getMethodName(), exp);

    // check the in-place pyramid
    MrsPyramid pyramid = MrsPyramid.open(smallElevationNoPyramidsPath.toString(),
                                         providerProperties);
    Assert.assertNotNull("Can't load pyramid", pyramid);

    MrsPyramidMetadata metadata = pyramid.getMetadata();
    Assert.assertNotNull("Can't load metadata", metadata);

    Assert.assertEquals("Wrong number of levels", 10, metadata.getMaxZoomLevel());

    Assert.assertEquals("Bad classification", Classification.Continuous, metadata.getClassification());
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
  public void buildPyramidMeanAggregator() throws Exception
  {
    BuildPyramid.setMIN_TILES_FOR_SPARK(5);

    String exp = String.format("BuildPyramid([%s], \"mean\")", smallElevationNoPyramidsPath);

    testUtils.runMapAlgebraExpression(conf, testname.getMethodName(), exp);

    // check the in-place pyramid
    MrsPyramid pyramid = MrsPyramid.open(smallElevationNoPyramidsPath.toString(),
                                         providerProperties);
    Assert.assertNotNull("Can't load pyramid", pyramid);

    MrsPyramidMetadata metadata = pyramid.getMetadata();
    Assert.assertNotNull("Can't load metadata", metadata);

    Assert.assertEquals("Wrong number of levels", 10, metadata.getMaxZoomLevel());

    Assert.assertEquals("Bad classification", Classification.Continuous, metadata.getClassification());
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
  public void buildPyramidSumAggregator() throws Exception
  {
    String exp = String.format("BuildPyramid([%s], \"sum\")", smallElevationNoPyramidsPath);

    testUtils.runMapAlgebraExpression(conf, testname.getMethodName(), exp);

    // check the in-place pyramid
    MrsPyramid pyramid = MrsPyramid.open(smallElevationNoPyramidsPath.toString(),
                                         providerProperties);
    Assert.assertNotNull("Can't load pyramid", pyramid);

    MrsPyramidMetadata metadata = pyramid.getMetadata();
    Assert.assertNotNull("Can't load metadata", metadata);

    Assert.assertEquals("Wrong number of levels", 10, metadata.getMaxZoomLevel());

    Assert.assertEquals("Bad classification", Classification.Continuous, metadata.getClassification());
    Assert.assertEquals("Bad resampling method", "SUM", metadata.getResamplingMethod());

    for (int level = metadata.getMaxZoomLevel(); level >= 1; level--)
    {
      MrsImage image = pyramid.getImage(level);
      Assert.assertNotNull("MrsImage image missing for level " + level, image);
      image.close();
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void buildPyramidModeAggregator() throws Exception
  {
    String exp = String.format("BuildPyramid([%s], \"mode\")", smallElevationNoPyramidsPath);

    testUtils.runMapAlgebraExpression(conf, testname.getMethodName(), exp);

    // check the in-place pyramid
    MrsPyramid pyramid = MrsPyramid.open(smallElevationNoPyramidsPath.toString(),
                                         providerProperties);
    Assert.assertNotNull("Can't load pyramid", pyramid);

    MrsPyramidMetadata metadata = pyramid.getMetadata();
    Assert.assertNotNull("Can't load metadata", metadata);

    Assert.assertEquals("Wrong number of levels", 10, metadata.getMaxZoomLevel());

    Assert.assertEquals("Bad classification", Classification.Continuous, metadata.getClassification());
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
  public void buildPyramidNearestAggregator() throws Exception
  {
    String exp = String.format("BuildPyramid([%s], \"nearest\")", smallElevationNoPyramidsPath);

    testUtils.runMapAlgebraExpression(conf, testname.getMethodName(), exp);

    // check the in-place pyramid
    MrsPyramid pyramid = MrsPyramid.open(smallElevationNoPyramidsPath.toString(),
                                         providerProperties);
    Assert.assertNotNull("Can't load pyramid", pyramid);

    MrsPyramidMetadata metadata = pyramid.getMetadata();
    Assert.assertNotNull("Can't load metadata", metadata);

    Assert.assertEquals("Wrong number of levels", 10, metadata.getMaxZoomLevel());

    Assert.assertEquals("Bad classification", Classification.Continuous, metadata.getClassification());
    Assert.assertEquals("Bad resampling method", "NEAREST", metadata.getResamplingMethod());

    for (int level = metadata.getMaxZoomLevel(); level >= 1; level--)
    {
      MrsImage image = pyramid.getImage(level);
      Assert.assertNotNull("MrsImage image missing for level " + level, image);
      image.close();
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void buildPyramidMinAggregator() throws Exception
  {
    String exp = String.format("BuildPyramid([%s], \"min\")", smallElevationNoPyramidsPath);

    testUtils.runMapAlgebraExpression(conf, testname.getMethodName(), exp);

    // check the in-place pyramid
    MrsPyramid pyramid = MrsPyramid.open(smallElevationNoPyramidsPath.toString(),
                                         providerProperties);
    Assert.assertNotNull("Can't load pyramid", pyramid);

    MrsPyramidMetadata metadata = pyramid.getMetadata();
    Assert.assertNotNull("Can't load metadata", metadata);

    Assert.assertEquals("Wrong number of levels", 10, metadata.getMaxZoomLevel());

    Assert.assertEquals("Bad classification", Classification.Continuous, metadata.getClassification());
    Assert.assertEquals("Bad resampling method", "MIN", metadata.getResamplingMethod());

    for (int level = metadata.getMaxZoomLevel(); level >= 1; level--)
    {
      MrsImage image = pyramid.getImage(level);
      Assert.assertNotNull("MrsImage image missing for level " + level, image);
      image.close();
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void buildPyramidMaxAggregator() throws Exception
  {
    String exp = String.format("BuildPyramid([%s], \"max\")", smallElevationNoPyramidsPath);

    testUtils.runMapAlgebraExpression(conf, testname.getMethodName(), exp);

    // check the in-place pyramid
    MrsPyramid pyramid = MrsPyramid.open(smallElevationNoPyramidsPath.toString(),
                                         providerProperties);
    Assert.assertNotNull("Can't load pyramid", pyramid);

    MrsPyramidMetadata metadata = pyramid.getMetadata();
    Assert.assertNotNull("Can't load metadata", metadata);

    Assert.assertEquals("Wrong number of levels", 10, metadata.getMaxZoomLevel());

    Assert.assertEquals("Bad classification", Classification.Continuous, metadata.getClassification());
    Assert.assertEquals("Bad resampling method", "MAX", metadata.getResamplingMethod());

    for (int level = metadata.getMaxZoomLevel(); level >= 1; level--)
    {
      MrsImage image = pyramid.getImage(level);
      Assert.assertNotNull("MrsImage image missing for level " + level, image);
      image.close();
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void buildPyramidMinAvgPairAggregator() throws Exception
  {
    String exp = String.format("BuildPyramid([%s], \"minAvgPair\")", smallElevationNoPyramidsPath);

    testUtils.runMapAlgebraExpression(conf, testname.getMethodName(), exp);

    // check the in-place pyramid
    MrsPyramid pyramid = MrsPyramid.open(smallElevationNoPyramidsPath.toString(),
                                         providerProperties);
    Assert.assertNotNull("Can't load pyramid", pyramid);

    MrsPyramidMetadata metadata = pyramid.getMetadata();
    Assert.assertNotNull("Can't load metadata", metadata);

    Assert.assertEquals("Wrong number of levels", 10, metadata.getMaxZoomLevel());

    Assert.assertEquals("Bad classification", Classification.Continuous, metadata.getClassification());
    Assert.assertEquals("Bad resampling method", "MINAVGPAIR", metadata.getResamplingMethod());

    for (int level = metadata.getMaxZoomLevel(); level >= 1; level--)
    {
      MrsImage image = pyramid.getImage(level);
      Assert.assertNotNull("MrsImage image missing for level " + level, image);
      image.close();
    }
  }
}
