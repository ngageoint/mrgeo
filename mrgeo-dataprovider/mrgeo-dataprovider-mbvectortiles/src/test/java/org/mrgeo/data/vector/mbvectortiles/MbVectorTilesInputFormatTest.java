/*
 * Copyright 2009-2017. DigitalGlobe, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.mrgeo.data.vector.mbvectortiles;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.vector.FeatureIdWritable;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.test.TestUtils;
import org.mrgeo.utils.HadoopUtils;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class MbVectorTilesInputFormatTest
{
  private static double POINT_EPSILON = 1e-7;
  private static String input;
  private static Path hdfsInput;
  private Configuration conf;
  private TaskAttemptContext context;
  private ProviderProperties providerProperties;

  @BeforeClass
  public static void init() throws IOException
  {
    String inputDir = TestUtils.composeInputDir(MbVectorTilesInputFormatTest.class);
    input = "file://" + inputDir;
    hdfsInput = TestUtils.composeInputHdfs(MbVectorTilesInputFormatTest.class, true);
  }

  @Before
  public void setup() throws IOException
  {
    MrGeoProperties.resetProperties();
    Job j = Job.getInstance(new Configuration());
    conf = j.getConfiguration();
    context = HadoopUtils.createTaskAttemptContext(conf, new TaskAttemptID());
    providerProperties = null;
  }

  @Test
  public void getSplitsZoom0() throws Exception
  {
    Path dbPath = new Path(input, "AmbulatoryPt.mbtiles");
    MbVectorTilesSettings dbSettings = new MbVectorTilesSettings(dbPath.toString(), new String[] { "ambulatory"}, 0, 1, null);
    MbVectorTilesInputFormat ifmt = new MbVectorTilesInputFormat(dbSettings);
    List<InputSplit> splits = ifmt.getSplits(context);
    Assert.assertNotNull(splits);
    Assert.assertEquals(1, splits.size());
    Assert.assertEquals(1L, ifmt.getRecordCount(conf));
  }

  @Test
  public void getSplitsZoom11_1() throws Exception
  {
    Path dbPath = new Path(input, "AmbulatoryPt.mbtiles");
    MbVectorTilesSettings dbSettings = new MbVectorTilesSettings(dbPath.toString(), new String[] { "ambulatory"}, 11, 1, null);
    MbVectorTilesInputFormat ifmt = new MbVectorTilesInputFormat(dbSettings);
    List<InputSplit> splits = ifmt.getSplits(context);
    Assert.assertNotNull(splits);
    Assert.assertEquals(3, splits.size());
    Assert.assertEquals(3L, ifmt.getRecordCount(conf));
    int count = 0;
    for (InputSplit split: splits) {
      RecordReader<FeatureIdWritable, Geometry> reader = ifmt.createRecordReader(split, context);
      Assert.assertNotNull(reader);
      while (reader.nextKeyValue()) count++;
    }
    System.out.println("count: " + count);
  }

  @Test
  public void getSplitsZoom11_2() throws Exception
  {
    Path dbPath = new Path(input, "AmbulatoryPt.mbtiles");
    Path hdfsPath = new Path(hdfsInput, "AmbulatoryPt.mbtiles");
    try {
      // Also test that we can load from the vector.base
      MrGeoProperties.getInstance().setProperty(MrGeoConstants.MRGEO_HDFS_VECTOR, hdfsInput.toString());
      HadoopFileUtils.copyFileToHdfs(dbPath.toString(), hdfsPath.toString());
      MbVectorTilesDataProvider dp = new MbVectorTilesDataProvider(conf, "mbvt", "filename=AmbulatoryPt.mbtiles;zoom=11;partition_size=2", providerProperties);
      MbVectorTilesSettings dbSettings = dp.parseResourceName();
      MbVectorTilesInputFormat ifmt = new MbVectorTilesInputFormat(dbSettings);
      List<InputSplit> splits = ifmt.getSplits(context);
      Assert.assertNotNull(splits);
      Assert.assertEquals(2, splits.size());
      Assert.assertEquals(3L, ifmt.getRecordCount(conf));
    }
    finally {
      HadoopFileUtils.delete(hdfsPath);
    }
  }

  @Test
  public void getSplitsZoom11_3() throws Exception
  {
    Path dbPath = new Path(input, "AmbulatoryPt.mbtiles");
    MbVectorTilesSettings dbSettings = new MbVectorTilesSettings(dbPath.toString(), new String[] { "ambulatory"}, 11, 3, null);
    MbVectorTilesInputFormat ifmt = new MbVectorTilesInputFormat(dbSettings);
    List<InputSplit> splits = ifmt.getSplits(context);
    Assert.assertNotNull(splits);
    Assert.assertEquals(1, splits.size());
    Assert.assertEquals(3L, ifmt.getRecordCount(conf));
  }

  @Test
  public void getSplitsZoom14_1() throws Exception
  {
    Path dbPath = new Path(input, "AmbulatoryPt.mbtiles");
    MbVectorTilesSettings dbSettings = new MbVectorTilesSettings(dbPath.toString(), new String[] { "ambulatory"}, 14, 1, null);
    MbVectorTilesInputFormat ifmt = new MbVectorTilesInputFormat(dbSettings);
    List<InputSplit> splits = ifmt.getSplits(context);
    Assert.assertNotNull(splits);
    Assert.assertEquals(4, splits.size());
    Assert.assertEquals(4L, ifmt.getRecordCount(conf));
    int count = 0;
    for (InputSplit split: splits) {
      RecordReader<FeatureIdWritable, Geometry> reader = ifmt.createRecordReader(split, context);
      Assert.assertNotNull(reader);
      while (reader.nextKeyValue()) count++;
    }
    Assert.assertEquals(8, count);
  }

  @Test
  public void getSplitsZoom14_3() throws Exception
  {
    Path dbPath = new Path(input, "AmbulatoryPt.mbtiles");
    MbVectorTilesSettings dbSettings = new MbVectorTilesSettings(dbPath.toString(), new String[] { "ambulatory"}, 14, 3, null);
    MbVectorTilesInputFormat ifmt = new MbVectorTilesInputFormat(dbSettings);
    List<InputSplit> splits = ifmt.getSplits(context);
    Assert.assertNotNull(splits);
    Assert.assertEquals(2, splits.size());
    Assert.assertEquals(4L, ifmt.getRecordCount(conf));
    int count = 0;
    for (InputSplit split: splits) {
      RecordReader<FeatureIdWritable, Geometry> reader = ifmt.createRecordReader(split, context);
      Assert.assertNotNull(reader);
      while (reader.nextKeyValue()) count++;
    }
    Assert.assertEquals(8, count);
  }
}