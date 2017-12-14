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

import org.apache.hadoop.mapreduce.InputSplit;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class MbVectorTilesInputFormatTest
{
  private MbVectorTilesInputFormat getInputFormatForWaterDefaultZoom()
  {
    String[] layers = { "water" };
    MbVectorTilesSettings dbSettings = new MbVectorTilesSettings(
            "/home/dave.johnson/Downloads/2017-07-03_new-zealand_wellington.mbtiles",
            layers
    );
    MbVectorTilesInputFormat inputFormat = new MbVectorTilesInputFormat(dbSettings);
    return inputFormat;
  }

  private MbVectorTilesInputFormat getInputFormatForWaterZoom14(int tilesPerPartition)
  {
    String[] layers = { "water" };
    MbVectorTilesSettings dbSettings = new MbVectorTilesSettings(
            "/home/dave.johnson/Downloads/2017-07-03_new-zealand_wellington.mbtiles",
            layers,
            14,
            tilesPerPartition,
            null

    );
    MbVectorTilesInputFormat inputFormat = new MbVectorTilesInputFormat(dbSettings);
    return inputFormat;
  }

  @Test
  public void getRecordCountZoom0() throws Exception
  {
    MbVectorTilesInputFormat inputFormat = getInputFormatForWaterDefaultZoom();
    Assert.assertEquals(1L, inputFormat.getRecordCount(0));
  }

  @Test
  public void getRecordCountZoom2() throws Exception
  {
    MbVectorTilesInputFormat inputFormat = getInputFormatForWaterDefaultZoom();
    Assert.assertEquals(2L, inputFormat.getRecordCount(6));
  }

  @Test
  public void getRecordCountZoom14() throws Exception
  {
    MbVectorTilesInputFormat inputFormat = getInputFormatForWaterDefaultZoom();
    Assert.assertEquals(4720L, inputFormat.getRecordCount(14));
  }

  @Test
  public void getRecordCountZoom15() throws Exception
  {
    MbVectorTilesInputFormat inputFormat = getInputFormatForWaterDefaultZoom();
    Assert.assertEquals(0L, inputFormat.getRecordCount(15));
  }

  @Test
  public void getSplitsZoom14_1() throws Exception
  {
    MbVectorTilesInputFormat inputFormat = getInputFormatForWaterZoom14(100);
    List<InputSplit> splits = inputFormat.getSplits(null);
    Assert.assertEquals(48, splits.size());
  }

  @Test
  public void getSplitsZoom14_2() throws Exception
  {
    MbVectorTilesInputFormat inputFormat = getInputFormatForWaterZoom14(1000);
    List<InputSplit> splits = inputFormat.getSplits(null);
    Assert.assertEquals(5, splits.size());
  }
}