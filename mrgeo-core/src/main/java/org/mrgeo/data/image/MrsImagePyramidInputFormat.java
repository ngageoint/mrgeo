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

package org.mrgeo.data.image;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.MrsPyramidInputFormat;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.data.tile.TiledInputFormatContext;
import org.mrgeo.mapreduce.formats.TileCollection;
import org.mrgeo.mapreduce.splitters.TiledInputSplit;

import java.awt.image.Raster;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The InputFormat class used by MrGeo for map/reduce jobs that operate on
 * image pyramids. This class is always configured as the InputFormat class
 * in MrGeo map/reduce jobs that take input from image pyramids. However,
 * when returning the input splits to use for the job, it uses an instance
 * of the MrsImageDataProvider for the specified input and gets the actual
 * native set of splits from that. It is required that the native splits
 * returned from the data provider are instances of TiledInputSplit so that
 * MrGeo can map/reduce over tiles properly.
 */
public class MrsImagePyramidInputFormat extends MrsPyramidInputFormat<Raster>
{
  @Override
  public RecordReader<TileIdWritable, TileCollection<Raster>> createRecordReader(InputSplit inputSplit,
      TaskAttemptContext context) throws IOException, InterruptedException
  {
    return new MrsImagePyramidRecordReader();
  }

  /**
   * Return native splits from the data provider for the passed in input.
   * It ensures that the native splits returned from the data provider are
   * instances of TiledInputSplit.
   */
  @Override
  protected List<TiledInputSplit> getNativeSplits(final JobContext context,
      final TiledInputFormatContext ifContext,
      final String input) throws IOException, InterruptedException
  {
    MrsImageDataProvider dp = DataProviderFactory.getMrsImageDataProvider(input,
        AccessMode.READ, context.getConfiguration());
    MrsImageInputFormatProvider ifProvider = dp.getTiledInputFormatProvider(ifContext);
    List<InputSplit> splits = ifProvider.getInputFormat(input).getSplits(context);
    // In order to work with MrGeo and input bounds cropping, the splits must be
    // of type TiledInputSplit.
    List<TiledInputSplit> result = new ArrayList<TiledInputSplit>(splits.size());
    for (InputSplit split : splits)
    {
      if (split instanceof TiledInputSplit)
      {
        result.add((TiledInputSplit)split);
      }
      else
      {
        throw new IOException("ERROR: native input splits must be instances of" +
            "TiledInputSplit. Received " + split.getClass().getCanonicalName());
      }
    }
    return result;
  }
}
