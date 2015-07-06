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

package org.mrgeo.data.vector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.geometry.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VectorInputFormat extends InputFormat<LongWritable, Geometry>
{
  static final Logger log = LoggerFactory.getLogger(VectorInputFormat.class);

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException
  {
    boolean debugEnabled = log.isDebugEnabled();
    VectorInputFormatContext ifContext = VectorInputFormatContext.load(context.getConfiguration());
    List<InputSplit> results = new ArrayList<InputSplit>();
    if (debugEnabled)
    {
      log.debug("Number of inputs to get splits for: " + ifContext.getInputs().size());
    }
    for (String input: ifContext.getInputs())
    {
      if (debugEnabled)
      {
        log.debug("Getting splits for input: " + input);
      }
      List<InputSplit> nativeSplits = getNativeSplits(context, ifContext, input);
      if (nativeSplits != null && !nativeSplits.isEmpty())
      {
        for (InputSplit nativeSplit: nativeSplits)
        {
          VectorInputSplit newSplit = new VectorInputSplit(input, nativeSplit);
          results.add(newSplit);
        }
      }
    }
    if (debugEnabled)
    {
      log.debug("VectorInputFormat.getSplits returns: " + results.size());
    }
    return results;
  }

  @Override
  public RecordReader<LongWritable, Geometry> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException
  {
//    return new VectorRecordReader();
    if (!(split instanceof VectorInputSplit))
    {
      throw new IOException("Expected a VectorInputSplit but got " + split.getClass().getName());
    }
    VectorInputSplit inputSplit = (VectorInputSplit)split;
    VectorDataProvider dp = DataProviderFactory.getVectorDataProvider(context.getConfiguration(),
        inputSplit.getVectorName(), AccessMode.READ);
    RecordReader<LongWritable, Geometry> recordReader = dp.getRecordReader();
    recordReader.initialize(inputSplit, context);
    return recordReader;
  }
  
  private List<InputSplit> getNativeSplits(JobContext context,
      VectorInputFormatContext ifContext,
      String input) throws IOException, InterruptedException
  {
    VectorDataProvider dp = DataProviderFactory.getVectorDataProvider(context.getConfiguration(),
        input, AccessMode.READ);
    VectorInputFormatProvider ifProvider = dp.getVectorInputFormatProvider(ifContext);
    List<InputSplit> results = ifProvider.getInputFormat(input).getSplits(context);
    if (log.isDebugEnabled())
    {
      log.debug("vector input format provider class is " + ifProvider.getClass().getName());
      log.debug("VectorInputFormat.getNativeSplits returns: " + results.size());
    }
    return results;
  }
}
