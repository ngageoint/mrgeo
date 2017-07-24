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

package org.mrgeo.hdfs.vector;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.mrgeo.data.vector.FeatureIdWritable;
import org.mrgeo.geometry.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

public class DelimitedVectorInputFormat extends InputFormat<FeatureIdWritable, Geometry> implements Serializable
{
private static final long serialVersionUID = 1L;
private static final Logger log = LoggerFactory.getLogger(DelimitedVectorInputFormat.class);
private static final String className = DelimitedVectorInputFormat.class.getSimpleName();
private static final String USE_NLINE_FORMAT = className + ".useNLineFormat";

public static void setupJob(Job job, int minFeaturesPerSplit, long featureCount)
{
  if (minFeaturesPerSplit > 0)
  {
    if (featureCount < 0)
    {
      throw new IllegalArgumentException("Expected a feature count");
    }
    int maxMapTasks = job.getConfiguration().getInt("mapred.tasktracker.map.tasks.maximum", -1);
    if (maxMapTasks > 0)
    {
      int featuresPerSplit = (int) (featureCount / maxMapTasks);
      if (featuresPerSplit < minFeaturesPerSplit)
      {
        featuresPerSplit = minFeaturesPerSplit;
      }
      job.getConfiguration().setBoolean(USE_NLINE_FORMAT, true);
      NLineInputFormat.setNumLinesPerSplit(job, featuresPerSplit);
    }
  }
}

@Override
public RecordReader<FeatureIdWritable, Geometry> createRecordReader(InputSplit split,
    TaskAttemptContext context) throws IOException, InterruptedException
{
  return new DelimitedVectorRecordReader();
}

@Override
public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException
{
  boolean useNLineFormat = context.getConfiguration().getBoolean(USE_NLINE_FORMAT, false);
  if (useNLineFormat)
  {
    List<InputSplit> splits = new NLineInputFormat().getSplits(context);
    // This is a workaround to what appears to be a bug in in how NLineInputFormat
    // computes its splits. When there are multiple splits in a file, it seems
    // the start position in the last split is off by one. Note that this corrective
    // code needs to check the last split for each different file that appears
    // in the list of splits.
    for (int index = 2; index < splits.size(); index++)
    {
      FileSplit previousSplit = (FileSplit) splits.get(index - 1);
      FileSplit currSplit = (FileSplit) splits.get(index);
      // If this index is the last split, or we've moved on to splits from a different
      // file, then we need to adjust the last split for that file.
      int lastFileIndex = -1;
      if (index == splits.size() - 1)
      {
        lastFileIndex = index;
      }
      else if (!currSplit.getPath().equals(previousSplit.getPath()))
      {
        lastFileIndex = index - 1;
      }
      if (lastFileIndex >= 2)
      {
        FileSplit lastFileSplit = (FileSplit) splits.get(lastFileIndex);
        FileSplit priorSplit = (FileSplit) splits.get(lastFileIndex - 1);
        if (lastFileSplit.getPath().equals(priorSplit.getPath()))
        {
          if (priorSplit.getPath().equals(lastFileSplit.getPath()) &&
              priorSplit.getStart() + priorSplit.getLength() < lastFileSplit.getStart())
          {
            // Adjust the start of previous split
            FileSplit replacement = new FileSplit(lastFileSplit.getPath(),
                priorSplit.getStart() + priorSplit.getLength(),
                lastFileSplit.getLength() + 1,
                lastFileSplit.getLocations());
            log.info("Replacing split: " + lastFileSplit);
            log.info("  With split: " + replacement);
            splits.set(lastFileIndex, replacement);
          }
        }
      }
    }
    return splits;
  }
  else
  {
    return new TextInputFormat().getSplits(context);
  }
}
}
