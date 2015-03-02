package org.mrgeo.hdfs.vector;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.mrgeo.geometry.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsVectorInputFormat extends InputFormat<LongWritable, Geometry> implements Serializable
{
  private static final long serialVersionUID = 1L;
  private static final Logger log = LoggerFactory.getLogger(HdfsVectorInputFormat.class);
  private static final String className = HdfsVectorInputFormat.class.getSimpleName();
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
        int featuresPerSplit = (int)(featureCount / maxMapTasks);
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
  public RecordReader<LongWritable, Geometry> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException
  {
    HdfsVectorRecordReader recordReader = new HdfsVectorRecordReader();
    recordReader.initialize(split, context);
    return recordReader;
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
        FileSplit previousSplit = (FileSplit)splits.get(index-1);
        FileSplit currSplit = (FileSplit)splits.get(index);
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
          FileSplit lastFileSplit = (FileSplit)splits.get(lastFileIndex);
          FileSplit priorSplit = (FileSplit)splits.get(lastFileIndex-1);
          if (lastFileSplit.getPath().equals(priorSplit.getPath()))
          if (priorSplit.getPath().equals(lastFileSplit.getPath()) &&
              priorSplit.getStart() + priorSplit.getLength() < lastFileSplit.getStart())
          {
            // Adjust the start of previous split
            FileSplit replacement = new FileSplit(lastFileSplit.getPath(),
                priorSplit.getStart() + priorSplit.getLength(),
                lastFileSplit.getLength() + 1,
                lastFileSplit.getLocations());
            log.info("Replacing split: " + lastFileSplit.toString());
            log.info("  With split: " + replacement.toString());
            splits.set(lastFileIndex, replacement);
          }
        }
      }
      return splits;
    }
    else
    {
      List<InputSplit> splits = new TextInputFormat().getSplits(context);
      return splits;
    }
  }
}
