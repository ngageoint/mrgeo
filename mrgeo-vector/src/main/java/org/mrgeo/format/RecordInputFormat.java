package org.mrgeo.format;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

import java.io.IOException;
import java.util.List;

public interface RecordInputFormat
{
  List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException;
  
  void setRecordsPerSplit(long numRecords);
  
  long getRecordCount(JobContext context) throws IOException;
}
