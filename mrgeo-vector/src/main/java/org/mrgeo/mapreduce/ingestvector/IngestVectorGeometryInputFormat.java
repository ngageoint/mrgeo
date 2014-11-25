package org.mrgeo.mapreduce.ingestvector;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.mrgeo.mapreduce.GeometryWritable;

import java.io.IOException;

public class IngestVectorGeometryInputFormat extends FileInputFormat<LongWritable, GeometryWritable>
{

  public IngestVectorGeometryInputFormat()
  {
  }

  @Override
  public RecordReader<LongWritable, GeometryWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException
  {
    final RecordReader<LongWritable, GeometryWritable> reader = new IngestVectorRecordReader();
    
    reader.initialize(split, context);

    return reader;
  }

  @Override
  protected boolean isSplitable(final JobContext context, final Path filename)
  {
    return false;
  }

}
