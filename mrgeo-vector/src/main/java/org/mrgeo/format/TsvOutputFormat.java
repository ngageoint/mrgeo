/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.format;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.geometry.WktConverter;
import org.mrgeo.mapreduce.GeometryWritable;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.utils.LeakChecker;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

public class TsvOutputFormat extends FileOutputFormat<LongWritable, Geometry> implements
    Serializable
{
  private static final long serialVersionUID = 1L;


  @Override
  public RecordWriter<LongWritable, Geometry> getRecordWriter(TaskAttemptContext context)
      throws IOException, InterruptedException
  {
    Path output = getDefaultWorkFile(context, ".tsv");
    FileSystem fs = HadoopFileUtils.getFileSystem(output);
    
    CsvOutputFormat.CsvRecordWriter result = new CsvOutputFormat.CsvRecordWriter(new Path(output.toString() + ".columns"), output);
    result.setDelimiter('\t');
    return result;
  }
}
