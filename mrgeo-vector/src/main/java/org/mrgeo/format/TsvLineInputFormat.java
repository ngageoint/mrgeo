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

package org.mrgeo.format;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;

/**
 * Reads tab separated values as geometries and creates record line based splits.
 */
public class TsvLineInputFormat extends TsvInputFormat implements RecordInputFormat
{
  private static final Logger llog = LoggerFactory.getLogger(TsvLineInputFormat.class);

  private static final long serialVersionUID = 1L;

  private long recordsPerSplit = 100;
  public long getRecordsPerSplit() { return recordsPerSplit; }
  @Override
  public void setRecordsPerSplit(long numRecords) { recordsPerSplit = numRecords; }
  
  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException
  {
    List<InputSplit> splits = new LinkedList<InputSplit>();
    Path[] paths = FileInputFormat.getInputPaths(context);
    Configuration conf = context.getConfiguration();
    
    long recordCtr = 0;
    for (Path path : paths)
    {
      FileSystem fs = path.getFileSystem(context.getConfiguration());
      FileStatus[] status = fs.globStatus(path);
      for (FileStatus s : status)
      {
        Path fileName = s.getPath();
        LineReader lr = null;
        InputStream in = null;
        try
        {
          in = HadoopFileUtils.open(conf, fileName); // fs.open(fileName);
          lr = new LineReader(in, conf);
          Text line = new Text();
          long begin = 0;
          long length = 0;
          int num = -1;
          while ((num = lr.readLine(line)) > 0)
          {
            recordCtr++;
            length += num;
            if (recordCtr == recordsPerSplit)
            {
              splits.add(new FileSplit(fileName, begin, length, new String[]{}));
              begin += length;
              length = 0;
              recordCtr = 0;
            }
          }
          //file size smaller than min split size or the last chunk of records was smaller than
          //the split size
          if (length != 0)
          {
            splits.add(new FileSplit(fileName, begin, length, new String[]{}));
          }
        }
        finally
        {
          if (lr != null)
          {
            lr.close();
          }
          if (in != null)
          {
            in.close();
          }
        }
      }
    }
    
    return splits;
  }
  
  @Override
  public long getRecordCount(JobContext context) throws IOException
  {
    Path[] paths = FileInputFormat.getInputPaths(context);
    Configuration conf = context.getConfiguration();
   
    //get the total number of records in all files
    long recordCount = 0;
    for (Path p : paths)
    {
      FileSystem fs = p.getFileSystem(context.getConfiguration());
      FileStatus[] status = fs.globStatus(p);
      for (FileStatus s : status)
      {
        Path fileName = s.getPath();
        if (s.isDir())
        {
          throw new IOException("Not a file: " + fileName);
        }
        LineReader lr = null;
        InputStream in = null;
        try
        {
          in = HadoopFileUtils.open(conf, fileName); // fs.open(fileName);
          lr = new LineReader(in, conf);
          Text line = new Text();
          while ((lr.readLine(line)) > 0)
          {
            recordCount++;
          }
        }
        finally
        {
          if (lr != null)
          {
            lr.close();
          }
          if (in != null)
          {
            in.close();
          }
        }
      }
    }
    llog.debug("recordCount = " + String.valueOf(recordCount));
    return recordCount;
  }
}
