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

package org.mrgeo.data.csv;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.mapreduce.GeometryWritable;
import org.mrgeo.utils.LeakChecker;

import java.io.IOException;
import java.io.PrintStream;

/**
 * It is assumed that all CSV files are in WGS84.
 * 
 * @author jason.surratt
 * 
 */
@Deprecated
public class CsvOutputFormat extends FileOutputFormat<LongWritable, GeometryWritable>
{
  
  public static class CsvRecordWriter extends RecordWriter<LongWritable, GeometryWritable>
  {
    PrintStream out;

    FSDataOutputStream stream = null;
    final boolean profile;
    protected String delimiter = ",";

    CsvRecordWriter(Path output) throws IOException
    {
      if (System.getProperty("mrgeo.profile", "false").compareToIgnoreCase("true") == 0)
      {
        LeakChecker.instance().add(this, ExceptionUtils.getStackTrace(new Throwable("KMLGeometryOutputFormat creation stack(ignore the Throwable...)")));
        profile = true;
      }
      else
      {
        profile = false;
      }

      FileSystem fs = HadoopFileUtils.getFileSystem(output);
      
      stream = fs.create(output);
      out = new PrintStream(stream);
    }

    @Override
    public void close(TaskAttemptContext arg0) throws IOException, InterruptedException
    {
      out.close();
      
      if (stream != null)
      {
        stream.close();
        stream = null;
        
        if (profile)
        {
          LeakChecker.instance().remove(this);
        }

      }
    }

    @Override
    public void write(LongWritable key, GeometryWritable value) throws IOException,
        InterruptedException
    {
      StringBuffer line = new StringBuffer();
      String sep = "";
      for (String attr : value.getGeometry().getAllAttributesSorted().values())
      {
        line.append(sep + attr);
        sep = delimiter;
      }
      out.println(line);
    }
  }

  @Override
  public RecordWriter<LongWritable, GeometryWritable> getRecordWriter(TaskAttemptContext context)
      throws IOException, InterruptedException
  {
    Path output = getDefaultWorkFile(context, ".csv");
    return new CsvRecordWriter(output);
  }
}
