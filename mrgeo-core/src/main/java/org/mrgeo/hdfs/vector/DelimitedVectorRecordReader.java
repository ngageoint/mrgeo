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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.mrgeo.data.vector.FeatureIdWritable;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.hdfs.vector.Column.FactorType;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class DelimitedVectorRecordReader extends RecordReader<FeatureIdWritable, Geometry>
{
private DelimitedParser delimitedParser;
private LineRecordReader recordReader;

public DelimitedVectorRecordReader()
{
}

public static DelimitedParser getDelimitedParser(String input, Configuration conf) throws IOException
{
  char delimiter = ',';
  if (input.toLowerCase().endsWith(".tsv"))
  {
    delimiter = '\t';
  }
  Path columnsPath = new Path(input + ".columns");
  List<String> attributeNames = new ArrayList<>();
  int xCol = -1;
  int yCol = -1;
  int geometryCol = -1;
  boolean skipFirstLine = false;
  FileSystem fs = HadoopFileUtils.getFileSystem(conf, columnsPath);
  if (!fs.exists(columnsPath))
  {
    // This is to cover the case where delimited text was the output of
    // a map/reduce operation. In that case "output.csv" is a directory
    // containing multiple part*.csv files. And the .columns file is
    // stored as output.tsv.columns at the parent level.
    columnsPath = new Path(columnsPath.getParent() + ".columns");
  }
  if (fs.exists(columnsPath))
  {
    InputStream in = null;
    try
    {
      in = HadoopFileUtils.open(conf, columnsPath); // fs.open(columnPath);
      ColumnDefinitionFile cdf = new ColumnDefinitionFile(in);
      skipFirstLine = cdf.isFirstLineHeader();

      int i = 0;
      for (Column col : cdf.getColumns())
      {
        String c = col.getName();

        if (col.getType() == FactorType.Numeric)
        {
          if (c.equals("x"))
          {
            xCol = i;
          }
          else if (c.equals("y"))
          {
            yCol = i;
          }
        }
        else
        {
          if (c.toLowerCase().equals("geometry"))
          {
            geometryCol = i;
          }
        }

        attributeNames.add(c);
        i++;
      }
    }
    finally
    {
      if (in != null)
      {
        in.close();
      }
    }
  }
  else
  {
    throw new IOException("Column file was not found: " + columnsPath);
  }
  DelimitedParser delimitedParser = new DelimitedParser(attributeNames,
      xCol, yCol, geometryCol, delimiter, '\"', skipFirstLine);
  return delimitedParser;
}

@Override
@SuppressWarnings("squid:S2095") // recordReader is closed explictly in the close() method
public void initialize(InputSplit split, TaskAttemptContext context) throws IOException,
    InterruptedException
{
  if (split instanceof FileSplit)
  {
    FileSplit fsplit = (FileSplit) split;
    delimitedParser = getDelimitedParser(fsplit.getPath().toString(),
        context.getConfiguration());
    recordReader = new LineRecordReader();
    recordReader.initialize(fsplit, context);
    // Skip the first
    if (delimitedParser.getSkipFirstLine())
    {
      // Only skip the first line of the first split. The other
      // splits are somewhere in the middle of the original file,
      // so their first lines should not be skipped.
      if (fsplit.getStart() != 0)
      {
        nextKeyValue();
      }
    }
  }
  else
  {
    throw new IOException("input split is not a FileSplit");
  }
}

@Override
public boolean nextKeyValue() throws IOException, InterruptedException
{
  return recordReader.nextKeyValue();
}

@Override
public FeatureIdWritable getCurrentKey() throws IOException, InterruptedException
{
  FeatureIdWritable key = new FeatureIdWritable(recordReader.getCurrentKey().get());
  return key;
}

@Override
public Geometry getCurrentValue() throws IOException, InterruptedException
{
  Text rawValue = recordReader.getCurrentValue();
  if (rawValue == null)
  {
    return null;
  }
  return delimitedParser.parse(rawValue.toString());
}

@Override
public float getProgress() throws IOException, InterruptedException
{
  return recordReader.getProgress();
}

@Override
public void close() throws IOException
{
  if (recordReader != null)
  {
    recordReader.close();
  }
}

public static class VectorLineProducer implements LineProducer
{
  LineRecordReader lineRecordReader;

  public VectorLineProducer(LineRecordReader recordReader)
  {
    lineRecordReader = recordReader;
  }

  @Override
  public void close() throws IOException
  {
  }

  @Override
  public String nextLine() throws IOException
  {
    if (lineRecordReader.nextKeyValue())
    {
      return lineRecordReader.getCurrentValue().toString();
    }
    return null;
  }
}
}
