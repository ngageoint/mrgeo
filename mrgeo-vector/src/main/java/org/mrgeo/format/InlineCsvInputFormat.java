/*
 * Copyright 2009-2014 DigitalGlobe, Inc.
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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.hdfs.vector.Column;
import org.mrgeo.hdfs.vector.Column.FactorType;
import org.mrgeo.hdfs.vector.ColumnDefinitionFile;
import org.mrgeo.hdfs.vector.DelimitedParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.io.WKTReader;

/**
 * This class is designed for handling small inputs of tabular data that don't
 * merit an external file. While there is no artificial limit you should think 
 * a max of several kilobytes not megabytes as everything is stored in memory.
 */
public class InlineCsvInputFormat extends InputFormat<LongWritable, Geometry> implements
    Serializable
{
  static final Logger log = LoggerFactory.getLogger(InlineCsvInputFormat.class);

  public static String COLUMNS = InlineCsvInputFormat.class.getName() + ".columns";
  public static String VALUES = InlineCsvInputFormat.class.getName() + ".values";

  private static final long serialVersionUID = 1L;

  public static String getColumns(Configuration conf)
  {
    return conf.get(COLUMNS);
  }

  public static String getValues(Configuration conf)
  {
    return conf.get(VALUES);
  }

  public static void setColumns(Configuration conf, String columns)
  {
    conf.set(COLUMNS, columns);
  }

  public static void setValues(Configuration conf, String values)
  {
    conf.set(VALUES, values);
  }

  @Override
  public RecordReader<LongWritable, Geometry> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException
  {
    InlineCsvRecordReader fr = new InlineCsvRecordReader();
    fr.initialize(split, context);
    return fr;
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException
  {
    List<InputSplit> result = new LinkedList<InputSplit>();
    // a single placeholder dummy split file.
    result.add(new FileSplit(new Path("/"), 0, 0, new String[0]));
    return result;
  }

  static public class InlineCsvRecordReader extends RecordReader<LongWritable, Geometry>
  {
    private LongWritable key = new LongWritable(-1);
    private InlineCsvReader csvReader = new InlineCsvReader();
    FileSplit split;

    public InlineCsvRecordReader()
    {
    }

    @Override
    public void close() throws IOException
    {
    }

    @Override
    public float getProgress() throws IOException
    {
      return csvReader.getProgress();
    }

    @Override
    public void initialize(InputSplit splt, TaskAttemptContext context) throws IOException
    {
      String columns = getColumns(context.getConfiguration());
      String values = getValues(context.getConfiguration());
      csvReader.initialize(columns, values);
    }

    @Override
    public boolean nextKeyValue() throws IOException
    {
      return csvReader.nextFeature();
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException
    {
      return key;
    }

    @Override
    public Geometry getCurrentValue() throws IOException, InterruptedException
    {
      return csvReader.getCurrentFeature();
    }

    @Override
    public String toString()
    {
      return csvReader.toString();
    }
  }

  // TODO:
  // The functionality in this class used to be included in the InlineCsvRecordReader. Separating
  // the actual reading functionality into this class represents a first-step toward re-factoring
  // how we input all vector data. The idea is to be able to read all vector data generically
  // both inside of map/reduce (with InputFormat objects) as well as outside of map/reduce through
  // POJOs. This class can be used from POJOs to read vector data from an InlineCsv source. However
  // we will need to consider creating a new interface called something like VectorReader which
  // provides the methods nextFeature and getCurrentFeature.
  static public class InlineCsvReader
  {
    private Geometry feature;
    private ColumnDefinitionFile cdf;
    //private FeatureSchemaStats schema;
    private String _line;
    private char encapsulator = '\'';
    private char _delimiter = ',';
    private char _lineSeparator = ';';
    private String[] _lines;
    private int _lineIndex = 0;
    private DelimitedParser delimitedParser;

    // we initialize to avoid a bunch of if (reader != null) code. It will be recreated in the 
    // initialize function.
    LineRecordReader reader = new LineRecordReader();

    public InlineCsvReader()
    {
    }

    public float getProgress()
    {
      return (float) _lineIndex / (float) _lines.length;
    }

    public ColumnDefinitionFile getColumnDefinitionFile()
    {
      return cdf;
    }

    static ColumnDefinitionFile parseColumns(String columns, char delim)
    {
      ColumnDefinitionFile cdf = new ColumnDefinitionFile();
      LinkedList<Column> columnList = new LinkedList<Column>();
      String[] columnArray = columns.split(Character.toString(delim));
      for (String cs : columnArray)
      {
        Column c = new Column(cs, FactorType.Nominal);
        columnList.add(c);
      }

      cdf.setColumns(columnList);

      return cdf;
    }

    public void initialize(String columns, String values)
    {
      List<String> attributes = new ArrayList<String>();
      //schema = new FeatureSchemaStats();
      _lines = values.split(Character.toString(_lineSeparator));

      cdf = parseColumns(columns, _delimiter);

//      boolean hasX = false;
//      boolean hasY = false;

      int i = 0;
      int xCol = -1;
      int yCol = -1;
      int geometryCol = -1;
      for (Column col : cdf.getColumns())
      {
        String c = col.getName();

        if (col.getType() == Column.FactorType.Numeric)
        {
          if (c.equals("x"))
          {
//            hasX = true;
            xCol = i;
          }
          else if (c.equals("y"))
          {
//            hasY = true;
            yCol = i;
          }
//          schema.addAttribute(c, AttributeType.DOUBLE);
//          schema.setAttributeMin(i, col.getMin());
//          schema.setAttributeMax(i, col.getMax());
        }
        else
        {
          if (c.toLowerCase().equals("geometry"))
          {
            geometryCol = i;
//            schema.addAttribute(c, AttributeType.GEOMETRY);
          }
//          else
//          {
//            schema.addAttribute(c, AttributeType.STRING);
//          }
        }
        attributes.add(c);
//        schema.setAttributeCount(i, col.getCount());
        i++;
      }

//      if (hasX && hasY)
//      {
//        schema.addAttribute("GEOMETRY", AttributeType.GEOMETRY);
//      }

      //feature = new BasicFeature(schema);
      delimitedParser = new DelimitedParser(attributes, xCol, yCol, geometryCol,
          _delimiter, encapsulator, cdf.isFirstLineHeader());
    }

    public boolean nextFeature() throws IOException
    {
      feature = null;

      boolean result = false;
      if (_lineIndex < _lines.length)
      {
        _line = _lines[_lineIndex++];
        feature = delimitedParser.parse(_line);
        result = true;
      }
      return result;
    }

    public Geometry getCurrentFeature()
    {
      return feature;
    }

    @Override
    public String toString()
    {
      return String.format("%d Current line: %s", reader.getCurrentKey().get(), _line);
    }
  }
}
