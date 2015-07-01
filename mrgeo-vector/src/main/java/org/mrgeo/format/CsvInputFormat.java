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

import com.vividsolutions.jts.io.WKTReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.geometry.GeometryFactory;
import org.mrgeo.geometry.WritableGeometry;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.hdfs.vector.Column;
import org.mrgeo.hdfs.vector.ColumnDefinitionFile;
import org.mrgeo.hdfs.vector.WktGeometryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Reads tab separated values as geometries.
 */
public class CsvInputFormat extends InputFormat<LongWritable, Geometry> implements Serializable
{
  static final Logger log = LoggerFactory.getLogger(CsvInputFormat.class);

  public static String COLUMNS_PATH = CsvInputFormat.class.getName() + ".columns.path";

  private static final long serialVersionUID = 1L;
  protected char _delimiter = ',';


  @SuppressWarnings("unused")
  public static void setInput( Configuration conf, Path input)
  {
  }

  @Override
  public RecordReader<LongWritable, Geometry> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException
  {
    CsvRecordReader fr = new CsvRecordReader(_delimiter);
    fr.initialize(split, context);
    return fr;
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException
  {
    List<InputSplit> result = new TextInputFormat().getSplits(context);
    return result;
  }

  static public class CsvRecordReader extends RecordReader<LongWritable, Geometry>
  {
    private LongWritable key = new LongWritable(-1);
    private WritableGeometry feature;
    private String _line;
    private char encapsulator = '\"';
    private int _xCol = -1, _yCol = -1, _geometryCol = -1;
    private WKTReader _wktReader = null;
    private char _delimiter = ',';
    private List<String> attributeNames;


    //FileSplit split;
    // we initialize to avoid a bunch of if (reader != null) code. It will be recreated in the
    // initialize function.
    LineRecordReader reader = new LineRecordReader();

    public CsvRecordReader()
    {
      super();
    }

    public CsvRecordReader(char delimiter)
    {
      super();

      _delimiter = delimiter;
    }

    @Override
    public void close() throws IOException
    {
      reader.close();
    }

    @Override
    public float getProgress() throws IOException
    {
      return reader.getProgress();
    }

    @Override
    public void initialize(InputSplit isplit, TaskAttemptContext context) throws IOException
    {
      reader.initialize(isplit, context);
      FileSplit fileSplit = (FileSplit) isplit;

      Path columnPath = new Path(fileSplit.getPath().toString() + ".columns");

      Configuration conf = context.getConfiguration();
      if (conf.get(COLUMNS_PATH) != null)
      {
        columnPath = new Path(conf.get(COLUMNS_PATH));
      }

//      Path[] inputs = FileInputFormat.getInputPaths(context);
//      if (inputs[0].toString().endsWith(".tsv"))
//      {
//        _delimiter = '\t';
//      }

      attributeNames = new ArrayList<String>();

      FileSystem fs = HadoopFileUtils.getFileSystem(conf, fileSplit.getPath());
      if (fs.exists(columnPath) == false)
      {
        columnPath = new Path(columnPath.getParent().toString() + ".columns");
      }
      if (fs.exists(columnPath))
      {
        InputStream in = null;
        try
        {
          in = HadoopFileUtils.open(conf, columnPath); // fs.open(columnPath);
          ColumnDefinitionFile cdf = new ColumnDefinitionFile(in);

          if (cdf.isFirstLineHeader() && fileSplit.getStart() == 0)
          {
            log.info("Skipping header.");
            reader.nextKeyValue();
          }

          boolean hasX = false;
          boolean hasY = false;
          boolean hasGeometry = false;

          int i = 0;
          for (Column col : cdf.getColumns())
          {
            String c = col.getName();

            if (col.getType() == Column.FactorType.Numeric)
            {
              if (c.equals("x"))
              {
                hasX = true;
                _xCol = i;
              }
              else if (c.equals("y"))
              {
                hasY = true;
                _yCol = i;
              }
            }
            else
            {
              if (c.toLowerCase().equals("geometry"))
              {
                hasGeometry = true;
                _geometryCol = i;
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
        throw new IOException("Column file was not found.");
      }
    }

    @Override
    public boolean nextKeyValue() throws IOException
    {
      if (_wktReader == null)
      {
        _wktReader = new WKTReader();
      }


      feature = null;

      if (reader.nextKeyValue())
      {
        Double x = null, y = null;
        String wktGeometry = null;
        Map<String, String> attrs = new HashMap<>();

        _line = reader.getCurrentValue().toString();
        // skip any empty lines as though they don't exist.
        while (_line.isEmpty() && reader.nextKeyValue())
        {
          _line = reader.getCurrentValue().toString();
        }

        if (!_line.isEmpty())
        {
          String[] values = split(_line, _delimiter, encapsulator);
          if (values.length == 0)
          {
            log.info("Values empty. Weird.");
          }

          if (_geometryCol < 0 && _xCol < 0 && _yCol < 0)
          {
            for (int i = 0; i < values.length; i++)
            {
              if (WktGeometryUtils.isValidWktGeometry(values[i]))
              {
                attributeNames = new ArrayList<>(values.length);
                for (int j = 0; j < values.length; j++)
                {
                  if (j == i)
                  {
                    _geometryCol = i;
                  }

                  attributeNames.add(Integer.toString(i));
                }
                break;
              }
            }
          }

          for (int i = 0; i < values.length; i++)
          {
            if (i == _geometryCol)
            {
              wktGeometry = values[i];
            }
            else if (i == _xCol)// && values[i] != null && values[i].length() > 0)
            {
              try
              {
                if (values[i].trim().length() > 0)
                {
                  x = Double.parseDouble(values[i]);
                }
                else
                {
                  x = null;
                }
              } catch (NumberFormatException e)
              {
                log.error("Invalid numeric value for x: " + values[i] + ". Continuing with null x value.");
                x = null;
              }
            }
            else if (i == _yCol)// && values[i] != null && values[i].length() > 0)
            {
              try
              {
                if (values[i].trim().length() > 0)
                {
                  y = Double.parseDouble(values[i]);
                }
                else
                {
                  y = null;
                }
              } catch (NumberFormatException e)
              {
                log.error("Invalid numeric value for y: " + values[i] + ". Continuing with null y value.");
                y = null;
              }
            }
            if (i < attributeNames.size())
            {
              attrs.put(attributeNames.get(i), values[i]);
            }
          }
        }

        if (wktGeometry != null)
        {
          try
          {
            feature = GeometryFactory.fromJTS(_wktReader.read(wktGeometry), attrs);
          }
          catch (Exception e)
          {
            //try to correct wktGeometry if possible
            try
            {
              feature = GeometryFactory.fromJTS(_wktReader.read(WktGeometryUtils.wktGeometryFixer(wktGeometry)));
            }
            catch (Exception e2)
            {
              //could not fix the geometry, so just set to null
              log.error("Could not fix geometry: " + wktGeometry + ". Continuing with null geometry.");
            }
          }
        }
        else if (_geometryCol == -1 && _xCol >= 0 && _yCol >= 0)
        {
          if (x != null && y != null)
          {
            feature = GeometryFactory.createPoint(x, y, attrs);
          }
        }

        if (feature == null)
        {
          feature = GeometryFactory.createEmptyGeometry(attrs);
        }

        return true;
      }

      return false;
    }

    static String[] split(String line, char delimiter, char encapsulator)
    {
      ArrayList<String> result = new ArrayList<String>();

      StringBuffer buf = new StringBuffer();

      for (int i = 0; i < line.length(); i++)
      {
        char c = line.charAt(i);
        if (c == delimiter)
        {
          result.add(buf.toString());
          buf.delete(0, buf.length());
        }
        else if (c == encapsulator)
        {
          // skip the first encapsulator
          i++;
          // clear out the buffer
          buf.delete(0, buf.length());
          // add data until we hit another encapsulator
          while (i < line.length() && line.charAt(i) != encapsulator)
          {
            c = line.charAt(i++);
            buf.append(c);
          }

          // add the encapsulated string
          result.add(buf.toString());
          // clear out the buffer
          buf.delete(0, buf.length());
          // skip the last encapsulator
          i++;

          if (i >= line.length())
          {
//            log.error("Missing token end character (" + encapsulator +
//                ") in line: " + line);

            // need to return here, or we will add a blank field on the end of the result
            return result.toArray(new String[result.size()]);
          }

          // find the next delimiter. There may be white space or something between.
          while (i < line.length() && line.charAt(i) != delimiter)
          {
            i++;
          }
        }
        else
        {
          buf.append(c);
        }
      }

      result.add(buf.toString());

      return result.toArray(new String[result.size()]);
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException
    {
      return key;
    }

    @Override
    public Geometry getCurrentValue() throws IOException, InterruptedException
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
