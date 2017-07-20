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

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.mrgeo.data.vector.FeatureIdWritable;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.utils.LeakChecker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads tab separated values as geometries.
 * <p>
 * http://code.flickr.com/blog/2009/05/21/flickr-shapefiles-public-dataset-10/
 */
public class CsvOutputFormat extends FileOutputFormat<FeatureIdWritable, Geometry> implements
    Serializable
{
static final Logger log = LoggerFactory.getLogger(CsvOutputFormat.class);

private static final long serialVersionUID = 1L;

public static void setup(Path outputPath, Job job)
{
  FileOutputFormat.setOutputPath(job, outputPath);
}

@Override
public RecordWriter<FeatureIdWritable, Geometry> getRecordWriter(TaskAttemptContext context)
    throws IOException, InterruptedException
{
  String baseOut = getOutputPath(context).toString();
  String extension = baseOut.substring(baseOut.length() - 4);
  Path output = getDefaultWorkFile(context, extension);

  CsvRecordWriter result = new CsvRecordWriter(new Path(baseOut + ".columns"), output);
  return result;
}

static public class CsvRecordWriter extends RecordWriter<FeatureIdWritable, Geometry>
{
  final private boolean profile;
  // we initialize to avoid a bunch of if (reader != null) code. It will be
  // recreated in the initialize function.
  //LineRecordReader reader = new LineRecordReader();
  //Vector<String> columns = new Vector<String>();
  PrintWriter writer;
  boolean first = true;
  boolean writeHeader;
  boolean writeGeometry = true;
  private OutputStream externalColumnsOutput;
  private OutputStream output;
  private Path columnsOutputPath;
  private char encapsulator = '\"';
  private char delimiter = ',';
  private Path _outputPath;
  private List<String> attributes;

  public CsvRecordWriter(Path columnsOutput, Path output) throws IOException
  {
    if (System.getProperty("mrgeo.profile", "false").compareToIgnoreCase("true") == 0)
    {
      LeakChecker.instance().add(this, ExceptionUtils
          .getStackTrace(new Throwable("KMLGeometryOutputFormat creation stack(ignore the Throwable...)")));
      profile = true;
    }
    else
    {
      profile = false;
    }


    FileSystem fs = HadoopFileUtils.getFileSystem(columnsOutput);

    log.info("columnsOutput path: " + columnsOutput.toString() + ", this: " + this);
    if (output.toString().endsWith(".tsv"))
    {
      delimiter = '\t';
    }

    _outputPath = output;

    this.output = fs.create(output);
    init(columnsOutput, this.output);
  }

  // This constructor is used when only one CsvRecordWriter is being used to
  public CsvRecordWriter(OutputStream columnsOutput, OutputStream output)
  {
    if (System.getProperty("mrgeo.profile", "false").compareToIgnoreCase("true") == 0)
    {
      profile = true;
    }
    else
    {
      profile = false;
    }

    externalColumnsOutput = columnsOutput;
    init(null, output);
  }

  /**
   * returns the output path if it is known.
   *
   * @return
   */
  public Path getOutputPath()
  {
    return _outputPath;
  }

  @SuppressWarnings("hiding")
  public void init(Path columnsOutputPath, OutputStream os)
  {
    if (columnsOutputPath != null)
    {
      log.info("In init, setting columnsOutputPath = " + columnsOutputPath + ", this: " + this);
      this.columnsOutputPath = columnsOutputPath;
    }
    else
    {
      writeHeader = true;
    }
    writer = new PrintWriter(os);
  }

  @Override
  public void close(TaskAttemptContext context) throws IOException
  {
    writer.flush();
    writer.close();

    if (output != null)
    {
      output.close();
      output = null;
    }

    if (profile)
    {
      LeakChecker.instance().remove(this);
    }
  }

  public void setDelimiter(char d)
  {
    delimiter = d;
  }

  @Override
  public void write(FeatureIdWritable key, Geometry value) throws IOException
  {
    if (first)
    {
      first = false;
      writeColumns(value);
    }

    boolean useDelim = false;

    if (writeGeometry)
    {
      if (value.isValid() && !value.isEmpty())
      {
        writeCell(value.toJTS().toString());
      }
      useDelim = true;
    }

    for (String attribute : attributes)
    {
      if (useDelim)
      {
        writer.append(delimiter);
      }
      useDelim = true;
      Object cell = value.getAttribute(attribute);

      if (cell != null)
      {
        writeCell(cell.toString());
      }
    }
    writer.append("\n");
  }

  private void writeColumns(Geometry sample) throws IOException
  {

    attributes = new ArrayList<>(sample.getAllAttributesSorted().keySet());


    boolean hasX = false;
    boolean hasY = false;
    for (String key : attributes)
    {
      if (key.equals("GEOMETRY"))
      {
        writeGeometry = false;
        break;
      }
      if (key.compareToIgnoreCase("x") == 0)
      {
        hasX = true;
      }
      if (key.compareToIgnoreCase("y") == 0)
      {
        hasY = true;
      }
    }

    writeGeometry = !(hasX && hasY);

    String delim = "";
    if (writeHeader)
    {
      if (writeGeometry)
      {
        writer.print("GEOMETRY");
        delim = "" + delimiter;
      }
      for (String attribute : attributes)
      {
        writer.print(delim);
        delim = "" + delimiter;

        writeCell(attribute);
      }

      writer.println("");
    }

    log.info("Made it to output column def file, columns Output path: " + columnsOutputPath
        + ", this: " + this);
    OutputStream columnsOutput = null;
    boolean closeColumnsOutputStream = false;
    try
    {
      if (externalColumnsOutput != null)
      {
        columnsOutput = externalColumnsOutput;
      }
      else
      {
        if (columnsOutputPath != null)
        {
          // Since there can be multiple reducers, and only one .columns file
          // is written (not one for each reducer), then we skip writing the file if it
          // already exists.
          FileSystem fs = HadoopFileUtils.getFileSystem(columnsOutputPath);
          if (!fs.exists(columnsOutputPath))
          {
            columnsOutput = fs.create(columnsOutputPath);
            closeColumnsOutputStream = true;
          }
        }
      }
      if (columnsOutput != null)
      {
        ColumnDefinitionFile cdf = new ColumnDefinitionFile();
        if (writeGeometry)
        {
          List<String> modified = new ArrayList();
          modified.add("GEOMETRY");
          modified.addAll(attributes);

          cdf.setColumns(modified);
        }
        else
        {
          cdf.setColumns(attributes);
        }
        cdf.setFirstLineHeader(writeHeader);
        cdf.store(columnsOutput);
        log.info("Done writing column definition file");
      }
      else
      {
        log.info("No need to write column definition, skipping. this = " + this);
      }
    }
    finally
    {
      if (closeColumnsOutputStream)
      {
        log.info("Closing columnsOutput. this = " + this);

        if (columnsOutput != null)
        {
          columnsOutput.close();
        }
      }
    }


  }

  private void writeCell(String v)
  {
    if (v.contains(Character.toString(delimiter)))
    {
      writer.print(encapsulator + v + encapsulator);
    }
    else
    {
      writer.print(v);
    }
  }
}
}
