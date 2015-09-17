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

package org.mrgeo.mapalgebra;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mrgeo.format.InlineCsvInputFormat;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.hdfs.vector.Column;
import org.mrgeo.hdfs.vector.ColumnDefinitionFile;
import org.mrgeo.hdfs.vector.Column.FactorType;
import org.mrgeo.mapalgebra.old.MapOpHadoop;
import org.mrgeo.mapalgebra.old.ParserAdapterHadoop;
import org.mrgeo.mapalgebra.parser.ParserNode;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.mapreduce.job.JobCancelledException;
import org.mrgeo.mapreduce.job.JobFailedException;
import org.mrgeo.progress.Progress;
import org.mrgeo.progress.ProgressHierarchy;
import org.mrgeo.utils.HadoopUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

public class SplitVectorMapOp extends VectorMapOpHadoop
{
  public static final String SPLIT_TYPE_TEST = "test";
  public static final String SPLIT_TYPE_TRAINING = "training";

  private int splitCount;
  private int currentSplit;
  private String splitType;

  public static String[] register()
  {
    return new String[] { "SplitVector" };
  }

  @Override
  public void addInput(MapOpHadoop n) throws IllegalArgumentException
  {
    if (_inputs.size() == 0)
    {
      if (!(n instanceof VectorMapOpHadoop))
      {
        throw new IllegalArgumentException("The first parameter must be a vector input.");
      }
      _inputs.add(n);
    }
    else
    {
      throw new IllegalArgumentException("Only one input is supported.");
    }
  }

  private void determineOutputForInlineCsvInput(InlineCsvInputFormatDescriptor ifd, char delim)
      throws IOException
      {
    // Set up a reader to be able to stream features from the input source
    InlineCsvInputFormat.InlineCsvReader csvReader = new InlineCsvInputFormat.InlineCsvReader();
    csvReader.initialize(ifd._columns, ifd._values);
    ColumnDefinitionFile inputCDF = csvReader.getColumnDefinitionFile();

    FileSystem dfs = HadoopFileUtils.getFileSystem(new Path(_outputName));
    FSDataOutputStream os = dfs.create(new Path(_outputName), true);
    PrintWriter pw = new PrintWriter(new java.io.OutputStreamWriter(os));
    try
    {
      long lineNumber = 0;
      boolean isSplit = splitType.equalsIgnoreCase("test");
      while (csvReader.nextFeature())
      {
        Geometry feature = csvReader.getCurrentFeature();
        if ((lineNumber % splitCount) == (currentSplit - 1))
        {
          if (isSplit)
          {
            String strFeature = featureToString(inputCDF, feature, delim);
            pw.println(strFeature);
          }
        }
        else
        {
          if (!isSplit)
          {
            String strFeature = featureToString(inputCDF, feature, delim);
            pw.println(strFeature);
          }
        }
        lineNumber++;
      }
    }
    finally
    {
      pw.close();
      if (os != null)
      {
        os.close();
      }
    }

    // Copy the input columns to the output columns, excluding the stats
    // because we've filtered the actual data, so the stats will be wrong.
    Path outputColumnsPath = new Path(_outputName + ".columns");
    ColumnDefinitionFile outputCDF = new ColumnDefinitionFile();
    Vector<Column> columns = inputCDF.getColumns();
    Vector<Column> newColumns = new Vector<Column>();
    for (Column column : columns)
    {
      newColumns.add(new Column(column.getName(), column.getType()));
    }
    outputCDF.setColumns(newColumns);
    outputCDF.store(outputColumnsPath);
    _output = new BasicInputFormatDescriptor(_outputName);
      }

  private static String featureToString(ColumnDefinitionFile cdf, Geometry feature, char delim)
  {
    StringBuffer sb = new StringBuffer();
    Vector<Column> columns = cdf.getColumns();
    for (Column column : columns)
    {
      Object value = feature.getAttribute(column.getName());
      if (sb.length() > 0)
      {
        sb.append(delim);
      }
      if (column.getType() != FactorType.Numeric)
      {
        sb.append('\"');
      }
      sb.append(value.toString());
      if (column.getType() != FactorType.Numeric)
      {
        sb.append('\"');
      }
    }
    return sb.toString();
  }

  @Override
  public String resolveOutputName() throws IOException
  {
    if (_outputName == null)
    {
      MapOpHadoop inputMapOp = _inputs.get(0);
      String outputBase = HadoopUtils.createRandomString(40);
      Path outputParent = HadoopFileUtils.getTempDir();
      if (inputMapOp instanceof InlineCsvMapOp)
      {
        _outputName = new Path(outputParent, outputBase + ".csv").toString();
        addTempFile(_outputName);
        return _outputName;
      }
      else
      {
        // Must be a VectorMapOpHadoop
        Path inputPath = new Path(((VectorMapOpHadoop)inputMapOp).getOutputName());
        if (inputPath.toString().endsWith(".tsv"))
        {
          _outputName = new Path(outputParent, outputBase + ".tsv").toString();
        }
        else if (inputPath.toString().endsWith(".csv"))
        {
          _outputName = new Path(outputParent, outputBase + ".csv").toString();
        }
        else
        {
          throw new IOException("Unable to split input: " + inputPath.toString());
        }
        addTempFile(_outputName);
      }
    }
    return _outputName;
  }

  @Override
  public void build(Progress p) throws IOException, JobFailedException, JobCancelledException
  {
    ProgressHierarchy ph = new ProgressHierarchy(p);
    ph.createChild(1.0f);
    ph.createChild(1.0f);

    MapOpHadoop inputMapOp = _inputs.get(0);
    Path inputPath = null;
    // TODO:
    // The following code is an ugly hack until we have time to re-factor the direct
    // reading of vector data. Right now, there is generic code for doing this (see
    // AutoFeatureInputFormat), but it's tightly coupled to the map/reduce InputFormat
    // and splits. We need to re-factor it so that the core part for reading the
    // vector data is independent of InputFormat. This represents a first step in that
    // direction where InlineCsvInputFormat itself was re-factored. That's why there's
    // a special case below - because the other vector formats have not been re-factored
    // and there is no generic interface in place for reading any vector data.
    if (inputMapOp instanceof InlineCsvMapOp)
    {
      InlineCsvInputFormatDescriptor ifd = (InlineCsvInputFormatDescriptor)((VectorMapOpHadoop)inputMapOp).getVectorOutput();
      determineOutputForInlineCsvInput(ifd, ',');
      return;
    }
    else if (inputMapOp instanceof VectorMapOpHadoop)
    {
      inputPath = new Path(((VectorMapOpHadoop)inputMapOp).getOutputName());
    }
    else
    {
      // defensive code since input should be VectorMapOpHadoop - see addInput()
      throw new IllegalArgumentException("Invalid value for vector argument to SplitVector");
    }
    //    SplitVectorDriver svd = new SplitVectorDriver();
    //    svd.run(getConf(), inputPath, splitCount, currentSplit, splitType, _outputName, p, jobListener);
    FileSystem dfs = HadoopFileUtils.getFileSystem(inputPath);
    if (!dfs.exists(inputPath))
    {
      throw new IOException(String.format("Cannot read contrast measures, %s does not exist", inputPath.toString()));
    }
    FileStatus[] outputFiles = dfs.listStatus(inputPath);
    List<Path> tsvFiles = new ArrayList<Path>();
    if (dfs.isFile(inputPath))
    {
      tsvFiles.add(inputPath);
    }
    else
    {
      for (FileStatus fileStatus : outputFiles)
      {
        if (fileStatus.isDir() == false)
        {
          Path fp = fileStatus.getPath();
          String name = fp.getName();
          if (name.startsWith("part-"))
          {
            tsvFiles.add(fp);
          }
        }
      }
    }
    FSDataOutputStream os = dfs.create(new Path(_outputName), true);
    java.io.PrintWriter pw = new java.io.PrintWriter(new java.io.OutputStreamWriter(os));
    try
    {
      long lineNumber = 0;
      boolean isTestSplit = splitType.equalsIgnoreCase("test");
      for (Path tsvFile : tsvFiles)
      {
        InputStream is = HadoopFileUtils.open(tsvFile); // dfs.open(tsvFile);
        java.io.BufferedReader r = new java.io.BufferedReader(new java.io.InputStreamReader(is));
        try
        {
          String line;
          while ((line = r.readLine()) != null)
          {
            if ((lineNumber % splitCount) == (currentSplit - 1))
            {
              if (isTestSplit)
              {
                pw.println(line);
              }
            }
            else
            {
              if (!isTestSplit)
              {
                pw.println(line);
              }
            }
            lineNumber++;
          }
        }
        finally
        {
          r.close();
          if (is != null)
          {
            is.close();
          }
        }
      }
    }
    finally
    {
      pw.close();
      if (os != null)
      {
        os.close();
      }
    }

    // Copy the input columns to the output columns, excluding the stats
    // because we've filtered the actual data, so the stats will be wrong.
    Path inputColumnsPath = new Path(inputPath.toString() + ".columns");
    Path outputColumnsPath = new Path(_outputName + ".columns");
    ColumnDefinitionFile inputCDF = new ColumnDefinitionFile(inputColumnsPath);
    ColumnDefinitionFile outputCDF = new ColumnDefinitionFile();
    Vector<Column> columns = inputCDF.getColumns();
    Vector<Column> newColumns = new Vector<Column>();
    for (Column column : columns)
    {
      newColumns.add(new Column(column.getName(), column.getType()));
    }
    outputCDF.setColumns(newColumns);
    outputCDF.store(outputColumnsPath);
    _output = new BasicInputFormatDescriptor(_outputName);
  }

  @Override
  public void moveOutput(String toName) throws IOException
  {
    super.moveOutput(toName);
    _outputName = toName;
    _output = new BasicInputFormatDescriptor(_outputName);
  }

  @Override
  public Vector<ParserNode> processChildren(final Vector<ParserNode> children, final ParserAdapterHadoop parser)
  {
    Vector<ParserNode> result = new Vector<ParserNode>();
    if (children.size() != 4)
    {
      throw new IllegalArgumentException(
          "SplitVector usage: SplitVector(<vector source>, splitCount, currentSplit, splitType");
    }
    // Validate splitCount
    splitCount = parseChildInt(children.get(1), "splitCount", parser);
    if (splitCount <= 1)
    {
      throw new IllegalArgumentException("splitCount must be > 1");
    }
    // Validate currentSplit
    currentSplit = parseChildInt(children.get(2), "currentSplit", parser);
    if (currentSplit < 1 || currentSplit > splitCount)
    {
      throw new IllegalArgumentException("currentSplit must be >= 1 and <= splitCount");
    }
    // Validate splitType, and convert from mixed case for the back-end
    splitType = parseChildString(children.get(3), "splitType", parser);
    if (splitType.equalsIgnoreCase(SPLIT_TYPE_TEST))
    {
      splitType = SPLIT_TYPE_TEST;
    }
    else if (splitType.equalsIgnoreCase(SPLIT_TYPE_TRAINING))
    {
      splitType = SPLIT_TYPE_TRAINING;
    }
    else
    {
      throw new IllegalArgumentException("splitType must be either \"test\" or \"training\"");
    }
    result.add(children.get(0));
    return result;
  }

  @Override
  public String toString()
  {
    return String.format("SplitVectorMapOp %s",
      _outputName == null ? "null" : _outputName );
  }

}
