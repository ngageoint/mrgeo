/*
 * Copyright 2009-2016 DigitalGlobe, Inc.
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
 *
 */

package org.mrgeo.hdfs.vector;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mrgeo.data.CloseableKVIterator;
import org.mrgeo.data.vector.FeatureIdWritable;
import org.mrgeo.data.vector.VectorReader;
import org.mrgeo.data.vector.VectorReaderContext;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.utils.tms.Bounds;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

@SuppressFBWarnings(value = "URF_UNREAD_FIELD", justification = "'context' kept for completeness")
public class DelimitedVectorReader implements VectorReader
{
private HdfsVectorDataProvider provider;
private VectorReaderContext context;
private Configuration conf;

public static class HdfsFileReader implements LineProducer
{
  private BufferedReader reader;
  private Path sourcePath;

  public void initialize(Configuration conf, Path p) throws IOException
  {
    this.sourcePath = p;
    InputStream is = HadoopFileUtils.open(conf, p);
    reader = new BufferedReader(new InputStreamReader(is));
  }

  @Override
  public void close() throws IOException
  {
    if (reader != null)
    {
      reader.close();
    }
  }

  @Override
  public String nextLine() throws IOException
  {
    return reader.readLine();
  }

  public String toString()
  {
    return (sourcePath != null) ? sourcePath.toString() : "unknown" ;
  }
}

public static class FeatureIdRangeVisitor implements DelimitedReader.DelimitedReaderVisitor
{
  private long minFeatureId = -1;
  private long maxFeatureId = -1;

  /**
   * Constructs a FeatureIdRangeVisitor which sets lower and upper bounds
   * for acceptable features. The min/max feature id values passed in are
   * inclusive. If either of the values are <= zero,
   * it will not be checked. In other words, if the minFeatureId is <= 0, then
   * there will be no minimum boundary on acceptable feature ids. Likewise,
   * if maxFeatureId is <= 0, then there will be no upper boundary on
   * acceptable feature ids.
   *
   * If both min and max feature id are <= 0, then this visitor will accept
   * all features.
   **/
  public FeatureIdRangeVisitor(long minFeatureId, long maxFeatureId)
  {
    this.minFeatureId = minFeatureId;
    this.maxFeatureId = maxFeatureId;
  }

  @Override
  public boolean accept(long id, Geometry geometry)
  {
    boolean acceptable = true;
    if (minFeatureId > 0)
    {
      acceptable = (id >= minFeatureId);
    }
    if (acceptable && maxFeatureId > 0)
    {
      acceptable = (id <= maxFeatureId);
    }
    return acceptable;
  }

  @Override
  public boolean stopReading(long id, Geometry geometry)
  {
    return (maxFeatureId > 0 && id > maxFeatureId);
  }
}

public static class BoundsVisitor implements DelimitedReader.DelimitedReaderVisitor
{
  private Bounds bounds;

  public BoundsVisitor(Bounds bounds)
  {
    this.bounds = bounds;
  }

  @Override
  public boolean accept(long id, Geometry geometry)
  {
    Bounds geomBounds = geometry.getBounds();
    return geomBounds != null && geomBounds.intersects(bounds);
  }

  @Override
  public boolean stopReading(long id, Geometry geometry)
  {
    // We must read all records since there is no ordering by geometry
    // that would allow us to stop reading early.
    return false;
  }
}

public DelimitedVectorReader(HdfsVectorDataProvider dp,
    VectorReaderContext context,
    Configuration conf)
{
  this.provider = dp;
  this.context = context;
  this.conf = conf;
}

@Override
public void close()
{
}

private DelimitedParser getDelimitedParser() throws IOException
{
  char delimiter = ',';
  if (provider.getResourceName().toLowerCase().endsWith(".tsv"))
  {
    delimiter = '\t';
  }
  String fileName = provider.getResolvedResourceName(true);
  Path columnsPath = new Path(fileName + ".columns");
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
    columnsPath = new Path(columnsPath.getParent().toString() + ".columns");
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

        if (col.getType() == Column.FactorType.Numeric)
        {
          if (c.equalsIgnoreCase("x"))
          {
            xCol = i;
          }
          else if (c.equalsIgnoreCase("y"))
          {
            yCol = i;
          }
        }
        else
        {
          if (c.equalsIgnoreCase("geometry"))
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
    skipFirstLine = true;
    // Read the column names from the first line of the file
    try (InputStream in = HadoopFileUtils.open(conf, new Path(fileName))) // fs.open(columnPath);
    {
      try (InputStreamReader isr = new InputStreamReader(in))
      {
        try (BufferedReader reader = new BufferedReader(isr))
        {
          String line = reader.readLine();
          if (line != null)
          {
            String[] columnNames = line.split(Character.toString(delimiter));
            int i = 0;
            for (String colName : columnNames)
            {
              if (colName.equalsIgnoreCase("x"))
              {
                xCol = i;
              }
              else if (colName.equalsIgnoreCase("y"))
              {
                yCol = i;
              }
              else if (colName.equalsIgnoreCase("geometry"))
              {
                geometryCol = i;
              }
              attributeNames.add(colName);
              i++;
            }
          }
        }
      }
    }
  }
  return new DelimitedParser(attributeNames,
      xCol, yCol, geometryCol, delimiter, '\"', skipFirstLine);
}

@Override
public CloseableKVIterator<FeatureIdWritable, Geometry> get() throws IOException
{
  HdfsFileReader fileReader = new HdfsFileReader();
  fileReader.initialize(conf, new Path(provider.getResolvedResourceName(true)));
  DelimitedParser delimitedParser = getDelimitedParser();
  return new DelimitedReader(fileReader, delimitedParser);
}

@Override
public boolean exists(FeatureIdWritable featureId) throws IOException
{
  Geometry geometry = get(featureId);
  return (geometry != null);
}

@Override
public Geometry get(FeatureIdWritable featureId) throws IOException
{
  HdfsFileReader fileReader = new HdfsFileReader();
  fileReader.initialize(conf, new Path(provider.getResolvedResourceName(true)));
  DelimitedParser delimitedParser = getDelimitedParser();
  FeatureIdRangeVisitor visitor = new FeatureIdRangeVisitor(featureId.get(), featureId.get());
  DelimitedReader reader = new DelimitedReader(fileReader, delimitedParser, visitor);
  if (reader.hasNext())
  {
    return reader.next();
  }
  return null;
}

@Override
public CloseableKVIterator<FeatureIdWritable, Geometry> get(Bounds bounds) throws IOException
{
  HdfsFileReader fileReader = new HdfsFileReader();
  fileReader.initialize(conf, new Path(provider.getResolvedResourceName(true)));
  DelimitedParser delimitedParser = getDelimitedParser();
  BoundsVisitor visitor = new BoundsVisitor(bounds);
  return new DelimitedReader(fileReader, delimitedParser, visitor);
}

@Override
public long count() throws IOException
{
  long featureCount = 0L;
  try (CloseableKVIterator<FeatureIdWritable, Geometry> iter = get())
  {
    while (iter.hasNext())
    {
      if (iter.next() != null)
      {
        featureCount++;
      }
    }
  }
  return featureCount;
}
}
