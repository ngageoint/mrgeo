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

package org.mrgeo.mapalgebra.vector;

import com.vividsolutions.jts.io.WKTReader;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.*;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.geometry.GeometryFactory;
import org.mrgeo.geometry.WritableGeometry;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.hdfs.vector.WktGeometryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.sql.*;
import java.util.*;

/**
 * Reads tab separated values as geometries.
 */
public class PgQueryInputFormat extends InputFormat<LongWritable, Geometry> implements Serializable
{
public final static String RESULT_COLLECTION = "PgQueryInputFormat.ResultCollection";
private static final Logger log = LoggerFactory.getLogger(PgQueryInputFormat.class);
private static final long serialVersionUID = 1L;
private static final String prefix = PgQueryInputFormat.class.getSimpleName();
public static final String USERNAME = prefix + ".username";
public static final String PASSWORD = prefix + ".password";
public static final String DBCONNECTION = prefix + ".dbconnection";

@SuppressFBWarnings(value = {"SQL_INJECTION_JDBC", "SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING",
    "OBL_UNSATISFIED_OBLIGATION_EXCEPTION_EDGE"}, justification =
    "1 & 2. This is how PGQuery is intended to work.  It is an open-ended query on a database we have no idea what it is," +
        "3. rs is closed outside this method. ")
public static ResultSet loadResultSet(Configuration conf) throws IOException, SQLException
{
  if (conf.get("mapred.input.dir") != null)
  {
    Path sqlPath = new Path(conf.get("mapred.input.dir"));
    FileSystem fs = HadoopFileUtils.getFileSystem(conf, sqlPath);
    ResultSet rs = null;
    if (sqlPath.toString().toLowerCase().endsWith(".sql"))
    {
      if (fs.exists(sqlPath))
      {
        try (FSDataInputStream in = fs.open(sqlPath))
        {
          try (InputStreamReader isr = new InputStreamReader(in))
          {
            try (BufferedReader br = new BufferedReader(isr))
            {
              StringBuilder sqlStr = new StringBuilder();
              String tmpStr = null;
              do
              {
                tmpStr = br.readLine();
                if (tmpStr != null)
                {
                  sqlStr.append(tmpStr);
                }
              } while (tmpStr != null);

              String username = conf.get(PgQueryInputFormat.USERNAME);
              String password = conf.get(PgQueryInputFormat.PASSWORD);
              String dbconnection = conf.get(PgQueryInputFormat.DBCONNECTION);

              Properties props = new Properties();
              props.setProperty("user", username);
              props.setProperty("password", password);
              props.setProperty("ssl", "true");

              try (Connection conn = DriverManager.getConnection(dbconnection, props))
              {
//              st = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
//              rs = st.executeQuery(sqlStr);

                try (Statement st = conn
                    .prepareStatement(sqlStr.toString(), ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY))
                {
                  rs = ((PreparedStatement) st).executeQuery();
                }
              }
              catch (SQLException e)
              {
                throw new IOException("Could not open database.", e);
              }

              return rs;
            }
          }
        }
      }
    }
  }
  throw new IllegalArgumentException("Neither a geometry collection or filename was set.");
}

//  public static void setInput(Configuration conf, ResultSet rs)
//  {
//  }

@Override
public RecordReader<LongWritable, Geometry> createRecordReader(InputSplit split,
    TaskAttemptContext context) throws IOException, InterruptedException
{
  ResultSet rs = null;
  try
  {
    rs = loadResultSet(context.getConfiguration());
  }
  catch (SQLException e)
  {
    throw new IOException("Could not get data from ResultSet.", e);
  }

  if (split instanceof ResultSetInputSplit)
  {
    ResultSetInputSplit giSplit = (ResultSetInputSplit) split;

    PgQueryRecordReader reader = new PgQueryRecordReader(rs, giSplit.getStart(), giSplit.getEnd());
    reader.initialize(giSplit, context);
    return reader;
  }
  else
  {
    throw new IOException("input split is not a ResultSetInputSplit");
  }

}

@Override
public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException
{
  Configuration conf = context.getConfiguration();
  int numSplits = conf.getInt("mapred.map.tasks", 2);

  int size = 0;
  ResultSet rs;
  try
  {
    rs = loadResultSet(context.getConfiguration());
    rs.last();
    size = rs.getRow();
    rs.beforeFirst();
  }
  catch (SQLException e)
  {
    throw new IOException("Could not get data from ResultSet.", e);
  }

  // make sure there are at least 10k features per node.
  final int MIN_FEATURES_PER_SPLIT = 10000;
  if (size / MIN_FEATURES_PER_SPLIT < numSplits)
  {
    numSplits = (int) Math.ceil((double) size / (double) MIN_FEATURES_PER_SPLIT);
  }

  List<InputSplit> result = new LinkedList<InputSplit>();

  for (int i = 0; i < numSplits; i++)
  {
    int start = (int) Math.round((double) i * (double) size / numSplits);
    int end = (int) Math.round((double) (i + 1) * (double) size / numSplits);
    result.add(new ResultSetInputSplit(start, end));
  }

  return result;
}

static public class ResultSetInputSplit extends InputSplit implements Serializable
{
  private static final long serialVersionUID = 1L;

  long endIndex;
  long startIndex;

  public ResultSetInputSplit(long start, long end)
  {
    this.startIndex = start;
    this.endIndex = end;
  }

  public long getEnd()
  {
    return endIndex;
  }

  @Override
  public long getLength()
  {
    return endIndex - startIndex;
  }

  @Override
  public String[] getLocations() throws IOException
  {
    return new String[0];
  }

  public long getStart()
  {
    return startIndex;
  }
}

static public class PgQueryRecordReader extends RecordReader<LongWritable, Geometry>
{
  private LongWritable key = new LongWritable(-1);
  private String _line;
  private int _geometryCol = -1;
  private WKTReader _wktReader = null;
  private ResultSet rs = null;
  private ResultSetMetaData rsMeta = null;
  private int numCols = 0;
  private long currentIndex;
  private long end;
  private long start;
  private List<String> attributeNames;
  private WritableGeometry feature;


  public PgQueryRecordReader()
  {
  }

  PgQueryRecordReader(ResultSet rs, long start, long end)
  {
    this.rs = rs;
    this.start = start;
    this.end = end;
    currentIndex = start - 1;
  }

  @Override
  public void close() throws IOException
  {
  }

  @Override
  public float getProgress() throws IOException, InterruptedException
  {
    long size = end - start;
    return (float) (currentIndex - start + 1) / (float) size;
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException
  {
    if (split instanceof ResultSetInputSplit)
    {
      Configuration conf = context.getConfiguration();
      ResultSetInputSplit ris = (ResultSetInputSplit) split;

      if (rs == null)
      {
        try
        {
          this.rs = loadResultSet(conf);
        }
        catch (SQLException e1)
        {
          throw new IOException(e1);
        }
      }

      if (rs == null)
      {
        throw new IOException("No results");
      }

      this.start = ris.startIndex;
      this.end = ris.endIndex;
      currentIndex = start - 1;

      attributeNames = new ArrayList<String>();

      try
      {
        rsMeta = rs.getMetaData();
        numCols = rsMeta.getColumnCount();

        //get metadata information and set schema
        try
        {
          rs.first();
          for (int i = 0; i < numCols; i++)
          {
            String colName = rsMeta.getColumnName(i + 1);
            attributeNames.add(colName);
          }

          rs.beforeFirst(); //move cursor to the beginning of the row
        }
        catch (SQLException e)
        {
          throw new IOException("Could not get data from ResultSet.", e);
        }
      }
      catch (SQLException e)
      {
        throw new IOException("Could now get data from ResultSet.", e);
      }
    }
    else
    {
      throw new IOException("input split is not a ResultSetInputSplit");
    }

  }

  @Override
  @SuppressWarnings("squid:S1166") // Exception caught and handled
  public boolean nextKeyValue() throws IOException
  {
    if (_wktReader == null)
    {
      _wktReader = new WKTReader();
    }
    boolean result = false;
    try
    {
      currentIndex++;
      if (currentIndex < end)
      {
        if (rs.next())
        {
          String wktGeometry = null;
          Map<String, String> attrs = new HashMap<>();

          String[] values = new String[numCols];
          for (int i = 0; i < numCols; i++)
          {
            values[i] = rs.getString(i + 1);
          }

          if (values.length == 0)
          {
            log.info("Values empty. Weird.");
          }

          for (int i = 0; i < values.length; i++)
          {
            if (i == _geometryCol)
            {
              wktGeometry = values[i];
            }
            attrs.put(attributeNames.get(i), values[i]);
          }


          if (wktGeometry != null)
          {
            try
            {
              feature = GeometryFactory.fromJTS(_wktReader.read(wktGeometry), attrs);
            }
            catch (Exception ignored)
            {
              //try to correct wktGeometry if possible
              try
              {
                feature = GeometryFactory.fromJTS(_wktReader.read(WktGeometryUtils.wktGeometryFixer(wktGeometry)));
              }
              catch (Exception e2)
              {
                //could not fix the geometry, so just set to null
                log.error("Could not fix geometry: " + wktGeometry + ". Continuing with null geometry.", e2);
                feature = GeometryFactory.createEmptyGeometry(attrs);
              }
            }
          }

          result = true;
        }
      }
    }
    catch (SQLException e)
    {
      throw new IOException("Could not get data from ResultSet.", e);
    }
    return result;
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
    return String.format("Current line: %s", _line);
  }
}
}
