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

package org.mrgeo.data.vector.pg;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.data.vector.FeatureIdWritable;
import org.mrgeo.data.vector.VectorInputFormat;
import org.mrgeo.geometry.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class PgVectorInputFormat extends VectorInputFormat
{
  private static Logger log = LoggerFactory.getLogger(PgVectorInputFormat.class);
  private static final int rowsPerPartition = Integer.parseInt(
          MrGeoProperties.getInstance().getProperty(
                  MrGeoConstants.MRGEO_POSTGRES_PARTITION_RECORDS,
                  "10000"));
  private PgDbSettings dbSettings;

  public PgVectorInputFormat(PgDbSettings dbSettings)
  {
    this.dbSettings = dbSettings;
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    // Get the count of records for the specified query and divide into
    // partitions based on a max count per partition.
    long recordCount = getRecordCount();
    long numPartitions = recordCount / rowsPerPartition + 1;
    List<InputSplit> splits = new ArrayList<InputSplit>();
    for (int i=0; i < numPartitions; i++) {
      PgInputSplit split = new PgInputSplit(i * rowsPerPartition, rowsPerPartition);
      splits.add(split);
    }
    return splits;
  }

  @Override
  public RecordReader<FeatureIdWritable, Geometry> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    return super.createRecordReader(split, context);
  }

  @SuppressFBWarnings(value = {"SQL_INJECTION_JDBC", "SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING"}, justification = "User supplied queries are a requirement")
  protected long getRecordCount() throws IOException
  {
    String countQuery = dbSettings.getCountQuery();
    if (countQuery == null || countQuery.isEmpty()) {
      // Look for the first occurrence of SELECT ... FROM and replace it with
      // SELECT count(*) FROM. Make sure to match case insensitively, but
      // the non-replaced portion of the string must retain its case (hence the
      // use of (?i) to inline the case insensitive match).
      countQuery = dbSettings.getQuery().replaceFirst("(?i)SELECT .* FROM", "SELECT count(*) FROM");
    }
    // Run the count query and grab the result.
    try (Connection conn = PgVectorDataProvider.getDbConnection(dbSettings))
    {
      try (Statement st = conn.prepareStatement(countQuery,
              ResultSet.TYPE_FORWARD_ONLY,
              ResultSet.CONCUR_READ_ONLY))
      {
        try (ResultSet rs = ((PreparedStatement) st).executeQuery())
        {
          rs.next();
          return rs.getLong(1);
        }
      }
    }
    catch (SQLException e)
    {
      String msg = "Unable to get the count of records using query: " + countQuery + ". Consider explicitly specifying the countQuery property of your data source.";
      log.error(msg, e);
      throw new IOException(msg, e);
    }
  }
}
