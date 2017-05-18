package org.mrgeo.data.vector.pg;

import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.mrgeo.data.vector.FeatureIdWritable;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.geometry.GeometryFactory;
import org.mrgeo.geometry.WritableGeometry;

import java.io.IOException;
import java.sql.*;
import java.util.Properties;

public class PgVectorRecordReader extends RecordReader<FeatureIdWritable, Geometry>
{
  private PgDbSettings dbSettings;
  private long offset;
  private long limit;
  private long currIndex;
  private int columnCount;
  private String[] columnLabels;
  private Connection conn;
  private Statement stmt;
  private ResultSet rs;
  private WKTReader wktReader;
  private WritableGeometry currGeom;
  private FeatureIdWritable currKey = new FeatureIdWritable();

  public PgVectorRecordReader(PgDbSettings dbSettings)
  {
    this.dbSettings = dbSettings;
  }

  @SuppressFBWarnings(value = {"SQL_INJECTION_JDBC", "SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING"}, justification = "User supplied queries are a requirement")
  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException
  {
    if (!(split instanceof PgInputSplit)) {
      throw new IOException("Expected an instance of PgInputSplit");
    }
    offset = ((PgInputSplit) split).getOffset();
    limit = ((PgInputSplit) split).getLimit();
    currIndex = offset - 1;
    try
    {
      conn = PgVectorDataProvider.getDbConnection(dbSettings);
      // If the offset is < 0, then there is only one partition, so no need
      // for a limit query.
      String fullQuery = (offset < 0) ? dbSettings.getQuery() : (dbSettings.getQuery() + " OFFSET " + offset + " LIMIT " + limit);
      stmt = conn.prepareStatement(fullQuery,
              ResultSet.TYPE_FORWARD_ONLY,
              ResultSet.CONCUR_READ_ONLY);
      rs = ((PreparedStatement) stmt).executeQuery();
      ResultSetMetaData metadata = rs.getMetaData();
      columnCount = metadata.getColumnCount();
      columnLabels = new String[columnCount];
      for (int c=1; c <= columnCount; c++) {
        columnLabels[c-1] = metadata.getColumnLabel(c);
      }
    }
    catch (SQLException e)
    {
      throw new IOException("Could not open database.", e);
    }
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException
  {
    if (wktReader == null)
    {
      wktReader = new WKTReader();
    }
    try {
      if (rs.next()) {
        String wkt = rs.getString(dbSettings.getGeomColumnLabel());
        try {
          currGeom = GeometryFactory.fromJTS(wktReader.read(wkt));
          for (int c=1; c <= columnCount; c++) {
            String columnLabel = columnLabels[c-1];
            if (!columnLabel.equals(dbSettings.getGeomColumnLabel())) {
              currGeom.setAttribute(columnLabel, rs.getString(c));
            }
          }
          currIndex++;
          return true;
        } catch (ParseException e) {
          throw new IOException("Unable to parse WKT from column " + dbSettings.getGeomColumnLabel() + " with value " + wkt);
        }
      }
    }
    catch(SQLException e) {
      throw new IOException("Error getting next key/value pair", e);
    }
    return false;
  }

  @Override
  public FeatureIdWritable getCurrentKey() throws IOException, InterruptedException
  {
    return currKey;
  }

  @Override
  public Geometry getCurrentValue() throws IOException, InterruptedException
  {
    return currGeom;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException
  {
    return (float)(currIndex + 1 - offset) / (float)limit;
  }

  @Override
  public void close() throws IOException
  {
    try {
      rs.close();
    } catch (SQLException e) {
      throw new IOException("Error closing JDBC result set", e);
    }
    try {
      stmt.close();
    } catch (SQLException e) {
      throw new IOException("Error closing JDBC statement", e);
    }
    try {
      conn.close();
    } catch (SQLException e) {
      throw new IOException("Error closing JDBC connection", e);
    }
  }
}
