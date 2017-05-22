package org.mrgeo.data.vector.pg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.vector.*;
import org.mrgeo.geometry.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class PgVectorDataProvider extends VectorDataProvider
{
  static Logger log = LoggerFactory.getLogger(PgVectorDataProvider.class);

  protected static boolean canOpen(
          String input,
          ProviderProperties providerProperties) throws IOException
  {
    PgDbSettings dbSettings = parseResourceName(input);
    try(Connection conn = getDbConnection(dbSettings)) {
      return true;
    }
    catch(SQLException e) {
      log.info("Unable to get DB connection to " + dbSettings.getUrl(), e);
    }
    return false;
  }

  static Connection getDbConnection(PgDbSettings dbSettings) throws SQLException
  {
    Properties props = new Properties();
    props.setProperty("user", dbSettings.getUsername());
    props.setProperty("password", dbSettings.getPassword());
    props.setProperty("ssl", dbSettings.getSsl());
    return DriverManager.getConnection(dbSettings.getUrl(), props);
  }

  public PgVectorDataProvider(Configuration conf, String inputPrefix, String input,
                              ProviderProperties providerProperties)
  {
    super(inputPrefix, input, providerProperties);
  }

  @Override
  public VectorMetadataReader getMetadataReader()
  {
    return new PgVectorMetadataReader(this);
  }

  @Override
  public VectorMetadataWriter getMetadataWriter()
  {
    // Not yet implemented
    return null;
  }

  @Override
  public VectorReader getVectorReader() throws IOException
  {
    return null;
  }

  @Override
  public VectorReader getVectorReader(VectorReaderContext context) throws IOException
  {
    return null;
  }

  @Override
  public VectorWriter getVectorWriter() throws IOException
  {
    // Not yet implemented
    return null;
  }

  @Override
  public RecordReader<FeatureIdWritable, Geometry> getRecordReader() throws IOException
  {
    PgDbSettings results = parseResourceName(getResourceName());
    return new PgVectorRecordReader(results);
  }

  @Override
  public RecordWriter<FeatureIdWritable, Geometry> getRecordWriter()
  {
    // Not yet implemented
    return null;
  }

  @Override
  public VectorInputFormatProvider getVectorInputFormatProvider(VectorInputFormatContext context) throws IOException
  {
    PgDbSettings results = parseResourceName(getResourceName());
    return new PgVectorInputFormatProvider(context, this, results);
  }

  @Override
  public VectorOutputFormatProvider getVectorOutputFormatProvider(VectorOutputFormatContext context) throws IOException
  {
    // Not yet implemented
    return null;
  }

  @Override
  public void delete() throws IOException
  {
    // Not yet implemented
  }

  @Override
  public void move(String toResource) throws IOException
  {
    // Not yet implemented
  }

  PgDbSettings parseResourceName() throws IOException
  {
    return parseResourceName(getResourceName());
  }

  /**
   * Parses the input string into the url, username, password, query,
   * and geometry column name. Each of the settings is separated by
   * a semi-colon. Each of the settings themselves are formatted as
   * "name=value".
   *
   * @param input
   */
  private static PgDbSettings parseResourceName(String input) throws IOException
  {
    Map<String, String> settings = new HashMap<String, String>();
    parseDataSourceSettings(input, settings);
    String url;
    if (settings.containsKey("url")) {
      url = settings.get("url");
    }
    else {
      throw new IOException("Missing expected url setting");
    }

    String username;
    if (settings.containsKey("username")) {
      username = settings.get("username");
    }
    else {
      throw new IOException("Missing expected username setting");
    }

    String password;
    if (settings.containsKey("password")) {
      password = settings.get("password");
    }
    else {
      throw new IOException("Missing expected password setting");
    }

    String query;
    if (settings.containsKey("query")) {
      query = settings.get("query");
    }
    else {
      throw new IOException("Missing expected query setting");
    }

    String countQuery = null;
    if (settings.containsKey("countQuery")) {
      countQuery = settings.get("countQuery");
    }

    String mbrQuery = null;
    if (settings.containsKey("mbrQuery")) {
      mbrQuery = settings.get("mbrQuery");
    }

    String geomColumnLabel = null;
    if (settings.containsKey("geometryField")) {
      geomColumnLabel = settings.get("geometryField");
    }

    String wktColumnLabel;
    if (settings.containsKey("wktField")) {
      wktColumnLabel = settings.get("wktField");
    }
    else {
      throw new IOException("Missing expected wktField setting");
    }

    if (mbrQuery == null && geomColumnLabel == null) {
      throw new IOException("You must specify either mbrQuery or geometryField");
    }

    String ssl;
    if (settings.containsKey("ssl")) {
      ssl = settings.get("ssl");
    }
    else {
      throw new IOException("Missing expected ssl setting");
    }
    return new PgDbSettings(url, username, password, query, countQuery,
            mbrQuery, geomColumnLabel, wktColumnLabel, ssl);
  }
}
