package org.mrgeo.data.vector.pg;

import org.apache.hadoop.conf.Configuration;
import org.mrgeo.data.DataProviderException;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.vector.VectorDataProvider;
import org.mrgeo.data.vector.VectorDataProviderFactory;

import java.io.IOException;
import java.util.Map;

public class PgVectorDataProviderFactory implements VectorDataProviderFactory
{
  static Configuration conf;

  private static Configuration getConf()
  {
    if (conf == null)
    {
      throw new IllegalArgumentException("The configuration was not initialized");
    }
    return conf;
  }

  @Override
  public boolean isValid(Configuration conf)
  {
    return true;
  }

  @Override
  public void initialize(Configuration config) throws DataProviderException
  {
    if (conf == null)
    {
      conf = config;
    }
  }

  @Override
  public boolean isValid() {
    return true;
  }

  @Override
  public String getPrefix() {
    return "pg";
  }

  @Override
  public Map<String, String> getConfiguration()
  {
    // All configuration settings are included in the resource name
    // itself. Nothing to do here.
    return null;
  }

  @Override
  public void setConfiguration(Map<String, String> properties)
  {
    // All configuration settings are included in the resource name
    // itself. Nothing to do here.
  }

  @Override
  public VectorDataProvider createVectorDataProvider(
          String prefix,
          String input,
          ProviderProperties providerProperties)
  {
    return new PgVectorDataProvider(getConf(), prefix, input, providerProperties);
  }

  @Override
  public String[] listVectors(ProviderProperties providerProperties) throws IOException
  {
    // We cannot give results for this method because all of the connection
    // information is contained in the resource name. So it is assumed that
    // the user knows all of the vectors available in the data source.
    return new String[0];
  }

  @Override
  public boolean canOpen(
          String input,
          ProviderProperties providerProperties) throws IOException
  {
    return PgVectorDataProvider.canOpen(input, providerProperties);
  }

  @Override
  public boolean canWrite(
          String input,
          ProviderProperties providerProperties) throws IOException
  {
    // We do not write vector data to Postgres yet
    return false;
  }

  @Override
  public boolean exists(
          String name,
          ProviderProperties providerProperties) throws IOException
  {
    return false;
  }

  @Override
  public void delete(String name, ProviderProperties providerProperties) throws IOException
  {
    throw new IOException("Cannot delete Postgres vector sources");
  }
}
