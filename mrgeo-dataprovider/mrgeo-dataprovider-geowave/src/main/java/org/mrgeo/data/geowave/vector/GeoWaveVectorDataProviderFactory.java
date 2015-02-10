package org.mrgeo.data.geowave.vector;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.mrgeo.data.vector.VectorDataProvider;
import org.mrgeo.data.vector.VectorDataProviderFactory;

public class GeoWaveVectorDataProviderFactory implements VectorDataProviderFactory
{
  @Override
  public boolean isValid(Configuration conf)
  {
    GeoWaveConnectionInfo connectionInfo = GeoWaveConnectionInfo.load(conf);
    return (connectionInfo != null);
  }

  @Override
  public boolean isValid(Properties providerProperties)
  {
    GeoWaveConnectionInfo connectionInfo = GeoWaveConnectionInfo.load();
    return (connectionInfo != null);
  }

  @Override
  public String getPrefix()
  {
    return "geowave";
  }

  @Override
  public VectorDataProvider createVectorDataProvider(String input,
      Configuration conf)
  {
    return new GeoWaveVectorDataProvider(input, conf);
  }

  @Override
  public VectorDataProvider createVectorDataProvider(String input,
      Properties providerProperties)
  {
    return new GeoWaveVectorDataProvider(input, providerProperties);
  }

  @Override
  public String[] listVectors(final Properties providerProperties) throws IOException
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean canOpen(String input, Configuration conf) throws IOException
  {
    // TODO Auto-generated method stub
    // TODO Need to implement
    return true;
  }

  @Override
  public boolean canOpen(String input, Properties providerProperties) throws IOException
  {
    // TODO Auto-generated method stub
    // TODO Need to implement
    return true;
  }

  @Override
  public boolean canWrite(String input, Configuration conf) throws IOException
  {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean canWrite(String input, Properties providerProperties) throws IOException
  {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean exists(String name, Configuration conf) throws IOException
  {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean exists(String name, Properties providerProperties) throws IOException
  {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void delete(String name, Configuration conf) throws IOException
  {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void delete(String name, Properties providerProperties) throws IOException
  {
    // TODO Auto-generated method stub
    
  }
}
