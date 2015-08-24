package org.mrgeo.data.vector.geowave;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.hadoop.conf.Configuration;
import org.mrgeo.data.DataProviderException;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.vector.VectorDataProvider;
import org.mrgeo.data.vector.VectorDataProviderFactory;

public class GeoWaveVectorDataProviderFactory implements VectorDataProviderFactory
{
  @Override
  public boolean isValid(Configuration conf)
  {
    return GeoWaveVectorDataProvider.isValid(conf);
  }

  @Override
  public void initialize(Configuration conf) throws DataProviderException
  {
  }

  @Override
  public boolean isValid()
  {
    return GeoWaveVectorDataProvider.isValid();
  }

  @Override
  public String getPrefix()
  {
    return "geowave";
  }

  @Override
  public Map<String, String> getConfiguration()
  {
    GeoWaveConnectionInfo connInfo = GeoWaveVectorDataProvider.getConnectionInfo();
    if (connInfo != null)
    {
      return connInfo.toMap();
    }
    return null;
  }

  @Override
  public void setConfiguration(Map<String, String> settings)
  {
    GeoWaveConnectionInfo connInfo = GeoWaveConnectionInfo.fromMap(settings);
    GeoWaveVectorDataProvider.setConnectionInfo(connInfo);
  }

  @Override
  public VectorDataProvider createVectorDataProvider(String prefix, String input,
      ProviderProperties providerProperties)
  {
    return new GeoWaveVectorDataProvider(prefix, input, providerProperties);
  }

  @Override
  public String[] listVectors(final ProviderProperties providerProperties) throws IOException
  {
    try
    {
      return GeoWaveVectorDataProvider.listVectors(providerProperties);
    }
    catch (AccumuloException e)
    {
      throw new IOException(e);
    }
    catch (AccumuloSecurityException e)
    {
      throw new IOException(e);
    }
  }

  @Override
  public boolean canOpen(String input, ProviderProperties providerProperties) throws IOException
  {
    try
    {
      return GeoWaveVectorDataProvider.canOpen(input, providerProperties);
    }
    catch (AccumuloException e)
    {
      throw new IOException(e);
    }
    catch (AccumuloSecurityException e)
    {
      throw new IOException(e);
    }
  }

  @Override
  public boolean canWrite(String input, ProviderProperties providerProperties) throws IOException
  {
    throw new IOException("GeoWave provider does not support writing vectors");
  }

  @Override
  public boolean exists(String name, ProviderProperties providerProperties) throws IOException
  {
    return canOpen(name, providerProperties);
  }

  @Override
  public void delete(String name, ProviderProperties providerProperties) throws IOException
  {
    throw new IOException("GeoWave provider does not support deleting vectors");
  }
}
