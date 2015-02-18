package org.mrgeo.data.geowave.vector;

import java.io.IOException;
import java.util.Properties;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.hadoop.conf.Configuration;
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
    try
    {
      return GeoWaveVectorDataProvider.canOpen(input, conf);
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
  public boolean canOpen(String input, Properties providerProperties) throws IOException
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
  public boolean canWrite(String input, Configuration conf) throws IOException
  {
    throw new IOException("GeoWave provider does not support writing vectors");
  }

  @Override
  public boolean canWrite(String input, Properties providerProperties) throws IOException
  {
    throw new IOException("GeoWave provider does not support writing vectors");
  }

  @Override
  public boolean exists(String name, Configuration conf) throws IOException
  {
    return canOpen(name, conf);
  }

  @Override
  public boolean exists(String name, Properties providerProperties) throws IOException
  {
    return canOpen(name, providerProperties);
  }

  @Override
  public void delete(String name, Configuration conf) throws IOException
  {
    throw new IOException("GeoWave provider does not support deleting vectors");
  }

  @Override
  public void delete(String name, Properties providerProperties) throws IOException
  {
    throw new IOException("GeoWave provider does not support deleting vectors");
  }
}
