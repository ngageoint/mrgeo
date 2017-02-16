package org.mrgeo.data.vector.geowave;

import org.apache.hadoop.conf.Configuration;
import org.mrgeo.data.DataProviderException;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.vector.VectorDataProvider;
import org.mrgeo.data.vector.VectorDataProviderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class GeoWaveVectorDataProviderFactory implements VectorDataProviderFactory
{
static Logger log = LoggerFactory.getLogger(GeoWaveVectorDataProviderFactory.class);
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
  return GeoWaveVectorDataProvider.isValid(conf);
}

@Override
@SuppressWarnings("squid:S2696") // Exception caught and handled
public void initialize(Configuration config) throws DataProviderException
{
  if (conf == null)
  {
    conf = config;
  }
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
  log.error("GeoWave classpath is: " + System.getProperty("java.class.path"));
  GeoWaveConnectionInfo connInfo = GeoWaveConnectionInfo.fromMap(settings);
  GeoWaveVectorDataProvider.setConnectionInfo(connInfo);
}

@Override
public VectorDataProvider createVectorDataProvider(String prefix, String input,
    ProviderProperties providerProperties)
{
  return new GeoWaveVectorDataProvider(getConf(), prefix, input, providerProperties);
}

@Override
public String[] listVectors(final ProviderProperties providerProperties) throws IOException
{
  return GeoWaveVectorDataProvider.listVectors(providerProperties);
}

@Override
public boolean canOpen(String input, ProviderProperties providerProperties) throws IOException
{
  return GeoWaveVectorDataProvider.canOpen(input, providerProperties);
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
