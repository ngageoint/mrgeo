package org.mrgeo.data.vector.geowave;

import java.io.IOException;
import java.util.Properties;

import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveInputConfigurator;
import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveInputFormat;
import mil.nga.giat.geowave.store.adapter.DataAdapter;
import mil.nga.giat.geowave.store.index.Index;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.mrgeo.data.DataProviderException;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.vector.VectorInputFormatContext;
import org.mrgeo.data.vector.VectorInputFormatProvider;
import org.mrgeo.geometry.Geometry;

public class GeoWaveVectorInputFormatProvider extends VectorInputFormatProvider
{
  private GeoWaveVectorDataProvider dataProvider;
//  private Integer minInputSplits; // ?
//  private Integer maxInputSplits; // ?
  
  public GeoWaveVectorInputFormatProvider(VectorInputFormatContext context,
      GeoWaveVectorDataProvider dataProvider)
  {
    super(context);
    this.dataProvider = dataProvider;
  }

  @Override
  public InputFormat<LongWritable, Geometry> getInputFormat(String input)
  {
    return new GeoWaveVectorInputFormat();
  }

  @Override
  public void setupJob(Job job, Properties providerProperties) throws DataProviderException
  {
    super.setupJob(job, providerProperties);
    Configuration conf = job.getConfiguration();
    GeoWaveConnectionInfo connectionInfo = GeoWaveVectorDataProvider.getConnectionInfo();
    GeoWaveInputFormat.setAccumuloOperationsInfo(
        job,
        connectionInfo.getZookeeperServers(),
        connectionInfo.getInstanceName(),
        connectionInfo.getUserName(),
        connectionInfo.getPassword(),
        connectionInfo.getNamespace());
    connectionInfo.writeToConfig(conf);
    GeoWaveVectorDataProvider.storeProviderProperties(providerProperties, conf);
    DataAdapter<?> adapter;
    try
    {
      adapter = dataProvider.getDataAdapter();
      if (adapter == null)
      {
        throw new DataProviderException("Missing data adapter in data provider for " + dataProvider.getResourceName());
      }
      GeoWaveInputFormat.addDataAdapter(conf, adapter);
      Index index = GeoWaveVectorDataProvider.getIndex();
      if (index == null)
      {
        throw new DataProviderException("Missing index in data provider for " + dataProvider.getResourceName());
      }
      GeoWaveInputFormat.addIndex(conf, index);
    }
    catch (AccumuloSecurityException e)
    {
      throw new DataProviderException(e);
    }
    catch (AccumuloException e)
    {
      throw new DataProviderException(e);
    }
    catch (IOException e)
    {
      throw new DataProviderException(e);
    }
    String strUserRoles = providerProperties.getProperty(DataProviderFactory.PROVIDER_PROPERTY_USER_ROLES);
    String[] userRoles = null;
    if (strUserRoles != null)
    {
      userRoles = strUserRoles.split(",");
    }
    if (userRoles != null)
    {
      for (String role: userRoles)
      {
        GeoWaveInputConfigurator.addAuthorization(GeoWaveInputFormat.class, conf, role.trim());
      }
    }
    
//    if (query != null) {
//      GeoWaveInputFormat.setQuery(
//          job,
//          query);
//    }
//    if (minInputSplits != null) {
//      GeoWaveInputFormat.setMinimumSplitCount(
//          job,
//          minInputSplits);
//    }
//    if (maxInputSplits != null) {
//      GeoWaveInputFormat.setMaximumSplitCount(
//          job,
//          maxInputSplits);
//    }
  }

  @Override
  public void teardown(Job job) throws DataProviderException
  {
  }
}
