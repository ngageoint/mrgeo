package org.mrgeo.data.vector.geowave;

import mil.nga.giat.geowave.core.cli.GenericStoreCommandLineOptions;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.config.ConfigUtils;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputConfigurator;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputFormat;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.geotime.store.query.*;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.mrgeo.data.DataProviderException;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.vector.FeatureIdWritable;
import org.mrgeo.data.vector.VectorInputFormatContext;
import org.mrgeo.data.vector.VectorInputFormatProvider;
import org.mrgeo.geometry.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GeoWaveVectorInputFormatProvider extends VectorInputFormatProvider
{
  static Logger log = LoggerFactory.getLogger(GeoWaveVectorInputFormatProvider.class);

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
  public InputFormat<FeatureIdWritable, Geometry> getInputFormat(String input)
  {
    return new GeoWaveVectorInputFormat();
  }

  @Override
  public void setupJob(Job job, ProviderProperties providerProperties) throws DataProviderException
  {
    super.setupJob(job, providerProperties);
    Configuration conf = job.getConfiguration();
    GeoWaveConnectionInfo connectionInfo = GeoWaveVectorDataProvider.getConnectionInfo();
    try
    {
      // TODO: Probably should make "accumulo" a mrgeo.conf geowave setting
//      final GenericStoreCommandLineOptions<AdapterStore> options = ((PersistableAdapterStore)dataProvider.getAdapterStore()).getCliOptions();
//      GeoWaveInputFormat.setAdapterStoreName(
//              config,
//              options.getFactory().getName());
//      GeoWaveInputFormat.setStoreConfigOptions(
//              config,
//              ConfigUtils.valuesToStrings(
//                      options.getConfigOptions(),
//                      options.getFactory().getOptions()));
//      GeoWaveInputFormat.setGeoWaveNamespace(
//              config,
//              options.getNamespace());
      GeoWaveInputFormat.setDataStoreName(
              conf,
              "accumulo");
      Map<String, String> options = new HashMap<String, String>();
      options.put("user", connectionInfo.getUserName());
      options.put("password", connectionInfo.getPassword());
      options.put("zookeeper", connectionInfo.getZookeeperServers());
      options.put("instance", connectionInfo.getInstanceName());
      GeoWaveInputFormat.setStoreConfigOptions(
              conf,
              options);
      GeoWaveInputFormat.setGeoWaveNamespace(conf, dataProvider.getNamespace());
//      GeoWaveInputFormat.setAccumuloOperationsInfo(
//          job,
//          connectionInfo.getZookeeperServers(),
//          connectionInfo.getInstanceName(),
//          connectionInfo.getUserName(),
//          connectionInfo.getPassword(),
//          dataProvider.getNamespace());
      connectionInfo.writeToConfig(conf);

      DataAdapter<?> adapter;
      adapter = dataProvider.getDataAdapter();
      if (adapter == null)
      {
        throw new DataProviderException("Missing data adapter in data provider for " + dataProvider.getPrefixedResourceName());
      }
      GeoWaveInputFormat.addDataAdapter(conf, adapter);
      PrimaryIndex index = dataProvider.getPrimaryIndex();
      if (index == null)
      {
        throw new DataProviderException("Missing index in data provider for " + dataProvider.getPrefixedResourceName());
      }
      GeoWaveInputFormat.addIndex(conf, index);
      // Configure CQL filtering if specified in the data provider
      String cql = dataProvider.getCqlFilter();
      if (cql != null && !cql.isEmpty())
      {
        conf.set(GeoWaveVectorRecordReader.CQL_FILTER, cql);
      }
      // Configure queries and index based on properties defined in the data provider
      DistributableQuery query = dataProvider.getQuery();
      if (query != null)
      {
        GeoWaveInputFormat.setQuery(conf, query);
      }
      QueryOptions queryOptions = dataProvider.getQueryOptions(providerProperties);
      if (queryOptions != null)
      {
        GeoWaveInputFormat.setQueryOptions(conf, queryOptions);
      }
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

  private TemporalConstraints getTemporalConstraints(Date startTime, Date endTime)
  {
    TemporalRange tr = new TemporalRange();
    if (startTime != null)
    {
      tr.setStartTime(startTime);
    }
    if (endTime != null)
    {
      tr.setEndTime(endTime);
    }
    TemporalConstraints tc = new TemporalConstraints();
    tc.add(tr);
    return tc;
  }

  @Override
  public void teardown(Job job) throws DataProviderException
  {
  }
}
