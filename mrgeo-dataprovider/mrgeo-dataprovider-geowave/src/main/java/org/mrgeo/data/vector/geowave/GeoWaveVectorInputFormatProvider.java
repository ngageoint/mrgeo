package org.mrgeo.data.vector.geowave;

import mil.nga.giat.geowave.datastore.accumulo.mapreduce.input.GeoWaveInputConfigurator;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.input.GeoWaveInputFormat;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.index.Index;
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
import java.util.List;

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
      GeoWaveInputFormat.setAccumuloOperationsInfo(
          job,
          connectionInfo.getZookeeperServers(),
          connectionInfo.getInstanceName(),
          connectionInfo.getUserName(),
          connectionInfo.getPassword(),
          dataProvider.getNamespace());
      connectionInfo.writeToConfig(conf);

      DataAdapter<?> adapter;
      adapter = dataProvider.getDataAdapter();
      if (adapter == null)
      {
        throw new DataProviderException("Missing data adapter in data provider for " + dataProvider.getPrefixedResourceName());
      }
      GeoWaveInputFormat.addDataAdapter(conf, adapter);
      Index index = dataProvider.getIndex();
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
      com.vividsolutions.jts.geom.Geometry spatialConstraint = dataProvider.getSpatialConstraint();
      Date startTimeConstraint = dataProvider.getStartTimeConstraint();
      Date endTimeConstraint = dataProvider.getEndTimeConstraint();
      if ((startTimeConstraint == null) != (endTimeConstraint == null))
      {
        throw new DataProviderException("When querying a GeoWave data source by time," +
                                        " both the start and the end are required.");
      }
      if (spatialConstraint != null)
      {
        if (startTimeConstraint != null || endTimeConstraint != null)
        {
          log.debug("Using GeoWave SpatialTemporalQuery");
          TemporalConstraints tc = getTemporalConstraints(startTimeConstraint, endTimeConstraint);
          GeoWaveInputFormat.setQuery(conf, new SpatialTemporalQuery(tc, spatialConstraint));
        }
        else
        {
          log.debug("Using GeoWave SpatialQuery");
          GeoWaveInputFormat.setQuery(conf, new SpatialQuery(spatialConstraint));
        }
      }
      else
      {
        if (startTimeConstraint != null || endTimeConstraint != null)
        {
          log.debug("Using GeoWave TemporalQuery");
          TemporalConstraints tc = getTemporalConstraints(startTimeConstraint, endTimeConstraint);
          GeoWaveInputFormat.setQuery(conf, new TemporalQuery(tc));
        }
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
    List<String> userRoles = null;
    if (providerProperties != null)
    {
      userRoles = providerProperties.getRoles();
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
