package org.mrgeo.data.geowave.vector;

import java.io.IOException;
import java.util.Properties;

import mil.nga.giat.geowave.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.accumulo.metadata.AccumuloIndexStore;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.store.CloseableIterator;
import mil.nga.giat.geowave.store.adapter.AdapterStore;
import mil.nga.giat.geowave.store.adapter.DataAdapter;
import mil.nga.giat.geowave.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.vector.AccumuloDataStatisticsStoreExt;
import mil.nga.giat.geowave.vector.VectorDataStore;
import mil.nga.giat.geowave.vector.adapter.FeatureDataAdapter;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.data.vector.VectorDataProvider;
import org.mrgeo.data.vector.VectorInputFormatContext;
import org.mrgeo.data.vector.VectorInputFormatProvider;
import org.mrgeo.data.vector.VectorMetadataReader;
import org.mrgeo.data.vector.VectorMetadataWriter;
import org.mrgeo.data.vector.VectorOutputFormatContext;
import org.mrgeo.data.vector.VectorOutputFormatProvider;
import org.mrgeo.data.vector.VectorReader;
import org.mrgeo.data.vector.VectorReaderContext;
import org.mrgeo.data.vector.VectorWriter;
import org.mrgeo.data.vector.VectorWriterContext;
import org.mrgeo.geometry.Geometry;
import org.opengis.feature.simple.SimpleFeatureType;

public class GeoWaveVectorDataProvider extends VectorDataProvider
{
  private GeoWaveConnectionInfo connectionInfo;
  private AccumuloOperations storeOperations;
  private AdapterStore adapterStore;
  private DataAdapter<?> dataAdapter;
  private VectorDataStore dataStore;
  private Index index;
  private GeoWaveVectorMetadataReader metaReader;
  private Properties providerProperties;

  public GeoWaveVectorDataProvider(String input, Configuration conf)
  {
    super(input);
    connectionInfo = GeoWaveConnectionInfo.load(conf);
    // TODO: Look up provider properties in the configuration and reload them
    // into a new Properties instance
  }

  public GeoWaveVectorDataProvider(String input, Properties providerProperties)
  {
    super(input);
    connectionInfo = GeoWaveConnectionInfo.load();
    this.providerProperties = providerProperties;
  }

  @Override
  public VectorMetadataReader getMetadataReader()
  {
    if (metaReader == null)
    {
      metaReader = new GeoWaveVectorMetadataReader(this);
    }
    return metaReader;
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
    try
    {
      init();
    }
    catch (AccumuloSecurityException e)
    {
      throw new IOException("AccumuloSecurityException in GeoWave data provider getVectorReader", e);
    }
    catch (AccumuloException e)
    {
      throw new IOException("AccumuloException in GeoWave data provider getVectorReader", e);
    }
    GeoWaveVectorReader reader = new GeoWaveVectorReader(dataStore,
        adapterStore.getAdapter(new ByteArrayId(this.getResourceName())),
        index);
    return reader;
  }

  @Override
  public VectorReader getVectorReader(VectorReaderContext context) throws IOException
  {
    return getVectorReader();
  }

  @Override
  public VectorWriter getVectorWriter()
  {
    // Not yet implemented
    return null;
  }

  @Override
  public VectorWriter getVectorWriter(VectorWriterContext context)
  {
    // Not yet implemented
    return null;
  }

  @Override
  public RecordReader<LongWritable, Geometry> getRecordReader()
  {
    return new GeoWaveVectorRecordReader();
  }

  @Override
  public RecordWriter<LongWritable, Geometry> getRecordWriter()
  {
    // Not yet implemented
    return null;
  }

  @Override
  public VectorInputFormatProvider getVectorInputFormatProvider(VectorInputFormatContext context)
  {
    return new GeoWaveVectorInputFormatProvider(context, connectionInfo);
  }

  @Override
  public VectorOutputFormatProvider getVectorOutputFormatProvider(VectorOutputFormatContext context)
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

  public DataAdapter<?> getDataAdapter() throws AccumuloSecurityException, AccumuloException
  {
    init();
    return dataAdapter;
  }

  private void init() throws AccumuloSecurityException, AccumuloException
  {
    storeOperations = new BasicAccumuloOperations(
        connectionInfo.getZookeeperServers(),
        connectionInfo.getInstanceName(),
        connectionInfo.getUserName(),
        connectionInfo.getPassword(),
        connectionInfo.getNamespace());
    final AccumuloIndexStore indexStore = new AccumuloIndexStore(
        storeOperations);

    final DataStatisticsStore statisticsStore = new AccumuloDataStatisticsStoreExt(
        storeOperations);
    adapterStore = new AccumuloAdapterStore(
        storeOperations);
    dataStore = new VectorDataStore(
        indexStore,
        adapterStore,
        statisticsStore,
        storeOperations);
    dataAdapter = adapterStore.getAdapter(new ByteArrayId(getResourceName()));
    CloseableIterator<Index> indices = dataStore.getIndices();
    if (indices.hasNext())
    {
      index = indices.next();
    }
    SimpleFeatureType sft = ((FeatureDataAdapter)dataAdapter).getType();
    int attributeCount = sft.getAttributeCount();
    System.out.println("attributeCount = " + attributeCount);
    CloseableIterator<?> iter = dataStore.query(dataAdapter, null);
    while (iter.hasNext())
    {
      Object value = iter.next();
      System.out.println("class is " + value.getClass().getName());
//      System.out.println("value is " + value);
    }
    // etc...
  }
}
