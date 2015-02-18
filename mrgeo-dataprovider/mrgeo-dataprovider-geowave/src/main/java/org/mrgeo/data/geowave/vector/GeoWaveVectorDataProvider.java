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
import mil.nga.giat.geowave.store.adapter.statistics.CountDataStatistics;
import mil.nga.giat.geowave.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.vector.AccumuloDataStatisticsStoreExt;
import mil.nga.giat.geowave.vector.VectorDataStore;
//import mil.nga.giat.geowave.vector.adapter.FeatureDataAdapter;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.mrgeo.data.DataProviderFactory;
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
//import org.opengis.feature.simple.SimpleFeatureType;

public class GeoWaveVectorDataProvider extends VectorDataProvider
{
  private static final String PROVIDER_PROPERTIES_SIZE = GeoWaveVectorDataProvider.class.getName() + "providerProperties.size";
  private static final String PROVIDER_PROPERTIES_KEY_PREFIX = GeoWaveVectorDataProvider.class.getName() + "providerProperties.key";
  private static final String PROVIDER_PROPERTIES_VALUE_PREFIX = GeoWaveVectorDataProvider.class.getName() + "providerProperties.value";

  private static GeoWaveConnectionInfo connectionInfo;
  private static AccumuloOperations storeOperations;
  private static AdapterStore adapterStore;
  private static DataStatisticsStore statisticsStore;
  private static VectorDataStore dataStore;
  private static Index index;

  private DataAdapter<?> dataAdapter;
  private GeoWaveVectorMetadataReader metaReader;
  private Properties providerProperties;

  public GeoWaveVectorDataProvider(String input, Configuration conf)
  {
    super(input);
    initConnectionInfo(conf);
    // We are on the task execution side of map/reduce, and the provider
    // properties are stored in the job configuration. Get them from there.
    providerProperties = new Properties();
    loadProviderProperties(providerProperties, conf);
  }

  public GeoWaveVectorDataProvider(String input, Properties providerProperties)
  {
    super(input);
    // This constructor is only called from driver-side (i.e. not in
    // map/reduce tasks), so the connection settings are obtained from
    // the mrgeo.conf file.
    if (connectionInfo == null)
    {
      connectionInfo = GeoWaveConnectionInfo.load();
    }
    this.providerProperties = providerProperties;
  }

  public static GeoWaveConnectionInfo getConnectionInfo()
  {
    return connectionInfo;
  }

  public static AccumuloOperations getStoreOperations() throws AccumuloSecurityException, AccumuloException, IOException
  {
    initDataSource();
    return storeOperations;
  }

  public static AdapterStore getAdapterStore() throws AccumuloSecurityException, AccumuloException, IOException
  {
    initDataSource();
    return adapterStore;
  }

  public static DataStatisticsStore getStatisticsStore() throws AccumuloSecurityException, AccumuloException, IOException
  {
    initDataSource();
    return statisticsStore;
  }

  public static VectorDataStore getDataStore() throws AccumuloSecurityException, AccumuloException, IOException
  {
    initDataSource();
    return dataStore;
  }

  public static Index getIndex() throws AccumuloSecurityException, AccumuloException, IOException
  {
    initDataSource();
    return index;
  }

  public DataAdapter<?> getDataAdapter() throws AccumuloSecurityException, AccumuloException, IOException
  {
    init();
    return dataAdapter;
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
    return new GeoWaveVectorInputFormatProvider(context, this);
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

  public static boolean isValid(Configuration conf)
  {
    initConnectionInfo(conf);
    return (connectionInfo != null);
  }

  public static boolean isValid()
  {
    initConnectionInfo();
    return (connectionInfo != null);
  }

  public static boolean canOpen(String input, Configuration conf) throws AccumuloException, AccumuloSecurityException, IOException
  {
    initConnectionInfo(conf);
    initDataSource();
    Properties providerProperties = new Properties();
    loadProviderProperties(providerProperties, conf);
    ByteArrayId adapterId = new ByteArrayId(input);
    DataAdapter<?> adapter = adapterStore.getAdapter(adapterId);
    if (adapter == null)
    {
      return false;
    }
    return checkAuthorizations(adapterId, providerProperties);
  }

  public static boolean canOpen(String input, Properties providerProperties) throws AccumuloException, AccumuloSecurityException, IOException
  {
    initConnectionInfo();
    initDataSource();
    ByteArrayId adapterId = new ByteArrayId(input);
    DataAdapter<?> adapter = adapterStore.getAdapter(adapterId);
    if (adapter == null)
    {
      return false;
    }
    return checkAuthorizations(adapterId, providerProperties);
  }

  private static boolean checkAuthorizations(ByteArrayId adapterId, Properties providerProperties)
  {
    // Check to see if the requester is authorized to see any of the data in
    // the adapter.
    String auths = providerProperties.getProperty(DataProviderFactory.PROVIDER_PROPERTY_USER_ROLES);
    if (auths != null)
    {
      CountDataStatistics<?> count = (CountDataStatistics<?>)statisticsStore.getDataStatistics(adapterId,  CountDataStatistics.STATS_ID, auths);
      return (count != null && count.isSet() && count.getCount() > 0);
    }
    else
    {
      CountDataStatistics<?> count = (CountDataStatistics<?>)statisticsStore.getDataStatistics(adapterId,  CountDataStatistics.STATS_ID);
      return (count != null && count.isSet() && count.getCount() > 0);
    }
  }

  public static void storeProviderProperties(Properties providerProperties, Configuration conf)
  {
    int index = 1;
    conf.setInt(PROVIDER_PROPERTIES_SIZE, providerProperties.size());
    for (Object key: providerProperties.keySet())
    {
      if (key != null)
      {
        conf.set(PROVIDER_PROPERTIES_KEY_PREFIX + "." + index, (String)key);
        String value = providerProperties.getProperty((String)key);
        if (value != null)
        {
          conf.set(PROVIDER_PROPERTIES_VALUE_PREFIX + "." + index, value);
        }
      }
    }
  }

  public static void loadProviderProperties(Properties providerProperties, Configuration conf)
  {
    int size = conf.getInt(PROVIDER_PROPERTIES_SIZE, 0);
    for (int index = 0; index < size; index++)
    {
      String key = conf.get(PROVIDER_PROPERTIES_KEY_PREFIX + "." + index);
      if (key != null)
      {
        String value = conf.get(PROVIDER_PROPERTIES_VALUE_PREFIX + "." + index);
        providerProperties.put(key, value);
      }
    }
  }

  private void init() throws AccumuloSecurityException, AccumuloException, IOException
  {
    // Now perform initialization for this specific data provider (i.e. for
    // this resource).
    dataAdapter = adapterStore.getAdapter(new ByteArrayId(getResourceName()));
// Testing code
//    SimpleFeatureType sft = ((FeatureDataAdapter)dataAdapter).getType();
//    int attributeCount = sft.getAttributeCount();
//    System.out.println("attributeCount = " + attributeCount);
//    CloseableIterator<?> iter = dataStore.query(dataAdapter, null);
//    try
//    {
//      while (iter.hasNext())
//      {
//        Object value = iter.next();
//        System.out.println("class is " + value.getClass().getName());
//        System.out.println("value is " + value);
//      }
//    }
//    finally
//    {
//      iter.close();
//    }
  }

  private static void initConnectionInfo(Configuration conf)
  {
    // The connectionInfo only needs to be set once. It is the same for
    // the duration of the JVM. Note that it is instantiated differently
    // on the driver-side than it is within map/reduce tasks. This method
    // loads connection settings from the job configuration.
    if (connectionInfo == null)
    {
      connectionInfo = GeoWaveConnectionInfo.load(conf);
    }
  }

  private static void initConnectionInfo()
  {
    // The connectionInfo only needs to be set once. It is the same for
    // the duration of the JVM. Note that it is instantiated differently
    // on the driver-side than it is within map/reduce tasks. This method
    // loads connection settings from the mrgeo.conf file.
    if (connectionInfo == null)
    {
      connectionInfo = GeoWaveConnectionInfo.load();
    }
  }

  private static void initDataSource() throws AccumuloException, AccumuloSecurityException, IOException
  {
    if (storeOperations == null)
    {
      storeOperations = new BasicAccumuloOperations(
          connectionInfo.getZookeeperServers(),
          connectionInfo.getInstanceName(),
          connectionInfo.getUserName(),
          connectionInfo.getPassword(),
          connectionInfo.getNamespace());
      final AccumuloIndexStore indexStore = new AccumuloIndexStore(
          storeOperations);
  
      statisticsStore = new AccumuloDataStatisticsStoreExt(
          storeOperations);
      adapterStore = new AccumuloAdapterStore(
          storeOperations);
      dataStore = new VectorDataStore(
          indexStore,
          adapterStore,
          statisticsStore,
          storeOperations);
      CloseableIterator<Index> indices = dataStore.getIndices();
      try
      {
        if (indices.hasNext())
        {
          index = indices.next();
        }
      }
      finally
      {
        indices.close();
      }
    }
  }
}
