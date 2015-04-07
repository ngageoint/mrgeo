package org.mrgeo.data.vector.geowave;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
import mil.nga.giat.geowave.store.query.BasicQuery;
import mil.nga.giat.geowave.store.query.Query;
import mil.nga.giat.geowave.vector.AccumuloDataStatisticsStoreExt;
import mil.nga.giat.geowave.vector.VectorDataStore;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
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
import org.opengis.filter.Filter;

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
  private Filter filter;
  private String cqlFilter;
  private GeoWaveVectorMetadataReader metaReader;
  private Properties providerProperties;

  public GeoWaveVectorDataProvider(String inputPrefix, String input, Configuration conf)
  {
    super(inputPrefix, input);
    initConnectionInfo(conf);
    // We are on the task execution side of map/reduce, and the provider
    // properties are stored in the job configuration. Get them from there.
    providerProperties = new Properties();
    loadProviderProperties(providerProperties, conf);
  }

  public GeoWaveVectorDataProvider(String inputPrefix, String input, Properties providerProperties)
  {
    super(inputPrefix, input);
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

  public static DataStatisticsStore getStatisticsStore() throws AccumuloSecurityException, AccumuloException, IOException
  {
    initDataSource();
    return statisticsStore;
  }

  public static Index getIndex() throws AccumuloSecurityException, AccumuloException, IOException
  {
    initDataSource();
    return index;
  }

  public String getGeoWaveResourceName() throws IOException
  {
    String[] specs = parseResourceName(getResourceName());
    return specs[0];
  }

  public DataAdapter<?> getDataAdapter() throws AccumuloSecurityException, AccumuloException, IOException
  {
    init();
    return dataAdapter;
  }

  /**
   * Returns the CQL filter for this data provider. If no filtering is required, then this will
   * return a null value.
   *
   * @return
   * @throws AccumuloSecurityException
   * @throws AccumuloException
   * @throws IOException
   */
  public String getCqlFilter() throws AccumuloSecurityException, AccumuloException, IOException
  {
    init();
    return cqlFilter;
  }

  /**
   * Parses the input string into the name of the input and the optional query string
   * that accompanies it. The two strings are separated by a semi-colon, and the query
   * should be included in double quotes if it contains any semi-colons or square brackets.
   * But the double quotes are not required otherwise.
   *
   * @param input
   * @return
   */
  private static String[] parseResourceName(String input) throws IOException
  {
    int semiColonIndex = input.indexOf(';');
    if (semiColonIndex == 0)
    {
      throw new IOException("Missing name from GeoWave data source: " + input);
    }
    String[] result = new String[2];
    if (semiColonIndex > 0)
    {
      result[0] = input.substring(0, semiColonIndex);
      String query = input.substring(semiColonIndex + 1).trim();
      if (query.startsWith("\""))
      {
        if (query.endsWith("\""))
        {
          result[1] = query.substring(1, query.length() - 1);
        }
        else
        {
          throw new IOException("Invalid query string, expected ending double quote: " + input);
        }
      }
      else
      {
        result[1] = query;
      }
    }
    else
    {
      result[0] = input;
      result[1] = null;
    }
    return result;
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
    Query query = new BasicQuery(new BasicQuery.Constraints());
    GeoWaveVectorReader reader = new GeoWaveVectorReader(dataStore,
        adapterStore.getAdapter(new ByteArrayId(this.getGeoWaveResourceName())),
        query, index, filter, providerProperties);
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

  public static String[] listVectors(final Properties providerProperties) throws AccumuloException, AccumuloSecurityException, IOException
  {
    initConnectionInfo();
    initDataSource();
    List<String> results = new ArrayList<String>();
    CloseableIterator<DataAdapter<?>> iter = adapterStore.getAdapters();
    try
    {
      while (iter.hasNext())
      {
        DataAdapter<?> adapter = iter.next();
        if (adapter != null)
        {
          ByteArrayId adapterId = adapter.getAdapterId();
          if (checkAuthorizations(adapterId, providerProperties))
          {
            results.add(adapterId.getString());
          }
        }
      }
    }
    finally
    {
      if (iter != null)
      {
        iter.close();
      }
    }
    String[] resultArray = new String[results.size()];
    return results.toArray(resultArray);
  }

  public static boolean canOpen(String input, Configuration conf) throws AccumuloException, AccumuloSecurityException, IOException
  {
    initConnectionInfo(conf);
    initDataSource();
    Properties providerProperties = new Properties();
    loadProviderProperties(providerProperties, conf);
    String[] specs = parseResourceName(input);
    ByteArrayId adapterId = new ByteArrayId(specs[0]);
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
    String[] specs = parseResourceName(input);
    ByteArrayId adapterId = new ByteArrayId(specs[0]);
    DataAdapter<?> adapter = adapterStore.getAdapter(adapterId);
    if (adapter == null)
    {
      return false;
    }
    return checkAuthorizations(adapterId, providerProperties);
  }

  private static boolean checkAuthorizations(ByteArrayId adapterId,
      Properties providerProperties) throws IOException, AccumuloException, AccumuloSecurityException
  {
    // Check to see if the requester is authorized to see any of the data in
    // the adapter.
    return (getAdapterCount(adapterId, providerProperties) > 0L);
  }

  public static long getAdapterCount(ByteArrayId adapterId,
      Properties providerProperties) throws IOException, AccumuloException, AccumuloSecurityException
  {
    initConnectionInfo();
    initDataSource();
    String auths = providerProperties.getProperty(DataProviderFactory.PROVIDER_PROPERTY_USER_ROLES);
    if (auths != null)
    {
      CountDataStatistics<?> count = (CountDataStatistics<?>)statisticsStore.getDataStatistics(adapterId,  CountDataStatistics.STATS_ID, auths);
      if (count != null && count.isSet())
      {
        return count.getCount();
      }
    }
    else
    {
      CountDataStatistics<?> count = (CountDataStatistics<?>)statisticsStore.getDataStatistics(adapterId,  CountDataStatistics.STATS_ID);
      if (count != null && count.isSet())
      {
        return count.getCount();
      }
    }
    return 0L;
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
    // Extract the GeoWave adapter name and optional CQL string
    String[] specs = parseResourceName(getResourceName());
    // Now perform initialization for this specific data provider (i.e. for
    // this resource).
    dataAdapter = adapterStore.getAdapter(new ByteArrayId(specs[0]));
    if (specs.length > 1 && specs[1] != null && !specs[1].isEmpty())
    {
      cqlFilter = specs[1];
      try
      {
        filter = ECQL.toFilter(specs[1]);
      }
      catch (CQLException e)
      {
        throw new IOException("Bad CQL filter: " + specs[1], e);
      }
    }
    else
    {
      filter = null;
    }
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
