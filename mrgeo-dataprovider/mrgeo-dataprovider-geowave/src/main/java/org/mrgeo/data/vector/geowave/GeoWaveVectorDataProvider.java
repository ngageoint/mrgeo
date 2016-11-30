package org.mrgeo.data.vector.geowave;

import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.query.cql.CQLQuery;
import mil.nga.giat.geowave.core.geotime.store.query.*;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.GeoWaveStoreFinder;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.CountDataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.RowRangeHistogramStatistics;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.StoreLoader;
import mil.nga.giat.geowave.core.store.query.BasicQuery;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.mapreduce.MapReduceDataStore;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputFormat;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.data.DataProviderException;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.vector.*;
import org.mrgeo.geometry.Geometry;
import org.opengis.filter.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class GeoWaveVectorDataProvider extends VectorDataProvider{
  static Logger log = LoggerFactory.getLogger(GeoWaveVectorDataProvider.class);

  private static final String PROVIDER_PROPERTIES_SIZE = GeoWaveVectorDataProvider.class.getName() + "providerProperties.size";
  private static final String PROVIDER_PROPERTIES_KEY_PREFIX = GeoWaveVectorDataProvider.class.getName() + "providerProperties.key";
  private static final String PROVIDER_PROPERTIES_VALUE_PREFIX = GeoWaveVectorDataProvider.class.getName() + "providerProperties.value";

  private static Map<String, DataSourceEntry> dataSourceEntries = new HashMap<String, DataSourceEntry>();
  private static GeoWaveConnectionInfo connectionInfo;
  private static final ReentrantReadWriteLock connectionLock = new ReentrantReadWriteLock();

  // Package private for unit testing
  static boolean initialized = false;

  private Configuration conf;
  private DataAdapter<?> dataAdapter;
  private PrimaryIndex primaryIndex;
  private DistributableQuery query;
  private Filter filter;
  private String cqlFilter;
  private com.vividsolutions.jts.geom.Geometry spatialConstraint;
  private Date startTimeConstraint;
  private Date endTimeConstraint;
  private String requestedIndexName;
  private GeoWaveVectorMetadataReader metaReader;
  private ProviderProperties providerProperties;

  private static class ParseResults
  {
    public String storeName;
    public String name;
    public Map<String, String> settings = new HashMap<String, String>();
  }

  public GeoWaveVectorDataProvider(Configuration conf, String inputPrefix, String input,
                                   ProviderProperties providerProperties)
  {
    super(inputPrefix, input);
    this.providerProperties = providerProperties;
    this.conf = conf;
  }

  public static GeoWaveConnectionInfo getConnectionInfo()
  {
    if (connectionInfo == null)
    {
      log.debug("attempting to load connection info");
      connectionLock.writeLock().lock();
      try
      {
          connectionInfo = GeoWaveConnectionInfo.load();
          log.debug("in getConnectionInfo, load returns " + connectionInfo);
      }
      finally {
        connectionLock.writeLock().unlock();
      }
    }
    connectionLock.readLock().lock();
    try
    {
      log.debug("returning connection info " + connectionInfo);
      return connectionInfo;
    }
    finally {
      connectionLock.readLock().unlock();
    }
  }

  static void setConnectionInfo(GeoWaveConnectionInfo connInfo)
  {
    connectionLock.writeLock().lock();
    try
    {
      connectionInfo = connInfo;
    }
    finally {
      connectionLock.writeLock().unlock();
    }
  }

  public AdapterStore getAdapterStore() throws IOException
  {
    String storeName = getStoreName();
    initDataSource(conf, storeName);
    DataSourceEntry entry = getDataSourceEntry(storeName);
    return entry.adapterStore;
  }

  public DataStore getDataStore() throws IOException
  {
    String storeName = getStoreName();
    initDataSource(conf, storeName);
    DataSourceEntry entry = getDataSourceEntry(storeName);
    return entry.dataStore;
  }

  public DataStatisticsStore getStatisticsStore() throws IOException
  {
    String storeName = getStoreName();
    initDataSource(conf, storeName);
    DataSourceEntry entry = getDataSourceEntry(storeName);
    return entry.statisticsStore;
  }

  public DataStorePluginOptions getDataStorePluginOptions() throws IOException
  {
    String storeName = getStoreName();
    initDataSource(conf, storeName);
    DataSourceEntry entry = getDataSourceEntry(storeName);
    return entry.inputStoreOptions;
  }

  public PrimaryIndex getPrimaryIndex() throws IOException
  {
    if (primaryIndex != null)
    {
      return primaryIndex;
    }
    String storeName = getStoreName();
    String resourceName = getGeoWaveResourceName();
    initDataSource(conf, storeName);
    DataSourceEntry entry = getDataSourceEntry(storeName);
    CloseableIterator<Index<?, ?>> indices = entry.indexStore.getIndices();
    try
    {
      DistributableQuery query = getQuery();
      while (indices.hasNext())
      {
        PrimaryIndex idx = (PrimaryIndex)indices.next();
        String indexName = idx.getId().getString();
        log.debug("Checking GeoWave index " + idx.getId().getString());
        NumericDimensionField<? extends CommonIndexValue>[] dimFields = idx.getIndexModel().getDimensions();
        for (NumericDimensionField<? extends CommonIndexValue> dimField : dimFields)
        {
          log.debug("  Dimension field: " + dimField.getFieldId().getString());
        }

        if (getStatisticsStore().getDataStatistics(
                getDataAdapter().getAdapterId(),
                RowRangeHistogramStatistics.composeId(idx.getId()),
                getAuthorizations(providerProperties)) != null)
        {
          log.debug("  Index stores data for " + resourceName);
          if (requestedIndexName != null && !requestedIndexName.isEmpty())
          {
            // The user requested a specific index. See if this is it, and then
            // make sure it supports the query criteria if there is any.
            if (indexName.equalsIgnoreCase(requestedIndexName))
            {
              log.debug("  Index matches the requested index");
              // Make sure if there is query criteria that the requested index can support
              // that query.
              if (query != null && !query.isSupported(idx))
              {
                throw new IOException(
                        "The requested index " + requestedIndexName + " does not support your query criteria");
              }
              primaryIndex = idx;
              return primaryIndex;
            }
          }
          else
          {
            // If there is no query, then just use the first index for this adapter
            if (query == null)
            {
              log.debug("  Since there is no query, we will use this index");
              primaryIndex = idx;
              return primaryIndex;
            }
            else
            {
              // Make sure the index supports the query, and get the number of fields
              // in the index. We want to use the index with the most fields.
              if (query.isSupported(idx))
              {
                log.debug("  This index does support this query " + query.getClass().getName());
                primaryIndex = idx;
                return primaryIndex;
              }
              else
              {
                log.debug("  This index does not support this query " + query.getClass().getName());
              }
            }
          }
        }
        else
        {
          // If the index does not contain data for the adapter, but it is the requested index
          // then report an error to the user.
          if (requestedIndexName != null && indexName.equalsIgnoreCase(requestedIndexName))
          {
            throw new IOException("The requested index " + requestedIndexName + " does not include " + resourceName);
          }
        }
      }
    }
    finally
    {
      indices.close();
    }
    if (primaryIndex != null)
    {
      return primaryIndex;
    }
    throw new IOException("Unable to find GeoWave index for adapter " + resourceName);
  }

  public String getGeoWaveResourceName() throws IOException
  {
    ParseResults results = parseResourceName(getResourceName());
    return results.name;
  }

  public String getStoreName() throws IOException
  {
    ParseResults results = parseResourceName(getResourceName());
    return results.storeName;
  }

  public DataAdapter<?> getDataAdapter() throws IOException
  {
    init();
    return dataAdapter;
  }

  /**
   * Returns the CQL filter for this data provider. If no filtering is required, then this will
   * return a null value.
   *
   * @return
   * @throws IOException
   */
  public String getCqlFilter() throws IOException
  {
    init();
    return cqlFilter;
  }

  public com.vividsolutions.jts.geom.Geometry getSpatialConstraint() throws IOException
  {
    init();
    return spatialConstraint;
  }

  @SuppressFBWarnings(value="EI_EXPOSE_REP", justification = "Used by trusted code")
  public Date getStartTimeConstraint() throws IOException
  {
    init();
    return startTimeConstraint;
  }

  @SuppressFBWarnings(value="EI_EXPOSE_REP", justification = "Used by trusted code")
  public Date getEndTimeConstraint() throws IOException
  {
    init();
    return endTimeConstraint;
  }

  public DistributableQuery getQuery() throws IOException
  {
    if (query != null)
    {
      return query;
    }
    com.vividsolutions.jts.geom.Geometry spatialConstraint = getSpatialConstraint();
    Date startTimeConstraint = getStartTimeConstraint();
    Date endTimeConstraint = getEndTimeConstraint();
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
        query = new SpatialTemporalQuery(tc, spatialConstraint);
      }
      else
      {
        log.debug("Using GeoWave SpatialQuery");
        query = new SpatialQuery(spatialConstraint);
      }
    }
    else
    {
      if (startTimeConstraint != null || endTimeConstraint != null)
      {
        log.debug("Using GeoWave TemporalQuery");
        TemporalConstraints tc = getTemporalConstraints(startTimeConstraint, endTimeConstraint);
        query = new TemporalQuery(tc);
      }
    }
    return query;
  }

  @SuppressFBWarnings(value="PZLA_PREFER_ZERO_LENGTH_ARRAYS", justification = "Null return value is valid")
  private String[] getAuthorizations(ProviderProperties providerProperties)
  {
    List<String> userRoles = null;
    if (providerProperties != null)
    {
      userRoles = providerProperties.getRoles();
    }
    if (userRoles != null)
    {
      String[] auths = new String[userRoles.size()];
      for (int i = 0; i < userRoles.size(); i++)
      {
        auths[i] = userRoles.get(i).trim();
      }
      return auths;
    }
    return null;
  }

  public QueryOptions getQueryOptions(ProviderProperties providerProperties) throws IOException
  {
    QueryOptions queryOptions = new QueryOptions(getDataAdapter(), getPrimaryIndex());
    String[] auths = getAuthorizations(providerProperties);
    if (auths != null)
    {
      queryOptions.setAuthorizations(auths);
    }
    return queryOptions;
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

  /**
   * Parses the input string into the optional namespace, the name of the input and
   * the optional query string
   * that accompanies it. The two strings are separated by a semi-colon, and the query
   * should be included in double quotes if it contains any semi-colons or square brackets.
   * But the double quotes are not required otherwise.
   *
   * @param input
   */
  private static ParseResults parseResourceName(String input) throws IOException
  {
    int semiColonIndex = input.indexOf(';');
    if (semiColonIndex == 0)
    {
      throw new IOException("Missing name from GeoWave data source: " + input);
    }
    int dotIndex = input.indexOf('.');
    ParseResults results = new ParseResults();
    if (semiColonIndex > 0)
    {
      if (dotIndex >= 0)
      {
        results.storeName = input.substring(0, dotIndex);
        results.name = input.substring(dotIndex + 1, semiColonIndex);
      }
      else
      {
        results.name = input.substring(0, semiColonIndex);
      }
      // Start parsing the data source settings
      String strSettings = input.substring(semiColonIndex + 1);
      // Now parse each property of the geowave source.
      parseDataSourceSettings(strSettings, results.settings);
    }
    else
    {
      if (dotIndex >= 0)
      {
        results.storeName = input.substring(0, dotIndex);
        results.name = input.substring(dotIndex + 1);
      }
      else
      {
        results.name = input;
      }
    }
    // If there is no datastore explicitly in the resource name, then we
    // check to make sure the GeoWave configuration for MrGeo only has one
    // datastore specified and use it. If there are multiple datastores
    // configured, then throw an exception.
    if (results.storeName == null || results.storeName.isEmpty())
    {
      GeoWaveConnectionInfo connectionInfo = getConnectionInfo();
      String[] dataStores = connectionInfo.getStoreNames();
      if (dataStores == null || dataStores.length == 0)
      {
        throw new IOException("Missing missing " + GeoWaveConnectionInfo.GEOWAVE_STORENAMES_KEY +
                              " in the MrGeo configuration");
      }
      else
      {
        if (dataStores.length > 1)
        {
          throw new IOException(
                  "You must specify a GeoWave data store for " + input +
                  " because multiple GeoWave data stores are configured in MrGeo (e.g." +
                  " mystore." + input + ")");
        }
        results.storeName = dataStores[0];
      }
    }
    return results;
  }

  // Package private for unit testing
  static void parseDataSourceSettings(String strSettings,
                                              Map<String, String> settings) throws IOException
  {
    boolean foundSemiColon = true;
    String remaining = strSettings.trim();
    if (remaining.isEmpty())
    {
      return;
    }

    int settingIndex = 0;
    while (foundSemiColon)
    {
      int equalsIndex = remaining.indexOf("=", settingIndex);
      if (equalsIndex >= 0)
      {
        String keyName = remaining.substring(settingIndex, equalsIndex).trim();
        // Everything after the = char
        remaining = remaining.substring(equalsIndex + 1).trim();
        if (remaining.length() > 0)
        {
          // Handle double-quoted settings specially, skipping escaped double
          // quotes inside the value.
          if (remaining.startsWith("\""))
          {
            // Find the index of the corresponding closing quote. Note that double
            // quotes can be escaped with a backslash (\) within the quoted string.
            int closingQuoteIndex = remaining.indexOf('"', 1);
            while (closingQuoteIndex > 0)
            {
              // If the double quote is not preceeded by an escape backslash,
              // then we've found the closing quote.
              if (remaining.charAt(closingQuoteIndex - 1) != '\\')
              {
                break;
              }
              closingQuoteIndex = remaining.indexOf('"', closingQuoteIndex + 1);
            }
            if (closingQuoteIndex >= 0)
            {
              String value = remaining.substring(1, closingQuoteIndex);
              log.debug("Adding GeoWave source key setting " + keyName + " = " + value);
              settings.put(keyName, value);
              settingIndex = 0;
              int nextSemiColonIndex = remaining.indexOf(';', closingQuoteIndex + 1);
              if (nextSemiColonIndex >= 0)
              {
                foundSemiColon = true;
                remaining = remaining.substring(nextSemiColonIndex + 1).trim();
              }
              else
              {
                // No more settings
                foundSemiColon = false;
              }
            }
            else
            {
              throw new IOException("Invalid GeoWave settings string, expected ending double quote for key " +
                                    keyName + " in " + strSettings);
            }
          }
          else
          {
            // The value is not quoted
            int semiColonIndex = remaining.indexOf(";");
            if (semiColonIndex >= 0)
            {
              String value = remaining.substring(0, semiColonIndex);
              log.debug("Adding GeoWave source key setting " + keyName + " = " + value);
              settings.put(keyName, value);
              settingIndex = 0;
              remaining = remaining.substring(semiColonIndex + 1);
            }
            else
            {
              log.debug("Adding GeoWave source key setting " + keyName + " = " + remaining);
              settings.put(keyName, remaining);
              // There are no more settings since there are no more semi-colons
              foundSemiColon = false;
            }
          }
        }
        else
        {
          throw new IOException("Missing value for " + keyName);
        }
      }
      else
      {
        throw new IOException("Invalid syntax. No value assignment in \"" + remaining + "\"");
      }
    }
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
    ParseResults results = parseResourceName(getResourceName());
    init(results);
    DataSourceEntry entry = getDataSourceEntry(results.storeName);
    Query query = new BasicQuery(new BasicQuery.Constraints());
    CQLQuery cqlQuery = new CQLQuery(query, filter,
                                     (FeatureDataAdapter)entry.adapterStore.getAdapter(new ByteArrayId(this.getGeoWaveResourceName())));
    return new GeoWaveVectorReader(results.storeName, entry.dataStore,
                                   entry.adapterStore.getAdapter(new ByteArrayId(this.getGeoWaveResourceName())),
                                   cqlQuery, getPrimaryIndex(), filter, providerProperties);
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
  public RecordReader<FeatureIdWritable, Geometry> getRecordReader() throws IOException
  {
    ParseResults results = parseResourceName(getResourceName());
    DistributableQuery query = null;
    init(results);
    query = getQuery();
    DataSourceEntry entry = getDataSourceEntry(results.storeName);
    DataAdapter<?> adapter = getDataAdapter();
    if (entry.dataStore instanceof MapReduceDataStore)
    {
      try
      {
        RecordReader delegateRecordReader = ((MapReduceDataStore) entry.dataStore).createRecordReader(
                query,
                new QueryOptions(adapter, getPrimaryIndex(), getAuthorizations(getProviderProperties())),
                entry.adapterStore,
                entry.statisticsStore,
                entry.indexStore, false, null);
        return new GeoWaveVectorRecordReader(delegateRecordReader);
      }
      catch (InterruptedException e)
      {
        throw new IOException(e);
      }
    }
    throw new IOException("GeoWave data store " + entry.dataStore.getClass().getName() +
                          " does not support Hadoop input");
  }

  @Override
  public RecordWriter<FeatureIdWritable, Geometry> getRecordWriter()
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
    // This must be a quick sanity check on the remote side for whether or not
    // the GeoWave data provider should be used on the remote side.
    initConnectionInfo(conf);
    return (getConnectionInfo() != null);
  }

  public static boolean isValid()
  {
    // This must be a quick sanity check on the client side for whether or not
    // the GeoWave data provider should be used.
    initConnectionInfo();
    return (getConnectionInfo() != null);
  }

  public static String[] listVectors(final ProviderProperties providerProperties) throws IOException
  {
    initConnectionInfo();
    List<String> results = new ArrayList<String>();
    for (String storeName: getConnectionInfo().getStoreNames())
    {
      initDataSource(null, storeName);
      DataSourceEntry entry = getDataSourceEntry(storeName);
      CloseableIterator<DataAdapter<?>> iter = entry.adapterStore.getAdapters();
      try
      {
        while (iter.hasNext())
        {
          DataAdapter<?> adapter = iter.next();
          if (adapter != null)
          {
            ByteArrayId adapterId = adapter.getAdapterId();
            if (checkAuthorizations(adapterId, storeName, providerProperties))
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
    }
    String[] resultArray = new String[results.size()];
    return results.toArray(resultArray);
  }

  public static boolean canOpen(String input,
                                ProviderProperties providerProperties) throws IOException
  {
    log.debug("Inside canOpen with " + input);
    initConnectionInfo();
    ParseResults results = parseResourceName(input);
    try
    {
      initDataSource(null, results.storeName);
      DataSourceEntry entry = getDataSourceEntry(results.storeName);
      CloseableIterator<DataAdapter<?>> iter = entry.adapterStore.getAdapters();
      while (iter.hasNext()) {
        DataAdapter<?> da = iter.next();
        log.debug("GeoWave adapter: " + da.getAdapterId().toString());
      }
      ByteArrayId adapterId = new ByteArrayId(results.name);
      DataAdapter<?> adapter = entry.adapterStore.getAdapter(adapterId);
      if (adapter == null)
      {
        return false;
      }
      return checkAuthorizations(adapterId, results.storeName, providerProperties);
    }
    catch(IOException ignored) {
    }
    catch(IllegalArgumentException e)
    {
      log.info("Unable to open " + input + " with the GeoWave data provider: " + e.getMessage());
    }
    return false;
  }

  private static boolean checkAuthorizations(ByteArrayId adapterId,
                                             String namespace,
      ProviderProperties providerProperties) throws IOException
  {
    // Check to see if the requester is authorized to see any of the data in
    // the adapter.
    return (getAdapterCount(adapterId, namespace, providerProperties) > 0L);
  }

  private static DataSourceEntry getDataSourceEntry(String namespace) throws IOException
  {
    DataSourceEntry entry = dataSourceEntries.get(namespace);
    if (entry == null)
    {
      throw new IOException("Data source was not yet initialized for namespace: " + namespace);
    }
    return entry;
  }

  public static long getAdapterCount(ByteArrayId adapterId,
                                     String namespace,
                                     ProviderProperties providerProperties)
          throws IOException
  {
    initConnectionInfo();
    initDataSource(null, namespace);
    DataSourceEntry entry = getDataSourceEntry(namespace);
    List<String> roles = null;
    if (providerProperties != null)
    {
      roles = providerProperties.getRoles();
    }
    if (roles != null && roles.size() > 0)
    {
      String auths = StringUtils.join(roles, ",");
      CountDataStatistics<?> count = (CountDataStatistics<?>)entry.statisticsStore.getDataStatistics(adapterId,  CountDataStatistics.STATS_ID, auths);
      if (count != null && count.isSet())
      {
        return count.getCount();
      }
    }
    else
    {
      CountDataStatistics<?> count = (CountDataStatistics<?>)entry.statisticsStore.getDataStatistics(adapterId,  CountDataStatistics.STATS_ID);
      if (count != null && count.isSet())
      {
        return count.getCount();
      }
    }
    return 0L;
  }

  private void init() throws IOException
  {
    // Don't initialize more than once.
    if (initialized)
    {
      return;
    }
    ParseResults results = parseResourceName(getResourceName());
    init(results);
  }

  private void init(ParseResults results) throws IOException
  {
    // Don't initialize more than once.
    if (initialized)
    {
      return;
    }
    initialized = true;
    // Extract the GeoWave adapter name and optional CQL string
    DataSourceEntry entry = getDataSourceEntry(results.storeName);
    // Now perform initialization for this specific data provider (i.e. for
    // this resource).
    dataAdapter = entry.adapterStore.getAdapter(new ByteArrayId(results.name));
    assignSettings(results.name, results.settings);

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

  // Package private for unit testing
  void assignSettings(String name, Map<String, String> settings) throws IOException
  {
    filter = null;
    spatialConstraint = null;
    startTimeConstraint = null;
    endTimeConstraint = null;
    for (Map.Entry<String, String> entry : settings.entrySet())
    {
      String keyName = entry.getKey();
      if (keyName != null && !keyName.isEmpty())
      {
        String value = entry.getValue();
        switch(keyName)
        {
          case "spatial":
          {
            WKTReader wktReader = new WKTReader();
            try
            {
              spatialConstraint = wktReader.read(value);
            }
            catch (ParseException e)
            {
              throw new IOException("Invalid WKT specified for spatial property of GeoWave data source " +
                                    name);
            }
            break;
          }

          case "startTime":
          {
            startTimeConstraint = parseDate(value);
            break;
          }

          case "endTime":
          {
            endTimeConstraint = parseDate(value);
            break;
          }

          case "cql":
          {
            if (value != null && !value.isEmpty())
            {
              cqlFilter = value;
              try
              {
                filter = ECQL.toFilter(value);
              }
              catch (CQLException e)
              {
                throw new IOException("Bad CQL filter: " + value, e);
              }
            }
            break;
          }
          case "index":
          {
            requestedIndexName = value.trim();
            break;
          }
          default:
            throw new IOException("Unrecognized setting for GeoWave data source " +
                                  name + ": " + keyName);
        }
      }
    }

    if ((startTimeConstraint == null) != (endTimeConstraint == null))
    {
      throw new IOException("When querying a GeoWave data source by time," +
                            " both the start and the end are required.");
    }
    if (startTimeConstraint != null && endTimeConstraint != null && startTimeConstraint.after(endTimeConstraint))
    {
      throw new IOException("For GeoWave data source " + name + ", startDate must be after endDate");
    }
  }

  private Date parseDate(String value)
  {
    DateTimeFormatter formatter = ISODateTimeFormat.dateOptionalTimeParser();
    DateTime dateTime = formatter.parseDateTime(value);
    return dateTime.toDate();
  }

  private static void initConnectionInfo(Configuration conf)
  {
    // The connectionInfo only needs to be set once. It is the same for
    // the duration of the JVM. Note that it is instantiated differently
    // on the driver-side than it is within map/reduce tasks. This method
    // loads connection settings from the job configuration.
    if (connectionInfo == null)
    {
      connectionLock.writeLock().lock();
      try
      {
          connectionInfo = GeoWaveConnectionInfo.load(conf);
      }
      finally {
        connectionLock.writeLock().unlock();
      }
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
      connectionLock.writeLock().lock();
      try
      {
          connectionInfo = GeoWaveConnectionInfo.load();
      }
      finally {
        connectionLock.writeLock().unlock();
      }
    }
  }

  @SuppressFBWarnings(value="PATH_TRAVERSAL_IN", justification = "It is ok to read the config file here")
  private static void initDataSource(Configuration conf, String storeName) throws IOException
  {
    DataSourceEntry entry = dataSourceEntries.get(storeName);
    if (entry == null)
    {
      entry = new DataSourceEntry();
      // Check to see if we are running client side or not. If the Hadoop config
      // contains the storenames property, then we should load geowave resources
      // based on settings in the Hadoop config. Otherwise, we should load the
      // geowave resources based on settings in the MrGeo config (on client side).
      if (conf == null || conf.get(GeoWaveConnectionInfo.GEOWAVE_STORENAMES_KEY) == null)
      {
        // Since the geowave store name is not in the configuration, then this is being
        // called from the client side.
        GeoWaveConnectionInfo connInfo = getConnectionInfo();
        String[] storeNamesForMrGeo = connInfo.getStoreNames();
        if (storeNamesForMrGeo == null)
        {
          throw new IOException("To use GeoWave, you must set " +
                                GeoWaveConnectionInfo.GEOWAVE_STORENAMES_KEY +
                                " in the MrGeo config file");
        }
        StoreLoader inputStoreLoader = new StoreLoader(storeName);

        if (!inputStoreLoader.loadFromConfig(new File(MrGeoProperties.findMrGeoConf())))
        {
          String msg = "Cannot find GeoWave store name: " + inputStoreLoader.getStoreName();
          log.error(msg);
          throw new IOException(msg);
        }
        DataStorePluginOptions inputStoreOptions = inputStoreLoader.getDataStorePlugin();
//        inputStoreOptions.getFactoryOptions().setGeowaveNamespace(namespace);
        dataSourceEntries.put(storeName, entry);
        entry.inputStoreOptions = inputStoreOptions;
        entry.indexStore = inputStoreOptions.createIndexStore();
//        entry.secondaryIndexStore = inputStoreOptions.createSecondaryIndexStore();
        entry.statisticsStore = inputStoreOptions.createDataStatisticsStore();
        entry.adapterStore = inputStoreOptions.createAdapterStore();
//        entry.adapterIndexMappingStore = inputStoreOptions.createAdapterIndexMappingStore();
        entry.dataStore = inputStoreOptions.createDataStore();
      }
      else
      {
        // The store name was in the configuration, so this is being called from
        // the remote side. Create a fake JobContext to satisfy the GeoWave API
        // access to the Hadoop configuration.
        JobContext context = new JobContextImpl(conf, new JobID());
        final Map<String, String> configOptions = GeoWaveInputFormat.getStoreConfigOptions(context);
        entry.inputStoreOptions = new DataStorePluginOptions(storeName, configOptions);
        entry.indexStore = entry.inputStoreOptions.createIndexStore();
//        entry.secondaryIndexStore = entry.inputStoreOptions.createSecondaryIndexStore();
        entry.statisticsStore = entry.inputStoreOptions.createDataStatisticsStore();
        entry.adapterStore = GeoWaveStoreFinder.createAdapterStore(configOptions);
//        entry.adapterIndexMappingStore = entry.inputStoreOptions.createAdapterIndexMappingStore();
        entry.dataStore = GeoWaveStoreFinder.createDataStore(configOptions);
      }
    }
  }

  static class DataSourceEntry
  {
    public DataSourceEntry()
    {
    }

    private DataStorePluginOptions inputStoreOptions;
    private IndexStore indexStore;
//    private SecondaryIndexDataStore secondaryIndexStore;
//    private AdapterIndexMappingStore adapterIndexMappingStore;
    private AdapterStore adapterStore;
    private DataStatisticsStore statisticsStore;
    private DataStore dataStore;
  }
}
