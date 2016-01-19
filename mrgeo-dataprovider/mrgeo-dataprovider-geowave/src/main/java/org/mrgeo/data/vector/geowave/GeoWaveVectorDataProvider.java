package org.mrgeo.data.vector.geowave;

import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloIndexStore;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.CountDataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.dimension.DimensionField;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.query.BasicQuery;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.adapter.vector.AccumuloDataStatisticsStoreExt;
import mil.nga.giat.geowave.adapter.vector.VectorDataStore;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.util.ClassUtil;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.vector.*;
import org.mrgeo.geometry.Geometry;
import org.opengis.filter.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class GeoWaveVectorDataProvider extends VectorDataProvider
{
  static Logger log = LoggerFactory.getLogger(GeoWaveVectorDataProvider.class);

  private static final String PROVIDER_PROPERTIES_SIZE = GeoWaveVectorDataProvider.class.getName() + "providerProperties.size";
  private static final String PROVIDER_PROPERTIES_KEY_PREFIX = GeoWaveVectorDataProvider.class.getName() + "providerProperties.key";
  private static final String PROVIDER_PROPERTIES_VALUE_PREFIX = GeoWaveVectorDataProvider.class.getName() + "providerProperties.value";

  private static Map<String, DataSourceEntry> dataSourceEntries = new HashMap<String, DataSourceEntry>();
  private static GeoWaveConnectionInfo connectionInfo;

  // Package private for unit testing
  static boolean initialized = false;

  private DataAdapter<?> dataAdapter;
  private Filter filter;
  private String cqlFilter;
  private com.vividsolutions.jts.geom.Geometry spatialConstraint;
  private Date startTimeConstraint;
  private Date endTimeConstraint;
  private GeoWaveVectorMetadataReader metaReader;
  private ProviderProperties providerProperties;

  private static class ParseResults
  {
    public String namespace;
    public String name;
    public Map<String, String> settings = new HashMap<String, String>();
  }

  public GeoWaveVectorDataProvider(String inputPrefix, String input, ProviderProperties providerProperties)
  {
    super(inputPrefix, input);
    // This constructor is only called from driver-side (i.e. not in
    // map/reduce tasks), so the connection settings are obtained from
    // the mrgeo.conf file.
    getConnectionInfo(); // initializes connectionInfo if needed
    this.providerProperties = providerProperties;
  }

  public static GeoWaveConnectionInfo getConnectionInfo()
  {
    if (connectionInfo == null)
    {
      connectionInfo = GeoWaveConnectionInfo.load();
    }
    return connectionInfo;
  }

  static void setConnectionInfo(GeoWaveConnectionInfo connInfo)
  {
    connectionInfo = connInfo;
  }

  public DataStatisticsStore getStatisticsStore() throws AccumuloSecurityException, AccumuloException, IOException
  {
    String namespace = getNamespace();
    initDataSource(namespace);
    DataSourceEntry entry = getDataSourceEntry(namespace);
    return entry.statisticsStore;
  }

  public Index getIndex() throws AccumuloSecurityException, AccumuloException, IOException
  {
    String namespace = getNamespace();
    initDataSource(namespace);
    DataSourceEntry entry = getDataSourceEntry(namespace);
    return entry.index;
  }

  public String getGeoWaveResourceName() throws IOException
  {
    ParseResults results = parseResourceName(getResourceName());
    return results.name;
  }

  public String getNamespace() throws IOException
  {
    ParseResults results = parseResourceName(getResourceName());
    return results.namespace;
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

  public com.vividsolutions.jts.geom.Geometry getSpatialConstraint() throws AccumuloSecurityException, AccumuloException, IOException
  {
    init();
    return spatialConstraint;
  }

  public Date getStartTimeConstraint() throws AccumuloSecurityException, AccumuloException, IOException
  {
    init();
    return startTimeConstraint;
  }

  public Date getEndTimeConstraint() throws AccumuloSecurityException, AccumuloException, IOException
  {
    init();
    return endTimeConstraint;
  }

  /**
   * Parses the input string into the optional namespace, the name of the input and
   * the optional query string
   * that accompanies it. The two strings are separated by a semi-colon, and the query
   * should be included in double quotes if it contains any semi-colons or square brackets.
   * But the double quotes are not required otherwise.
   *
   * @param input
   * @return
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
        results.namespace = input.substring(0, dotIndex);
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
        results.namespace = input.substring(0, dotIndex);
        results.name = input.substring(dotIndex + 1);
      }
      else
      {
        results.name = input;
      }
    }
    // If there is no namespace explicitly in the resource name, then we
    // check to make sure the GeoWave configuration for MrGeo only has one
    // namespace specified and use it. If there are multiple namespaces
    // configured, then throw an exception.
    if (results.namespace == null || results.namespace.isEmpty())
    {
      GeoWaveConnectionInfo connectionInfo = getConnectionInfo();
      String[] namespaces = connectionInfo.getNamespaces();
      if (namespaces == null || namespaces.length == 0)
      {
        throw new IOException("Missing missing " + GeoWaveConnectionInfo.GEOWAVE_NAMESPACES_KEY +
                              " in the MrGeo configuration");
      }
      else
      {
        if (namespaces.length > 1)
        {
          throw new IOException(
                  "You must specify a GeoWave namespace for " + input +
                  " because multiple GeoWave namespaces are configured in MrGeo (e.g." +
                  " MyNamespace." + input + ")");
        }
        results.namespace = namespaces[0];
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
    try
    {
      init(results);
    }
    catch (AccumuloSecurityException e)
    {
      throw new IOException("AccumuloSecurityException in GeoWave data provider getVectorReader", e);
    }
    catch (AccumuloException e)
    {
      throw new IOException("AccumuloException in GeoWave data provider getVectorReader", e);
    }
    DataSourceEntry entry = getDataSourceEntry(results.namespace);
    Query query = new BasicQuery(new BasicQuery.Constraints());
    GeoWaveVectorReader reader = new GeoWaveVectorReader(results.namespace, entry.dataStore,
        entry.adapterStore.getAdapter(new ByteArrayId(this.getGeoWaveResourceName())),
        query, entry.index, filter, providerProperties);
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
  public RecordReader<FeatureIdWritable, Geometry> getRecordReader()
  {
    return new GeoWaveVectorRecordReader();
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
    initConnectionInfo(conf);
    return (connectionInfo != null);
  }

  public static boolean isValid()
  {
    initConnectionInfo();
    return (connectionInfo != null);
  }

  public static String[] listVectors(final ProviderProperties providerProperties) throws AccumuloException, AccumuloSecurityException, IOException
  {
    initConnectionInfo();
    List<String> results = new ArrayList<String>();
    for (String namespace: connectionInfo.getNamespaces())
    {
      initDataSource(namespace);
      DataSourceEntry entry = getDataSourceEntry(namespace);
      CloseableIterator<DataAdapter<?>> iter = entry.adapterStore.getAdapters();
      try
      {
        while (iter.hasNext())
        {
          DataAdapter<?> adapter = iter.next();
          if (adapter != null)
          {
            ByteArrayId adapterId = adapter.getAdapterId();
            if (checkAuthorizations(adapterId, namespace, providerProperties))
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
                                ProviderProperties providerProperties) throws AccumuloException, AccumuloSecurityException, IOException
  {
    initConnectionInfo();
    ParseResults results = parseResourceName(input);
    try
    {
      initDataSource(results.namespace);
      DataSourceEntry entry = getDataSourceEntry(results.namespace);
      ByteArrayId adapterId = new ByteArrayId(results.name);
      DataAdapter<?> adapter = entry.adapterStore.getAdapter(adapterId);
      if (adapter == null)
      {
        return false;
      }
      return checkAuthorizations(adapterId, results.namespace, providerProperties);
    }
    catch(IllegalArgumentException e)
    {
      log.info("Unable to open " + input + " with the GeoWave data provider: " + e.getMessage());
    }
    return false;
  }

  private static boolean checkAuthorizations(ByteArrayId adapterId,
                                             String namespace,
      ProviderProperties providerProperties) throws IOException, AccumuloException, AccumuloSecurityException
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
          throws IOException, AccumuloException, AccumuloSecurityException
  {
    initConnectionInfo();
    initDataSource(namespace);
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

  private void init() throws AccumuloSecurityException, AccumuloException, IOException
  {
    // Don't initialize more than once.
    if (initialized)
    {
      return;
    }
    ParseResults results = parseResourceName(getResourceName());
    init(results);
  }

  private void init(ParseResults results) throws AccumuloSecurityException, AccumuloException, IOException
  {
    // Don't initialize more than once.
    if (initialized)
    {
      return;
    }
    initialized = true;
    // Extract the GeoWave adapter name and optional CQL string
    DataSourceEntry entry = getDataSourceEntry(results.namespace);
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
    for (String keyName : settings.keySet())
    {
      if (keyName != null && !keyName.isEmpty())
      {
        String value = settings.get(keyName);
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

  private static void initDataSource(String namespace) throws AccumuloException, AccumuloSecurityException, IOException
  {
    DataSourceEntry entry = dataSourceEntries.get(namespace);
    if (entry == null)
    {
      entry = new DataSourceEntry();
      dataSourceEntries.put(namespace, entry);
      entry.storeOperations = new BasicAccumuloOperations(
          connectionInfo.getZookeeperServers(),
          connectionInfo.getInstanceName(),
          connectionInfo.getUserName(),
          connectionInfo.getPassword(),
          namespace);
      final AccumuloIndexStore indexStore = new AccumuloIndexStore(
          entry.storeOperations);
  
      entry.statisticsStore = new AccumuloDataStatisticsStoreExt(
          entry.storeOperations);
      entry.adapterStore = new AccumuloAdapterStore(
          entry.storeOperations);
      entry.dataStore = new VectorDataStore(
          indexStore,
          entry.adapterStore,
          entry.statisticsStore,
          entry.storeOperations);
      CloseableIterator<Index> indices = entry.dataStore.getIndices();
      try
      {
        if (indices.hasNext())
        {
          entry.index = indices.next();
          if (log.isDebugEnabled())
          {
            log.debug("Found GeoWave index " + entry.index.getId().getString());
//            log.debug("  index type = " + entry.index.getDimensionalityType().toString());
            DimensionField<? extends CommonIndexValue>[] dimFields = entry.index.getIndexModel().getDimensions();
            for (DimensionField<? extends CommonIndexValue> dimField : dimFields)
            {
              log.debug("  Dimension field: " + dimField.getFieldId().getString());
            }
          }
        }
      }
      finally
      {
        indices.close();
      }
    }
  }

  static class DataSourceEntry
  {
    public DataSourceEntry()
    {
    }

    private AccumuloOperations storeOperations;
    private AdapterStore adapterStore;
    private DataStatisticsStore statisticsStore;
    private VectorDataStore dataStore;
    private Index index;
  }
}
