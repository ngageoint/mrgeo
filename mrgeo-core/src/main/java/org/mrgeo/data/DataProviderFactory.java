/*
 * Copyright 2009-2016 DigitalGlobe, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 *
 */

package org.mrgeo.data;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.data.adhoc.AdHocDataProvider;
import org.mrgeo.data.adhoc.AdHocDataProviderFactory;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.data.image.MrsImageDataProviderFactory;
import org.mrgeo.data.vector.VectorDataProvider;
import org.mrgeo.data.vector.VectorDataProviderFactory;
import org.mrgeo.utils.DependencyLoader;
import org.mrgeo.utils.HadoopUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * This class is responsible for creating instances of several types of data providers.
 * A data provider is an abstraction that provides read/write access to data in a
 * back-end data store without knowing the actual type of the back-end data store
 * (e.g. HDFS, Accumulo, ...). There is an abstract data provider class for each
 * type of data to be accessed, including AdHoc data, and MrsImage data.
 * These are described below. Each instance of a data provider returned by the methods
 * in this class are for a specific piece of data (like a java.io.File instance references
 * a specific file on the disk). Data providers are implemented in data plugins
 * and the providers are discovered dynamically at runtime through the Java
 * ServiceLoader.
 * <p>
 * MrsImage data providers access a MrGeo image (referred to as a MrsImage). A MrsImage
 * data provider is for writing to a MrsImage.
 * <p>
 * AdHoc data providers are used for accessing other types of data besides raw
 * images and MrsImages. This data provider is used within MrGeo while performing
 * processing in order to save state as it works.
 * <p>
 * Vector data providers access a vector data source.
 */
public class DataProviderFactory
{
/**
 * The key for the caller's user name that MrGeo will include in provider properties
 * objects passed to methods in this API. If the MrGeo installation is not secured,
 * then this property will not be included in provider properties, and provider
 * properties may be null.
 */
public static final String PROVIDER_PROPERTY_USER_NAME = "mrgeo.security.user.name";
/**
 * The key for the caller's security roles (comma-delimited) that MrGeo will include
 * in provider properties objects passed to methods in this API. If the MrGeo
 * installation is not secured, then this property will not be included in provider
 * properties, and provider properties may be null.
 */
public static final String PROVIDER_PROPERTY_USER_ROLES = "mrgeo.security.user.roles";
final static String PREFERRED_PROPERTYNAME = "preferred.provider";
final static String PREFERRED_ADHOC_PROPERTYNAME = "preferred.adhoc.provider";
final static String PREFERRED_MRSIMAGE_PROPERTYNAME = "preferred.image.provider";
final static String PREFERRED_VECTOR_PROPERTYNAME = "preferred.vector.provider";

final static String BASECLASS = DataProviderFactory.class.getSimpleName() + ".";
final static String PREFERRED_ADHOC_PROVIDER_NAME = BASECLASS + PREFERRED_ADHOC_PROPERTYNAME;
final static String PREFERRED_MRSIMAGE_PROVIDER_NAME = BASECLASS + PREFERRED_MRSIMAGE_PROPERTYNAME;
final static String PREFERRED_VECTOR_PROVIDER_NAME = BASECLASS + PREFERRED_VECTOR_PROPERTYNAME;

final static String DATA_PROVIDER_CONFIG_PREFIX = BASECLASS + "config.";

private final static String PREFIX_CHAR = ":"; // use ":" for the prefix delimiter
private final static int PROVIDER_CACHE_SIZE = 50;
private final static int PROVIDER_CACHE_EXPIRE = 10; // minutes
static Logger log = LoggerFactory.getLogger(DataProviderFactory.class);
static String preferredAdHocProviderName = null;
static String preferredImageProviderName = null;
static String preferredVectorProviderName = null;
private static Configuration basicConf;
private static Map<String, String> configSettings;
private static Cache<String, AdHocDataProvider> adHocProviderCache = CacheBuilder
    .newBuilder().maximumSize(PROVIDER_CACHE_SIZE).expireAfterAccess(
        PROVIDER_CACHE_EXPIRE, TimeUnit.MINUTES).removalListener(
        new RemovalListener<String, AdHocDataProvider>()
        {
          @Override
          public void onRemoval(final RemovalNotification<String, AdHocDataProvider> notification)
          {
            log.debug("resource cache removal: " + notification.getKey());
          }
        }).build();
private static Cache<String, MrsImageDataProvider> mrsImageProviderCache = CacheBuilder
    .newBuilder().maximumSize(PROVIDER_CACHE_SIZE).expireAfterAccess(
        PROVIDER_CACHE_EXPIRE, TimeUnit.MINUTES).removalListener(
        new RemovalListener<String, MrsImageDataProvider>()
        {
          @Override
          public void onRemoval(final RemovalNotification<String, MrsImageDataProvider> notification)
          {
            log.debug("resource cache removal: " + notification.getKey());
          }
        }).build();
private static Cache<String, VectorDataProvider> vectorProviderCache = CacheBuilder
    .newBuilder().maximumSize(PROVIDER_CACHE_SIZE).expireAfterAccess(
        PROVIDER_CACHE_EXPIRE, TimeUnit.MINUTES).removalListener(
        new RemovalListener<String, VectorDataProvider>()
        {
          @Override
          public void onRemoval(final RemovalNotification<String, VectorDataProvider> notification)
          {
            log.debug("resource cache removal: " + notification.getKey());
          }
        }).build();
private static Map<String, AdHocDataProviderFactory> adHocProviderFactories;
private static Map<String, MrsImageDataProviderFactory> mrsImageProviderFactories;
private static Map<String, VectorDataProviderFactory> vectorProviderFactories;

public static void saveProviderPropertiesToConfig(final ProviderProperties providerProperties,
    final Configuration conf)
{
  log.debug("Saving provider properties to config");
  if (providerProperties != null)
  {
    conf.set(PROVIDER_PROPERTY_USER_NAME, providerProperties.getUserName());
    conf.set(PROVIDER_PROPERTY_USER_ROLES, StringUtils.join(providerProperties.getRoles(), ","));
  }
  // Also, we want to save the configuration settings for each data provider
  // in the Configuration as well so they can be re-instantiated on the remote
  // side of a map/reduce 1 job.
  Map<String, String> configSettings = getConfigurationFromProviders();
  log.debug("Saving " + configSettings.size() + " configuration settings from data providers to config");
  for (Map.Entry<String, String> es : configSettings.entrySet())
  {
    conf.set(DATA_PROVIDER_CONFIG_PREFIX + es.getKey(), es.getValue());
  }
}

public static ProviderProperties loadProviderPropertiesFromConfig(Configuration conf)
{
  // Tell each data provider to load their config settings from the Configuration.
  // This is the inverse operation to saveProviderPropertiesToConfig.
  Iterator<Map.Entry<String, String>> iter = conf.iterator();
  Map<String, String> configSettings = new HashMap<String, String>();
  int prefixLen = DATA_PROVIDER_CONFIG_PREFIX.length();
  while (iter.hasNext())
  {
    Map.Entry<String, String> entry = iter.next();
    if (entry.getKey().startsWith(DATA_PROVIDER_CONFIG_PREFIX))
    {
      configSettings.put(entry.getKey().substring(prefixLen),
          entry.getValue());
    }
  }
  setConfigurationForProviders(configSettings);

  String userName = conf.get(PROVIDER_PROPERTY_USER_NAME, "");
  List<String> roles = new ArrayList<String>();
  String strRoles = conf.get(PROVIDER_PROPERTY_USER_ROLES, "");
  if (strRoles != null && !strRoles.isEmpty())
  {
    String[] separated = strRoles.split(",");
    for (String r : separated)
    {
      roles.add(r);
    }
  }
  return new ProviderProperties(userName, roles);
}

public static Map<String, String> getConfigurationFromProviders()
{
  Map<String, String> result = new HashMap<String, String>();
  try
  {
    initialize(getBasicConfig());
  }
  catch (DataProviderException e)
  {
    log.error("Unable to initialize data providers", e);
    return result;
  }

  if (adHocProviderFactories != null)
  {
    for (final AdHocDataProviderFactory dpf : adHocProviderFactories.values())
    {
      Map<String, String> p = dpf.getConfiguration();
      if (p != null)
      {
        log.debug("Got " + p.size() + " config settings from " + dpf.getClass().getName());
        result.putAll(p);
      }
      else
      {
        log.debug("Got no config settings from " + dpf.getClass().getName());
      }
    }
  }

  if (mrsImageProviderFactories != null)
  {
    for (final MrsImageDataProviderFactory dpf : mrsImageProviderFactories.values())
    {
      Map<String, String> p = dpf.getConfiguration();
      if (p != null)
      {
        log.debug("Got " + p.size() + " config settings from " + dpf.getClass().getName());
        result.putAll(p);
      }
      else
      {
        log.debug("Got no config settings from " + dpf.getClass().getName());
      }
    }
  }
  if (vectorProviderFactories != null)
  {
    for (final VectorDataProviderFactory dpf : vectorProviderFactories.values())
    {
      Map<String, String> p = dpf.getConfiguration();
      if (p != null)
      {
        log.debug("Got " + p.size() + " config settings from " + dpf.getClass().getName());
        result.putAll(p);
      }
      else
      {
        log.debug("Got no config settings from " + dpf.getClass().getName());
      }
    }
  }
  return result;
}

public static void setConfigurationForProviders(Map<String, String> properties)
{
  if (log.isInfoEnabled())
  {
    if (properties != null)
    {
      log.debug("Config settings passed to all data providers has size " + properties.size());
    }
    else
    {
      log.debug("Config settings passed to all data providers is empty");
    }
  }
  configSettings = properties;
}

/**
 * Create a data provider for a new ad hoc data source with a randomly generated
 * name. Use this method if you don't care what the name of the data source is.
 * This is useful for storing temporary data that will be accessed during
 * processing, and then deleted after processing completes. This method should
 * be called from the name node side, not from mappers or reducers.
 * <p>
 * TODO: Add information about how it chooses the appropriate provider for the
 * name passed in.
 * <p>
 * TODO: When ad hoc data is implemented in Accumulo, this method will need to
 * be changed so that it receives the providerProperties argument just like
 * the getMrsImageDataProvider method.
 *
 * @return An ad hoc data provider for a newly created, randomly named resource.
 * @throws DataProviderNotFound
 */
public static AdHocDataProvider createAdHocDataProvider(final ProviderProperties providerProperties)
    throws DataProviderNotFound, DataProviderException
{
  return createAdHocDataProvider(getBasicConfig(), providerProperties);
}

/**
 * Create a data provider for a new ad hoc data source with a randomly generated
 * name. Use this method if you don't care what the name of the data source is.
 * This is useful for storing temporary data that will be accessed during
 * processing, and then deleted after processing completes. This method should
 * be called from the data node side (inside mappers and reducers).
 * <p>
 * TODO: Add information about how it chooses the appropriate provider for the
 * name passed in.
 *
 * @return An ad hoc data provider for a newly created, randomly named resource.
 * @throws DataProviderNotFound
 */
public static AdHocDataProvider createAdHocDataProvider(final Configuration conf)
    throws DataProviderNotFound, DataProviderException
{
  return createAdHocDataProvider(conf, loadProviderPropertiesFromConfig(conf));
}

/**
 * Create a data provider for a specifically named ad hoc data source. Use this
 * method if you need an ad hoc data source with a name that you assign. This
 * would be used for accessing/storing named data that is not raw imagery being
 * ingested or a MrsImage.
 * <p>
 * TODO: Add information about how it chooses the appropriate provider for the
 * name passed in.
 * <p>
 * TODO: When ad hoc data is implemented in Accumulo, this method will need to
 * be changed so that it receives the providerProperties argument just like
 * the getMrsImageDataProvider method.
 *
 * @param name
 * @return
 * @throws DataProviderNotFound
 */
public static AdHocDataProvider getAdHocDataProvider(final String name,
    final AccessMode mode,
    final ProviderProperties providerProperties) throws DataProviderNotFound
{
  return getAdHocDataProvider(name, mode, getBasicConfig(), providerProperties);
}

public static AdHocDataProvider getAdHocDataProvider(final String name,
    final AccessMode mode,
    final Configuration conf) throws DataProviderNotFound
{
  return getAdHocDataProvider(name, mode, conf,
      loadProviderPropertiesFromConfig(conf));
}

// public static TileDataProvider getDataProvider(final String name) throws DataProviderNotFound
// {
// initialize();
// for (TileDataProvider dp : mrsImageProviders)
// {
// if (dp.canOpen(name))
// {
// return dp;
// }
// }
// throw new DataProviderNotFound("Unable to find a data provider for " + name);
// }

/**
 * Returns a list of MrsImages available from all data sources. The names returned
 * can be subsequently passed as the name parameter to getMrsImageDataProvider().
 *
 * @return
 * @throws IOException
 */
public static String[] listImages(final ProviderProperties providerProperties) throws IOException
{
  initialize(getBasicConfig());
  List<String> results = new ArrayList<String>();
  for (final MrsImageDataProviderFactory factory : mrsImageProviderFactories.values())
  {
    String[] images = factory.listImages(providerProperties);
    if (images != null && images.length > 0)
    {
      results.addAll(Arrays.asList(images));
    }
  }
  String[] returnValue = new String[results.size()];
  return results.toArray(returnValue);
}

/**
 * Returns a list of vectors available from all data sources. The names returned
 * can be subsequently passed as the name parameter to getVectorDataProvider().
 *
 * @return
 * @throws IOException
 */
public static String[] listVectors(final ProviderProperties providerProperties) throws IOException
{
  initialize(getBasicConfig());
  List<String> results = new ArrayList<String>();
  for (final VectorDataProviderFactory factory : vectorProviderFactories.values())
  {
    String[] vectors = factory.listVectors(providerProperties);
    if (vectors != null && vectors.length > 0)
    {
      results.addAll(Arrays.asList(vectors));
    }
  }
  String[] returnValue = new String[results.size()];
  return results.toArray(returnValue);
}

public static MrsImageDataProvider createTempMrsImageDataProvider(ProviderProperties props)
    throws DataProviderNotFound, DataProviderException
{
  return createTempMrsImageDataProvider(getBasicConfig(), props);
}

public static MrsImageDataProvider createTempMrsImageDataProvider(Configuration conf)
    throws DataProviderNotFound, DataProviderException
{
  return createTempMrsImageDataProvider(conf, loadProviderPropertiesFromConfig(conf));
}

/**
 * Create a data provider for a MrsImage data resource. This method should be
 * called to access a MrsImage (an image ingested into MrGeo) as well as
 * metadata about a MrsImage. Do not call this method from code that runs on
 * the data node size of Hadoop. Use the other signature that accepts a
 * Configuration parameter in that case.
 * <p>
 * TODO: Add information about how it chooses the appropriate provider for the
 * name passed in.
 *
 * @param name
 * @param accessMode
 * @return
 * @throws DataProviderNotFound
 */
public static MrsImageDataProvider getMrsImageDataProvider(final String name,
    AccessMode accessMode,
    ProviderProperties props) throws DataProviderNotFound
{
  return getMrsImageDataProvider(name, accessMode, getBasicConfig(), props);
}

public static MrsImageDataProvider getMrsImageDataProviderNoCache(final String name,
    AccessMode accessMode,
    ProviderProperties props) throws DataProviderNotFound
{

  try
  {
    return new MrsImageLoader(name, accessMode, getBasicConfig(), props).call();
  }
  catch (Exception e)
  {
    throw new DataProviderNotFound("Error loading " + name, e);
  }
}

/**
 * Create a data provider for a MrsImage data resource. This method should be
 * called to access a MrsImage (an image ingested into MrGeo) as well as
 * metadata about a MrsImage. Call this method when you need to get a data
 * provider from within a mapper or reducer or any other functionality that
 * runs on the data node side of Hadoop.
 * <p>
 * TODO: Add information about how it chooses the appropriate provider for the
 * name passed in.
 *
 * @param name
 * @param accessMode
 * @param conf
 * @return
 * @throws DataProviderNotFound
 */
public static MrsImageDataProvider getMrsImageDataProvider(final String name,
    AccessMode accessMode,
    final Configuration conf) throws DataProviderNotFound
{
  return getMrsImageDataProvider(name, accessMode, conf,
      loadProviderPropertiesFromConfig(conf));
}

/**
 * Create a data provider for a vector data resource. This method should be
 * called to access a vector as well as metadata about a vector.
 *
 * @param name
 * @return
 * @throws DataProviderNotFound
 */
public static VectorDataProvider getVectorDataProvider(final String name,
    AccessMode accessMode,
    ProviderProperties providerProperties) throws DataProviderNotFound
{
  return getVectorDataProvider(name, accessMode, getBasicConfig(), providerProperties);
}

public static VectorDataProvider getVectorDataProvider(final String name,
    AccessMode accessMode, final Configuration conf) throws DataProviderNotFound
{
  return getVectorDataProvider(name, accessMode, conf,
      loadProviderPropertiesFromConfig(conf));
}

/**
 * Previously requested data providers are stored in a cache to speed the process
 * of repeated requests for a data provider for the same resource. Call this
 * method to force the data providers to be newly created the next time they are
 * requested.
 */
public static void invalidateCache()
{
  adHocProviderCache.invalidateAll();
  mrsImageProviderCache.invalidateAll();
  vectorProviderCache.invalidateAll();
}

/**
 * Similar to invalidateCache(), except it invalidates a specific resource instead
 * of all resources.
 *
 * @param resource
 */
public static void invalidateCache(final String resource)
{
  if (resource != null && !resource.isEmpty())
  {
    adHocProviderCache.invalidate(resource);
    mrsImageProviderCache.invalidate(resource);
    vectorProviderCache.invalidate(resource);

    log.debug("invalidating cache: " + resource);
  }
}

/**
 * Deletes the specified resource.
 *
 * @param resource
 * @param providerProperties
 * @throws IOException
 */
public static void delete(final String resource,
    final ProviderProperties providerProperties) throws IOException
{
  MrsImageDataProvider mrsImageProvider = getMrsImageDataProvider(resource,
      AccessMode.OVERWRITE, providerProperties);
  if (mrsImageProvider != null)
  {
    mrsImageProvider.delete();
    mrsImageProviderCache.invalidate(resource);
    return;
  }

  AdHocDataProvider adHocProvider = getAdHocDataProvider(resource, AccessMode.OVERWRITE,
      providerProperties);
  if (adHocProvider != null)
  {
    adHocProvider.delete();
    adHocProviderCache.invalidate(resource);
    return;
  }

  VectorDataProvider vectorProvider = getVectorDataProvider(resource, AccessMode.OVERWRITE,
      providerProperties);
  if (vectorProvider != null)
  {
    vectorProvider.delete();
    vectorProviderCache.invalidate(resource);
    return;
  }
}

public static void addDependencies(final Configuration conf) throws IOException
{
  if (adHocProviderFactories != null)
  {
    for (final AdHocDataProviderFactory dp : adHocProviderFactories.values())
    {
      DependencyLoader.addDependencies(conf, dp.getClass());
    }
  }

  if (mrsImageProviderFactories != null)
  {
    for (final MrsImageDataProviderFactory dp : mrsImageProviderFactories.values())
    {
      DependencyLoader.addDependencies(conf, dp.getClass());
    }
  }
  if (vectorProviderFactories != null)
  {
    for (final VectorDataProviderFactory dp : vectorProviderFactories.values())
    {
      DependencyLoader.addDependencies(conf, dp.getClass());
    }
  }
}

public static Set<String> getDependencies() throws IOException
{
  log.debug("Getting dependencies for all providers");
  initialize(getBasicConfig());
  Set<String> dependencies = new HashSet<String>();
  if (adHocProviderFactories != null)
  {
    for (final AdHocDataProviderFactory dp : adHocProviderFactories.values())
    {
      log.debug("Getting dependencies for " + dp.getClass().getName());
      dependencies.addAll(DependencyLoader.getDependencies(dp.getClass()));
    }
  }

  if (mrsImageProviderFactories != null)
  {
    for (final MrsImageDataProviderFactory dp : mrsImageProviderFactories.values())
    {
      log.debug("Getting dependencies for " + dp.getClass().getName());
      dependencies.addAll(DependencyLoader.getDependencies(dp.getClass()));
    }
  }
  if (vectorProviderFactories != null)
  {
    for (final VectorDataProviderFactory dp : vectorProviderFactories.values())
    {
      log.debug("Getting dependencies for " + dp.getClass().getName());
      dependencies.addAll(DependencyLoader.getDependencies(dp.getClass()));
    }
  }
  return dependencies;
}

protected synchronized static void initialize(final Configuration conf) throws DataProviderException
{
  if (adHocProviderFactories == null)
  {
    log.info("Initializing ad hoc provider factories");
    adHocProviderFactories = new HashMap<String, AdHocDataProviderFactory>();
    // Find the mrsImageProviders
    final ServiceLoader<AdHocDataProviderFactory> dataProviderLoader = ServiceLoader
        .load(AdHocDataProviderFactory.class);
    for (final AdHocDataProviderFactory dp : dataProviderLoader)
    {
      if (configSettings != null)
      {
        dp.setConfiguration(configSettings);
      }
      if (dp.isValid())
      {
        log.info("Found ad hoc data provider factory " + dp.getClass().getName());
        adHocProviderFactories.put(dp.getPrefix(), dp);
        dp.initialize(conf);
      }
      else
      {
        log.info("Skipping ad hoc data provider " + dp.getClass().getName() +
            " because isValid returned false");
      }
    }
  }

  if (mrsImageProviderFactories == null)
  {
    log.info("Initializing image provider factories");

    mrsImageProviderFactories = new HashMap<String, MrsImageDataProviderFactory>();

    // Find the mrsImageProviders
    final ServiceLoader<MrsImageDataProviderFactory> dataProviderLoader = ServiceLoader
        .load(MrsImageDataProviderFactory.class);
    for (final MrsImageDataProviderFactory dp : dataProviderLoader)
    {
      try
      {
        if (configSettings != null)
        {
          dp.setConfiguration(configSettings);
        }
        if (dp.isValid())
        {
          log.info("Found mrs image data provider factory {} {}", dp.getPrefix(), dp.getClass().getName());
          mrsImageProviderFactories.put(dp.getPrefix(), dp);
          dp.initialize(conf);
        }
        else
        {
          log.info("Skipping mrs image data provider " + dp.getClass().getName() +
              " because isValid returned false");
        }
      }
      catch (Exception e)
      {
        // no op, just won't put the provider in the list
        log.warn("Ignoring " + dp.getClass().getName(), e);
      }
    }
  }
  if (vectorProviderFactories == null)
  {
    log.info("Initializing vector provider factories");

    boolean debugEnabled = log.isDebugEnabled();
    vectorProviderFactories = new HashMap<String, VectorDataProviderFactory>();

    if (debugEnabled)
    {
      log.debug("Finding vector provider factories");
    }
    // Find the vectorProviders
    final ServiceLoader<VectorDataProviderFactory> dataProviderLoader = ServiceLoader
        .load(VectorDataProviderFactory.class);
    int count = 0;
    for (final VectorDataProviderFactory dp : dataProviderLoader)
    {
      try
      {
        if (debugEnabled)
        {
          log.debug("Checking if vector factory is valid: " + dp.getClass().getName() + " with config " +
              ((conf == null) ? "null" : "not null"));
        }
        if (configSettings != null)
        {
          dp.setConfiguration(configSettings);
        }
        if (dp.isValid())
        {
          if (debugEnabled)
          {
            log.debug("Factory " + dp.getClass().getName() + " is valid, uses prefix: " + dp.getPrefix());
          }
          vectorProviderFactories.put(dp.getPrefix(), dp);
          dp.initialize(conf);
          count++;
        }
        else
        {
          if (debugEnabled)
          {
            log.debug("Factory " + dp.getClass().getName() + " is NOT valid, uses prefix: " + dp.getPrefix());
          }
          log.info("Skipping vector data provider " + dp.getClass().getName() +
              " because isValid returned false");
        }
      }
      catch (Exception e)
      {
        log.warn("Skipping vector factory provider " + dp.getClass().getName() + " due to exception", e);
      }
    }
    if (count == 0)
    {
      log.warn("No vector factory providers were found");
    }
  }

  findPreferredProvider(conf);
}

@SuppressWarnings("squid:S1166") // Exception caught and handled
protected static String getPrefix(String name)
{
  int ndx = name.indexOf(PREFIX_CHAR);
  if (ndx > 0)
  {
    // 1st check if the system see's the name as a valid URI
    try
    {
      new URL(name).toURI();
      return null;
    }
    catch (URISyntaxException | MalformedURLException ignored)
    {
      // no op
    }

    // now check for the :// part (usually for URI's not added to the system, like hdfs://, or s3://
    int afterPrefixIndex = ndx + PREFIX_CHAR.length();
    // If the prefix character is a colon, we need to make sure that the name is
    // not actually a URL. So if the name has at least two more characters, and
    // the two characters immediately following the colon are //, then we return
    // no prefix because we assume it's a URL.
    if ((PREFIX_CHAR.equals(":")) && (name.length() > afterPrefixIndex + 3) &&
        (name.charAt(afterPrefixIndex) == '/') && (name.charAt(afterPrefixIndex + 1) == '/'))
    {
      return null;
    }
    else
    {
      return name.substring(0, ndx);
    }
  }
  return null;
}

private static AdHocDataProvider createAdHocDataProvider(final Configuration conf,
    final ProviderProperties props) throws DataProviderNotFound, DataProviderException
{
  initialize(conf);
  for (final AdHocDataProviderFactory factory : adHocProviderFactories.values())
  {
    AdHocDataProvider provider;
    try
    {
      provider = factory.createAdHocDataProvider(props);
    }
    catch (IOException e)
    {
      throw new DataProviderException("Can not create ad hoc data provider", e);
    }
    adHocProviderCache.put(provider.getResourceName(), provider);
    return provider;
  }
  throw new DataProviderNotFound("Unable to find an ad hoc data provider for ");
}

@SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST_OF_RETURN_VALUE", justification = "No, it's not.")
private static AdHocDataProvider getAdHocDataProvider(final String name,
    final AccessMode mode,
    final Configuration conf,
    final ProviderProperties props) throws DataProviderNotFound
{
  try
  {
    // Make sure that ad hoc resources are cached uniquely per user
    String cacheKey = getResourceCacheKey(name, conf, props);
    if (mode == AccessMode.OVERWRITE || mode == AccessMode.WRITE)
    {
      invalidateCache(cacheKey);
    }
    return adHocProviderCache.get(cacheKey,
        new AdHocLoader(name, mode, conf, props));
  }
  catch (ExecutionException e)
  {
    if (e.getCause() instanceof DataProviderNotFound)
    {
      throw (DataProviderNotFound) e.getCause();
    }
    throw new DataProviderNotFound(e);
  }
}

// Be sure that provider caching is unique for different users so that we're
// not using the wrong user credentials. On the server side, we include the
// calling user name in the key. On the data node side, there is no need to
// do this because the cache only contains providers used for one job, which
// is executed for one user.
private static String getResourceCacheKey(final String resourceName,
    final Configuration conf,
    final ProviderProperties providerProperties)
{
  if (providerProperties != null)
  {
    String userName = providerProperties.getUserName();
    if (userName != null && !userName.isEmpty())
    {
      return resourceName + "," + userName;
    }
  }
  return resourceName;
}

private static MrsImageDataProvider createTempMrsImageDataProvider(final Configuration conf,
    final ProviderProperties providerProperties) throws DataProviderNotFound, DataProviderException
{

  initialize(conf);
  for (final MrsImageDataProviderFactory factory : mrsImageProviderFactories.values())
  {
    MrsImageDataProvider provider;
    try
    {
      provider = factory.createTempMrsImageDataProvider(providerProperties);
    }
    catch (IOException e)
    {
      throw new DataProviderException("Can not create temporary mrs image data provider", e);
    }

    mrsImageProviderCache.put(provider.getResourceName(), provider);
    return provider;
  }

  throw new DataProviderException("Can not create temporary mrs image data provider");
}

@SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST_OF_RETURN_VALUE", justification = "We _are_ checking!")
private static MrsImageDataProvider getMrsImageDataProvider(final String name,
    AccessMode accessMode,
    final Configuration conf,
    final ProviderProperties providerProperties) throws DataProviderNotFound
{
  try
  {
    // Make sure that image resources are cached uniquely by user
    String cacheKey = getResourceCacheKey(name, conf, providerProperties);
    // If a resource was already accessed in read mode, and then again in
    // OVERWRITE or WRITE mode, then force the cache to re-load the resource
    // to execute validation beforehand
    if (accessMode == AccessMode.OVERWRITE || accessMode == AccessMode.WRITE)
    {
      mrsImageProviderCache.invalidate(cacheKey);
    }
    if (log.isDebugEnabled())
    {
      log.debug("Loading from mrsImageProviderCache");
      log.debug("   cacheKey: {}", cacheKey);
      log.debug("   name: {}", name);
      log.debug("   accessMode: {}", accessMode.name());
      log.debug("   conf: {}", conf);
      log.debug("   provider properties: {}", providerProperties);
    }

    MrsImageLoader loader = new MrsImageLoader(name, accessMode, conf, providerProperties);

    return mrsImageProviderCache.get(cacheKey, loader);

//    return mrsImageProviderCache.get(cacheKey,
//        new MrsImageLoader(name, accessMode, conf, providerProperties));
  }
  catch (ExecutionException e)
  {
    if (e.getCause() instanceof DataProviderNotFound)
    {
      throw (DataProviderNotFound) e.getCause();
    }
    throw new DataProviderNotFound(e);
  }
}

@SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST_OF_RETURN_VALUE", justification = "We _are_ checking!")
private static VectorDataProvider getVectorDataProvider(final String name,
    AccessMode accessMode,
    final Configuration conf,
    final ProviderProperties providerProperties) throws DataProviderNotFound
{
  try
  {
    // Make sure that vector resources are cached uniquely by user
    String cacheKey = getResourceCacheKey(name, conf, providerProperties);
    // If a resource was already accessed in read mode, and then again in
    // OVERWRITE or WRITE mode, then force the cache to re-load the resource
    // to execute validation beforehand
    if (accessMode == AccessMode.OVERWRITE || accessMode == AccessMode.WRITE)
    {
      vectorProviderCache.invalidate(cacheKey);
    }
    return vectorProviderCache.get(cacheKey,
        new VectorLoader(name, accessMode, conf, providerProperties));
  }
  catch (ExecutionException e)
  {
    if (e.getCause() instanceof DataProviderNotFound)
    {
      throw (DataProviderNotFound) e.getCause();
    }
    throw new DataProviderNotFound(e);
  }
}

private static void findPreferredProvider(Configuration conf)
{
  preferredAdHocProviderName = findValue(conf, PREFERRED_ADHOC_PROVIDER_NAME, PREFERRED_ADHOC_PROPERTYNAME);
  // no preferred provider, use the 1st one...
  if (preferredAdHocProviderName == null)
  {
    for (final AdHocDataProviderFactory factory : adHocProviderFactories.values())
    {
      preferredAdHocProviderName = factory.getPrefix();

      setValue(preferredAdHocProviderName, conf, PREFERRED_ADHOC_PROVIDER_NAME, PREFERRED_ADHOC_PROPERTYNAME);
      log.info("Making {} preferred ad hoc provider ", preferredAdHocProviderName);
      break;
    }
  }
  else
  {
    log.debug("Using preferred ad hoc provider {}", preferredAdHocProviderName);
  }

  preferredImageProviderName = findValue(conf, PREFERRED_MRSIMAGE_PROVIDER_NAME, PREFERRED_MRSIMAGE_PROPERTYNAME);
  // no preferred provider, use the 1st one...
  if (preferredImageProviderName == null)
  {
    for (final MrsImageDataProviderFactory factory : mrsImageProviderFactories.values())
    {
      preferredImageProviderName = factory.getPrefix();
      setValue(preferredImageProviderName, conf, PREFERRED_MRSIMAGE_PROVIDER_NAME, PREFERRED_MRSIMAGE_PROPERTYNAME);

      log.info("Making {} preferred image provider ", preferredImageProviderName);

      break;
    }
  }
  else
  {
    log.debug("Using preferred image provider " + preferredImageProviderName);
  }


  preferredVectorProviderName = findValue(conf, PREFERRED_VECTOR_PROVIDER_NAME, PREFERRED_VECTOR_PROPERTYNAME);
  // no preferred provider, use the 1st one...
  if (preferredVectorProviderName == null)
  {
    for (final VectorDataProviderFactory factory : vectorProviderFactories.values())
    {
      preferredVectorProviderName = factory.getPrefix();
      setValue(preferredVectorProviderName, conf, PREFERRED_VECTOR_PROVIDER_NAME, PREFERRED_VECTOR_PROPERTYNAME);

      log.info("Making {} preferred vector provider ", preferredVectorProviderName);

      break;
    }
  }
  else
  {
    log.debug("Using preferred vector provider " + preferredVectorProviderName);
  }
}

private static String findValue(final Configuration conf, final String confName, final String propName)
{
  String name = null;

  // 1st look in the config
  if (conf != null)
  {
    name = conf.get(confName, null);
  }

  if (name == null)
  {
    Properties mp = MrGeoProperties.getInstance();
    if (mp != null)
    {
      name = MrGeoProperties.getInstance().getProperty(propName, null);
    }
    // look for the generic name
    if (name == null)
    {
      mp = MrGeoProperties.getInstance();
      if (mp != null)
      {
        name = MrGeoProperties.getInstance().getProperty(PREFERRED_PROPERTYNAME, null);
      }
    }

  }

  return name;
}

private static void setValue(String value, final Configuration conf, final String confName, final String propName)
{
  if (conf != null)
  {
    conf.set(confName, value);
  }

  Properties mp = MrGeoProperties.getInstance();
  if (mp != null)
  {
    MrGeoProperties.getInstance().setProperty(propName, value);
  }
}

private synchronized static Configuration getBasicConfig()
{
  if (basicConf == null)
  {
    basicConf = HadoopUtils.createConfiguration();
  }
  return basicConf;
}

public enum AccessMode
{
  READ, WRITE, OVERWRITE
}

private static class AdHocLoader implements Callable<AdHocDataProvider>
{
  private String prefix;
  private String name;
  private AccessMode accessMode;
  private Configuration conf;
  private ProviderProperties props;

  public AdHocLoader(final String name,
      final AccessMode accessMode,
      final Configuration conf,
      final ProviderProperties props)
  {
    this.conf = conf;
    this.props = props;
    this.prefix = getPrefix(name);
    if (prefix != null)
    {
      this.name = name.substring(this.prefix.length() + PREFIX_CHAR.length());
    }
    else
    {
      this.name = name;
    }
    this.accessMode = accessMode;
  }

  @Override
  public AdHocDataProvider call() throws Exception
  {
    initialize(conf);
    final AdHocDataProviderFactory factory = findFactory();
    if (accessMode == AccessMode.READ)
    {
      if (factory != null)
      {
        if (factory.canOpen(name, props))
        {
          return factory.createAdHocDataProvider(name, props);
        }
        else
        {
          log.info("Could not open " + name + " using factory " + factory.getClass().getName());
        }
      }
      throw new DataProviderNotFound("Unable to find an ad hoc data provider for " + name);
    }
    else if (accessMode == AccessMode.OVERWRITE)
    {
      if (factory != null)
      {
        if (factory.exists(name, props))
        {
          factory.delete(name, props);
        }
        return factory.createAdHocDataProvider(name, props);
      }
      return getPreferredProvider().createAdHocDataProvider(name, props);
    }
    else
    {
      if (factory != null)
      {
        if (factory.canWrite(name, props))
        {
          return factory.createAdHocDataProvider(name, props);
        }
        throw new DataProviderNotFound("Unable to find an ad hoc data provider for " + name);
      }
      return getPreferredProvider().createAdHocDataProvider(name, props);
    }

  }

  private AdHocDataProviderFactory getPreferredProvider() throws DataProviderNotFound
  {
    if (adHocProviderFactories.containsKey(preferredAdHocProviderName))
    {
      return adHocProviderFactories.get(preferredAdHocProviderName);
    }

    throw new DataProviderNotFound("No ad hoc data providers found ");
  }

  private AdHocDataProviderFactory findFactory() throws IOException
  {
    if (prefix != null)
    {
      if (adHocProviderFactories.containsKey(prefix))
      {
        return adHocProviderFactories.get(prefix);
      }
    }
    for (final AdHocDataProviderFactory factory : adHocProviderFactories.values())
    {
      if (factory.exists(name, props))
      {
        return factory;
      }
      log.debug("resource cache load: " + name);
    }

    return null;
  }
}

private static class MrsImageLoader implements Callable<MrsImageDataProvider>
{
  private String prefix;
  private String name;
  private AccessMode accessMode;
  private Configuration conf;
  private ProviderProperties props;

  public MrsImageLoader(final String name,
      final AccessMode accessMode,
      final Configuration conf,
      final ProviderProperties props)
  {
    this.conf = conf;
    this.props = props;
    this.prefix = getPrefix(name);
    if (prefix != null)
    {
      this.name = name.substring(this.prefix.length() + PREFIX_CHAR.length());
    }
    else
    {
      this.name = name;
    }
    this.accessMode = accessMode;
  }

  @Override
  public MrsImageDataProvider call() throws Exception
  {
    initialize(conf);
    final MrsImageDataProviderFactory factory = findFactory();
    if (accessMode == AccessMode.READ)
    {
      if (factory != null)
      {
        if (factory.canOpen(name, props))
        {
          return factory.createMrsImageDataProvider(name, props);
        }
        else
        {
          log.warn("Could not open " + name + " using factory " + factory.getClass().getName());
        }
      }
      throw new DataProviderNotFound("Unable to find a MrsImage data provider for " + name);
    }
    else if (accessMode == AccessMode.OVERWRITE)
    {
      if (factory != null)
      {
        if (factory.exists(name, props))
        {
          factory.delete(name, props);
        }
        return factory.createMrsImageDataProvider(name, props);
      }
      return getPreferredProvider().createMrsImageDataProvider(name, props);
    }
    else
    {
      if (factory != null)
      {
        if (factory.canWrite(name, props))
        {
          return factory.createMrsImageDataProvider(name, props);
        }
        throw new DataProviderNotFound("Unable to find a MrsImage data provider for " + name);
      }
      return getPreferredProvider().createMrsImageDataProvider(name, props);
    }
  }

  private MrsImageDataProviderFactory getPreferredProvider() throws DataProviderNotFound
  {
    if (mrsImageProviderFactories.containsKey(preferredImageProviderName))
    {
      return mrsImageProviderFactories.get(preferredImageProviderName);
    }

    throw new DataProviderNotFound("No MrsImage data providers found ");
  }

  private MrsImageDataProviderFactory findFactory() throws IOException
  {
    if (prefix != null)
    {
      if (mrsImageProviderFactories.containsKey(prefix))
      {
        if (log.isDebugEnabled())
        {
          log.debug("returning " + mrsImageProviderFactories.get(prefix).getClass().getName());
        }
        return mrsImageProviderFactories.get(prefix);
      }
      else
      {
        if (log.isInfoEnabled())
        {
          log.info("No image data provider matches prefix " + prefix);
        }
      }
    }
    for (final MrsImageDataProviderFactory factory : mrsImageProviderFactories.values())
    {
      if (factory.exists(name, props))
      {
        if (log.isDebugEnabled())
        {
          log.debug("Returning provider " + factory.getClass().getName() + " for image " + name);
        }
        return factory;
      }
      else
      {
        if (log.isInfoEnabled())
        {
          log.info("Image " + name + " does not exist for provider " + factory.getClass().getName());
        }
      }
    }

    return null;
  }

}

private static class VectorLoader implements Callable<VectorDataProvider>
{
  private String name;
  private String prefix;
  private AccessMode accessMode;
  private Configuration conf;
  private ProviderProperties props;

  public VectorLoader(final String name,
      final AccessMode accessMode,
      final Configuration conf,
      final ProviderProperties props)
  {
    this.conf = conf;
    if (conf == null && props == null)
    {
      this.props = new ProviderProperties();
    }
    else
    {
      this.props = props;
    }
    this.prefix = getPrefix(name);
    if (prefix != null)
    {
      this.name = name.substring(this.prefix.length() + PREFIX_CHAR.length());
    }
    else
    {
      this.name = name;
    }
    this.accessMode = accessMode;
  }

  @Override
  public VectorDataProvider call() throws Exception
  {
    initialize(conf);

    final VectorDataProviderFactory factory = findFactory();
    if (accessMode == AccessMode.READ)
    {
      if (factory != null)
      {
        if (log.isDebugEnabled())
        {
          log.debug("For vector " + name + ", found factory: " + factory.getClass().getName());
        }
        if (factory.canOpen(name, props))
        {
          if (log.isDebugEnabled())
          {
            log.debug("Factory " + factory.getClass().getName() + " is able to open vector " + name);
          }
          return factory.createVectorDataProvider(prefix, name, props);
        }
        else
        {
          if (log.isInfoEnabled())
          {
            log.info("Unable to open vector " + name + " with data provider " + factory.getClass().getName());
          }
        }
      }
      else
      {
        log.info("Unable to find a data provider to use for vector " + name);
      }
      // Log some useful debug information
      String msg = "Unable to find a vector data provider for " + name + " using prefix " + prefix;
      if (log.isDebugEnabled())
      {
        log.debug(msg);
        log.debug("Available vector provider factories: " + vectorProviderFactories.size());
        for (VectorDataProviderFactory f : vectorProviderFactories.values())
        {
          log.debug(f.getPrefix() + " using " + f.getClass().getName());
        }
        String cp = System.getProperty("java.class.path");
        log.debug("java.class.path=" + cp);
      }
      throw new DataProviderNotFound(msg);
    }
    else if (accessMode == AccessMode.OVERWRITE)
    {
      if (factory != null)
      {
        if (factory.exists(name, props))
        {
          factory.delete(name, props);
        }
        return factory.createVectorDataProvider(prefix, name, props);
      }
      return getPreferredProvider().createVectorDataProvider(prefix, name, props);
    }
    else
    {
      if (factory != null)
      {
        if (factory.canWrite(name, props))
        {
          return factory.createVectorDataProvider(prefix, name, props);
        }
        String msg = "Unable to find a vector data provider for " + name + " using prefix " + prefix;
        if (log.isDebugEnabled())
        {
          log.debug(msg);
          log.debug("Available vector provider factories: " + vectorProviderFactories.size());
          for (VectorDataProviderFactory f : vectorProviderFactories.values())
          {
            log.debug(f.getPrefix() + " using " + f.getClass().getName());
          }
          String cp = System.getProperty("java.class.path");
          log.debug("java.class.path=" + cp);
        }
        throw new DataProviderNotFound(msg);
      }
      return getPreferredProvider().createVectorDataProvider(prefix, name, props);
    }
  }

  private VectorDataProviderFactory getPreferredProvider() throws DataProviderNotFound
  {
    if (vectorProviderFactories.containsKey(preferredVectorProviderName))
    {
      return vectorProviderFactories.get(preferredVectorProviderName);
    }

    throw new DataProviderNotFound("No vector data providers found ");
  }

  private VectorDataProviderFactory findFactory() throws IOException
  {
    boolean debugEnabled = log.isDebugEnabled();
    if (debugEnabled)
    {
      log.debug("Looking for factory for prefix: " + ((prefix != null) ? prefix : "null") + " and name " + name);
      log.debug("Vector factory count = " + vectorProviderFactories.size());
    }
    if (prefix != null)
    {
      if (vectorProviderFactories.containsKey(prefix))
      {
        if (debugEnabled)
        {
          log.debug("Returning factory from prefix cache: " + vectorProviderFactories.get(prefix).getClass().getName());
        }
        return vectorProviderFactories.get(prefix);
      }
    }
    for (final VectorDataProviderFactory factory : vectorProviderFactories.values())
    {
      if (debugEnabled)
      {
        log.debug("Checking factory: " + factory.getClass().getName());
      }
      if (factory.exists(name, props))
      {
        if (debugEnabled)
        {
          log.debug("Returning factory from provider properties: " + factory.getClass().getName());
        }
        return factory;
      }
      if (debugEnabled)
      {
        log.debug("resource cache load: " + name);
      }
    }
    if (debugEnabled)
    {
      log.debug("Returning null factory");
    }
    return null;
  }
}
}
