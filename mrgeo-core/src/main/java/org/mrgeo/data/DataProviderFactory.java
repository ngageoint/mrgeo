/*
 * Copyright 2009-2014 DigitalGlobe, Inc.
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
 */

package org.mrgeo.data;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

import org.apache.hadoop.conf.Configuration;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.data.adhoc.AdHocDataProvider;
import org.mrgeo.data.adhoc.AdHocDataProviderFactory;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.data.image.MrsImageDataProviderFactory;
import org.mrgeo.data.ingest.ImageIngestDataProvider;
import org.mrgeo.data.ingest.ImageIngestDataProviderFactory;
import org.mrgeo.data.vector.VectorDataProvider;
import org.mrgeo.data.vector.VectorDataProviderFactory;
import org.mrgeo.utils.DependencyLoader;
import org.mrgeo.utils.HadoopUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * This class is responsible for creating instances of several types of data providers.
 * A data provider is an abstraction that provides read/write access to data in a
 * back-end data store without knowing the actual type of the back-end data store
 * (e.g. HDFS, Accumulo, ...). There is an abstract data provider class for each
 * type of data to be accessed, including AdHoc data, MrsImage data, and ImageIngest data.
 * These are described below. Each instance of a data provider returned by the methods
 * in this class are for a specific piece of data (like a java.io.File instance references
 * a specific file on the disk). Data providers are implemented in data plugins
 * and the providers are discovered dynamically at runtime through the Java
 * ServiceLoader.
 *
 * ImageIngest data providers access a source of raw imagery that is to be ingested
 * into MrGeo. The image ingest functionality uses this data provider to read the
 * actual bytes of source imagery while ingesting that imagery into MrGeo.
 *
 * MrsImage data providers access a MrGeo image (referred to as a MrsImage). A MrsImage
 * data provider is for writing to a MrsImage while ingesting a raw image into MrGeo.
 *
 * AdHoc data providers are used for accessing other types of data besides raw
 * images and MrsImages. This data provider is used within MrGeo while performing
 * processing in order to save state as it works.
 *
 * Vector data providers access a vector data source.
 */
public class DataProviderFactory
{
  static Logger log = LoggerFactory.getLogger(DataProviderFactory.class);

  public enum AccessMode { READ, WRITE, OVERWRITE }

  final static String PREFERRED_PROPERTYNAME = "preferred.provider";
  final static String PREFERRED_ADHOC_PROPERTYNAME = "preferred.adhoc.provider";
  final static String PREFERRED_INGEST_PROPERTYNAME = "preferred.ingest.provider";
  final static String PREFERRED_MRSIMAGE_PROPERTYNAME = "preferred.image.provider";
  final static String PREFERRED_VECTOR_PROPERTYNAME = "preferred.vector.provider";

  final static String BASECLASS = DataProviderFactory.class.getSimpleName() + ".";
  final static String PREFERRED_ADHOC_PROVIDER_NAME = BASECLASS + PREFERRED_ADHOC_PROPERTYNAME;
  final static String PREFERRED_INGEST_PROVIDER_NAME = BASECLASS + PREFERRED_INGEST_PROPERTYNAME;
  final static String PREFERRED_MRSIMAGE_PROVIDER_NAME = BASECLASS + PREFERRED_MRSIMAGE_PROPERTYNAME;
  final static String PREFERRED_VECTOR_PROVIDER_NAME = BASECLASS + PREFERRED_VECTOR_PROPERTYNAME;

  private final static String PREFIX_CHAR = ":"; // use ":" for the prefix delimiter
  private final static int PROVIDER_CACHE_SIZE = 50;
  private final static int PROVIDER_CACHE_EXPIRE = 10; // minutes

  private static Configuration basicConf;

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

  public static void saveProviderPropertiesToConfig(final Properties providerProperties,
      final Configuration conf)
  {
    if (providerProperties != null)
    {
      for (Map.Entry<Object,Object> entry : providerProperties.entrySet())
      {
        conf.set(entry.getKey().toString(), entry.getValue().toString());
      }
    }
  }

  public static String getProviderProperty(final String key,
      final Properties providerProperties)
  {
    return providerProperties.getProperty(key);
  }

  public static String getProviderProperty(final String key,
      final Configuration conf)
  {
    return conf.get(key);
  }

  public static void setProviderProperty(final String key, final String value,
      final Properties providerProperties)
  {
    providerProperties.setProperty(key, value);
  }

  private static class AdHocLoader implements Callable<AdHocDataProvider>
  {
    private String prefix;
    private String name;
    private AccessMode accessMode;
    private Configuration conf;
    private Properties props;

    public AdHocLoader(final String name,
        final AccessMode accessMode,
        final Configuration conf,
        final Properties props)
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
      initialize(conf, props);
      final AdHocDataProviderFactory factory = findFactory();
      if (accessMode == AccessMode.READ)
      {
        if (factory != null)
        {
          if (props != null)
          {
            if (factory.canOpen(name, props))
            {
              return factory.createAdHocDataProvider(name, props);
            }
          }
          else
          {
            if (factory.canOpen(name, conf))
            {
              return factory.createAdHocDataProvider(name, conf);
            }
          }
        }
        throw new DataProviderNotFound("Unable to find an ad hoc data provider for " + name);
      }
      else if (accessMode == AccessMode.OVERWRITE)
      {
        if (factory != null)
        {
          if (props != null)
          {
            if (factory.exists(name, props))
            {
              factory.delete(name, props);
            }
            return factory.createAdHocDataProvider(name, props);
          }
          else
          {
            if (factory.exists(name, conf))
            {
              factory.delete(name, conf);
            }
            return factory.createAdHocDataProvider(name, conf);
          }
        }
        if (props != null)
        {
          return getPreferredProvider().createAdHocDataProvider(name, props);
        }
        else
        {
          return getPreferredProvider().createAdHocDataProvider(name, conf);
        }
      }
      else
      {
        if (factory != null)
        {
          if (props != null)
          {
            if (factory.canWrite(name, props))
            {
              return factory.createAdHocDataProvider(name, props);
            }
            throw new DataProviderNotFound("Unable to find an ad hoc data provider for " + name);
          }
          else
          {
            if (factory.canWrite(name, conf))
            {
              return factory.createAdHocDataProvider(name, conf);
            }
            throw new DataProviderNotFound("Unable to find an ad hoc data provider for " + name);
          }
        }
        if (props != null)
        {
          return getPreferredProvider().createAdHocDataProvider(name, props);
        }
        else
        {
          return getPreferredProvider().createAdHocDataProvider(name, conf);
        }
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
        if (props != null)
        {
          if (factory.exists(name, props))
          {
            return factory;
          }
          log.debug("resource cache load: " + name);
        }
        else
        {
          if (factory.exists(name, conf))
          {
            return factory;
          }
          log.debug("resource cache load: " + name);
        }
      }

      return null;
    }
  }

  private static class ImageIngestLoader implements Callable<ImageIngestDataProvider>
  {
    private String prefix;
    private String name;
    private AccessMode accessMode;

    public ImageIngestLoader(final String name, final AccessMode accessMode)
    {
      this.prefix = getPrefix(name);
      if (prefix != null)
      {
        this.name = name.substring(this.prefix.length() + PREFIX_CHAR.length());
      }
      else
      {
        this.name = name;
      }
      this.name = name;
      this.accessMode = accessMode;
    }


    @Override
    public ImageIngestDataProvider call() throws Exception
    {
      initialize(null, null);
      final ImageIngestDataProviderFactory factory = findFactory();
      if (accessMode == AccessMode.READ)
      {
        if (factory != null)
        {
          if (factory.canOpen(name))
          {
            return factory.createImageIngestDataProvider(name);
          }
        }
        throw new DataProviderNotFound("Unable to find an image ingest data provider for " + name);
      }
      else if (accessMode == AccessMode.OVERWRITE)
      {
        if (factory != null)
        {
          if (factory.exists(name))
          {
            factory.delete(name);
          }
          return factory.createImageIngestDataProvider(name);
        }
        return getPreferredProvider().createImageIngestDataProvider(name);
      }
      else
      {
        if (factory != null)
        {
          if (factory.canWrite(name))
          {
            return factory.createImageIngestDataProvider(name);
          }
          throw new DataProviderNotFound("Unable to find an image ingest data provider for " + name);
        }
        return getPreferredProvider().createImageIngestDataProvider(name);
      }

    }

    private ImageIngestDataProviderFactory getPreferredProvider() throws DataProviderNotFound
    {
      if (imageIngestProviderFactories.containsKey(preferredIngestProviderName))
      {
        return imageIngestProviderFactories.get(preferredIngestProviderName);
      }

      throw new DataProviderNotFound("No ad hoc data providers found ");
    }

    private ImageIngestDataProviderFactory findFactory() throws IOException
    {
      if (prefix != null)
      {
        if (imageIngestProviderFactories.containsKey(prefix))
        {
          return imageIngestProviderFactories.get(prefix);
        }
      }
      for (final ImageIngestDataProviderFactory factory : imageIngestProviderFactories.values())
      {
        if (factory.exists(name))
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
    private Properties props;

    public MrsImageLoader(final String name,
        final AccessMode accessMode,
        final Configuration conf,
        final Properties props)
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
      initialize(conf, props);
      final MrsImageDataProviderFactory factory = findFactory();
      if (accessMode == AccessMode.READ)
      {
        if (factory != null)
        {
          if (props != null)
          {
            if (factory.canOpen(name, props))
            {
              return factory.createMrsImageDataProvider(name, props);
            }
          }
          else
          {
            if (factory.canOpen(name, conf))
            {
              return factory.createMrsImageDataProvider(name, conf);
            }
          }
        }
        throw new DataProviderNotFound("Unable to find a MrsImage data provider for " + name);
      }
      else if (accessMode == AccessMode.OVERWRITE)
      {
        if (factory != null)
        {
          if (props != null)
          {
            if (factory.exists(name, props))
            {
              factory.delete(name, props);
            }
            return factory.createMrsImageDataProvider(name, props);
          }
          else
          {
            if (factory.exists(name, conf))
            {
              factory.delete(name, conf);
            }
            return factory.createMrsImageDataProvider(name, conf);
          }
        }
        if (props != null)
        {
          return getPreferredProvider().createMrsImageDataProvider(name, props);
        }
        else
        {
          return getPreferredProvider().createMrsImageDataProvider(name, conf);
        }
      }
      else
      {
        if (factory != null)
        {
          if (props != null)
          {
            if (factory.canWrite(name, props))
            {
              return factory.createMrsImageDataProvider(name, props);
            }
            throw new DataProviderNotFound("Unable to find a MrsImage data provider for " + name);
          }
          else
          {
            if (factory.canWrite(name, conf))
            {
              return factory.createMrsImageDataProvider(name, conf);
            }
            throw new DataProviderNotFound("Unable to find a MrsImage data provider for " + name);
          }
        }
        if (props != null)
        {
          return getPreferredProvider().createMrsImageDataProvider(name, props);
        }
        else
        {
          return getPreferredProvider().createMrsImageDataProvider(name, conf);
        }
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
          return mrsImageProviderFactories.get(prefix);
        }
      }
      for (final MrsImageDataProviderFactory factory : mrsImageProviderFactories.values())
      {
        if (props != null)
        {
          if (factory.exists(name, props))
          {
            return factory;
          }
          log.debug("resource cache load: " + name);
        }
        else
        {
          if (factory.exists(name, conf))
          {
            return factory;
          }
          log.debug("resource cache load: " + name);
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
    private Properties props;

    public VectorLoader(final String name,
        final AccessMode accessMode,
        final Configuration conf,
        final Properties props)
    {
      this.conf = conf;
      if (conf == null && props == null)
      {
        this.props = new Properties();
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
      initialize(conf, props);
      final VectorDataProviderFactory factory = findFactory();
      if (accessMode == AccessMode.READ)
      {
        if (factory != null)
        {
          if (props != null)
          {
            if (factory.canOpen(name, props))
            {
              return factory.createVectorDataProvider(name, props);
            }
          }
          else
          {
            if (factory.canOpen(name, conf))
            {
              return factory.createVectorDataProvider(name, conf);
            }
          }
        }
        // Log some useful debug information
        String msg = "Unable to find a vector data provider for " + name + " using prefix " + prefix;
        if (log.isDebugEnabled())
        {
          log.debug(msg);
          log.debug("Available vector provider factories: " + vectorProviderFactories.size());
          for (VectorDataProviderFactory f: vectorProviderFactories.values())
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
          if (props != null)
          {
            if (factory.exists(name, props))
            {
              factory.delete(name, props);
            }
            return factory.createVectorDataProvider(name, props);
          }
          else
          {
            if (factory.exists(name, conf))
            {
              factory.delete(name, conf);
            }
            return factory.createVectorDataProvider(name, conf);
          }
        }
        if (props != null)
        {
          return getPreferredProvider().createVectorDataProvider(name, props);
        }
        else
        {
          return getPreferredProvider().createVectorDataProvider(name, conf);
        }
      }
      else
      {
        if (factory != null)
        {
          if (props != null)
          {
            if (factory.canWrite(name, props))
            {
              return factory.createVectorDataProvider(name, props);
            }
            String msg = "Unable to find a vector data provider for " + name + " using prefix " + prefix;
            if (log.isDebugEnabled())
            {
              log.debug(msg);
              log.debug("Available vector provider factories: " + vectorProviderFactories.size());
              for (VectorDataProviderFactory f: vectorProviderFactories.values())
              {
                log.debug(f.getPrefix() + " using " + f.getClass().getName());
              }
              String cp = System.getProperty("java.class.path");
              log.debug("java.class.path=" + cp);
            }
            throw new DataProviderNotFound(msg);
          }
          else
          {
            if (factory.canWrite(name, conf))
            {
              return factory.createVectorDataProvider(name, conf);
            }
            String msg = "Unable to find a vector data provider for " + name + " using prefix " + prefix;
            if (log.isDebugEnabled())
            {
              log.debug(msg);
              log.debug("Available vector provider factories: " + vectorProviderFactories.size());
              for (VectorDataProviderFactory f: vectorProviderFactories.values())
              {
                log.debug(f.getPrefix() + " using " + f.getClass().getName());
              }
              String cp = System.getProperty("java.class.path");
              log.debug("java.class.path=" + cp);
            }
            throw new DataProviderNotFound(msg);
          }
        }
        if (props != null)
        {
          return getPreferredProvider().createVectorDataProvider(name, props);
        }
        else
        {
          return getPreferredProvider().createVectorDataProvider(name, conf);
        }
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
        if (props != null)
        {
          if (factory.exists(name, props))
          {
            if (debugEnabled)
            {
              log.debug("Returning factory from provider properties: " + vectorProviderFactories.get(prefix).getClass().getName());
            }
            return factory;
          }
          if (debugEnabled)
          {
            log.debug("resource cache load: " + name);
          }
        }
        else
        {
          if (factory.exists(name, conf))
          {
            if (debugEnabled)
            {
              log.debug("Returning factory from configuration: " + vectorProviderFactories.get(prefix).getClass().getName());
            }
            return factory;
          }
          if (debugEnabled)
          {
            log.debug("resource cache load: " + name);
          }
        }
      }
      if (debugEnabled)
      {
        log.debug("Returning null factory");
      }
      return null;
    }
  }

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

  private static Cache<String, ImageIngestDataProvider> imageIngestProviderCache = CacheBuilder
      .newBuilder().maximumSize(PROVIDER_CACHE_SIZE).expireAfterAccess(
          PROVIDER_CACHE_EXPIRE, TimeUnit.MINUTES).removalListener(
          new RemovalListener<String, ImageIngestDataProvider>()
          {
            @Override
            public void onRemoval(
                final RemovalNotification<String, ImageIngestDataProvider> notification)
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


  protected static Map<String, AdHocDataProviderFactory> adHocProviderFactories;
  protected static Map<String, ImageIngestDataProviderFactory> imageIngestProviderFactories;
  protected static Map<String, MrsImageDataProviderFactory> mrsImageProviderFactories;
  protected static Map<String, VectorDataProviderFactory> vectorProviderFactories;

  protected static String preferredAdHocProviderName = null;
  protected static String preferredIngestProviderName = null;
  protected static String preferredImageProviderName = null;
  protected static String preferredVectorProviderName = null;

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
   * Create a data provider for a new ad hoc data source with a randomly generated
   * name. Use this method if you don't care what the name of the data source is.
   * This is useful for storing temporary data that will be accessed during
   * processing, and then deleted after processing completes. This method should
   * be called from the name node side, not from mappers or reducers.
   *
   * TODO: Add information about how it chooses the appropriate provider for the
   * name passed in.
   *
   * TODO: When ad hoc data is implemented in Accumulo, this method will need to
   * be changed so that it receives the providerProperties argument just like
   * the getMrsImageDataProvider method.
   *
   * @return An ad hoc data provider for a newly created, randomly named resource.
   * @throws DataProviderNotFound
   */
  public static AdHocDataProvider createAdHocDataProvider(final Properties providerProperties)
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
   *
   * TODO: Add information about how it chooses the appropriate provider for the
   * name passed in.
   *
   * @return An ad hoc data provider for a newly created, randomly named resource.
   * @throws DataProviderNotFound
   */
  public static AdHocDataProvider createAdHocDataProvider(final Configuration conf)
      throws DataProviderNotFound, DataProviderException
  {
    return createAdHocDataProvider(conf, null);
  }

  private static AdHocDataProvider createAdHocDataProvider(final Configuration conf,
      final Properties props) throws DataProviderNotFound, DataProviderException
  {
    initialize(conf, props);
    for (final AdHocDataProviderFactory factory : adHocProviderFactories.values())
    {
      AdHocDataProvider provider;
      try
      {
        if (props != null)
        {
          provider = factory.createAdHocDataProvider(props);
        }
        else
        {
          provider = factory.createAdHocDataProvider(conf);
        }
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

  /**
   * Create a data provider for a specifically named ad hoc data source. Use this
   * method if you need an ad hoc data source with a name that you assign. This
   * would be used for accessing/storing named data that is not raw imagery being
   * ingested or a MrsImage.
   *
   * TODO: Add information about how it chooses the appropriate provider for the
   * name passed in.
   *
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
      final Properties providerProperties) throws DataProviderNotFound
  {
    return getAdHocDataProvider(name, mode, getBasicConfig(), providerProperties);
  }

  public static AdHocDataProvider getAdHocDataProvider(final String name,
      final AccessMode mode,
      final Configuration conf) throws DataProviderNotFound
  {
    return getAdHocDataProvider(name, mode, conf, null);
  }

  private static AdHocDataProvider getAdHocDataProvider(final String name,
      final AccessMode mode,
      final Configuration conf,
      final Properties props) throws DataProviderNotFound
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
        throw (DataProviderNotFound)e.getCause();
      }
      throw new DataProviderNotFound(e);
    }
  }

  /**
   * Create a data provider for accessing raw imagery such as GeoTIFF data and
   * storing it during ingest processing to a format that can be used as input
   * for map/reduce processing.
   *
   * During ingest, the raw imagery is read and tiled, and each individual tile
   * is written to an intermediate data format which is then used as input to a
   * map/reduce job which outputs a MrsImage.
   *
   * TODO: Add information about how it chooses the appropriate provider for the
   * name passed in.
   *
   * @param name
   * @return
   * @throws DataProviderNotFound
   */
  public static ImageIngestDataProvider getImageIngestDataProvider(final String name,
      AccessMode accessMode) throws DataProviderNotFound
  {
    try
    {
      if (accessMode == AccessMode.OVERWRITE || accessMode == AccessMode.WRITE)
      {
        invalidateCache(name);
      }
      return imageIngestProviderCache.get(name,
          new ImageIngestLoader(name, accessMode));
    }
    catch (ExecutionException e)
    {
      if (e.getCause() instanceof DataProviderNotFound)
      {
        throw (DataProviderNotFound)e.getCause();
      }
      throw new DataProviderNotFound(e);
    }
  }

  /**
   * Returns a list of MrsImages available from all data sources. The names returned
   * can be subsequently passed as the name parameter to getMrsImageDataProvider().
   *
   * @return
   * @throws IOException
   */
  public static String[] listImages(final Properties providerProperties) throws IOException
  {
    initialize(null, providerProperties);
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
  public static String[] listVectors(final Properties providerProperties) throws IOException
  {
    initialize(null, providerProperties);
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

  // Be sure that provider caching is unique for different users so that we're
  // not using the wrong user credentials. On the server side, we include the
  // calling user name in the key. On the data node side, there is no need to
  // do this because the cache only contains providers used for one job, which
  // is executed for one user.
  private static String getResourceCacheKey(final String resourceName,
      final Configuration conf,
      final Properties providerProperties)
  {
    if (providerProperties != null)
    {
      String userName = getProviderProperty(DataProviderFactory.PROVIDER_PROPERTY_USER_NAME,
          providerProperties);
      if (userName != null && !userName.isEmpty())
      {
        return resourceName + "," + userName;
      }
    }
    return resourceName;
  }

  /**
   * Create a data provider for a MrsImage data resource. This method should be
   * called to access a MrsImage (an image ingested into MrGeo) as well as
   * metadata about a MrsImage. Do not call this method from code that runs on
   * the data node size of Hadoop. Use the other signature that accepts a
   * Configuration parameter in that case.
   *
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
      Properties props) throws DataProviderNotFound
  {
    return getMrsImageDataProvider(name, accessMode, getBasicConfig(), props);
  }

  /**
   * Create a data provider for a MrsImage data resource. This method should be
   * called to access a MrsImage (an image ingested into MrGeo) as well as
   * metadata about a MrsImage. Call this method when you need to get a data
   * provider from within a mapper or reducer or any other functionality that
   * runs on the data node side of Hadoop.
   *
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
    return getMrsImageDataProvider(name, accessMode, conf, null);
  }

  private static MrsImageDataProvider getMrsImageDataProvider(final String name,
      AccessMode accessMode,
      final Configuration conf,
      final Properties providerProperties) throws DataProviderNotFound
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
      return mrsImageProviderCache.get(cacheKey,
          new MrsImageLoader(name, accessMode, conf, providerProperties));
    }
    catch (ExecutionException e)
    {
      if (e.getCause() instanceof DataProviderNotFound)
      {
        throw (DataProviderNotFound)e.getCause();
      }
      throw new DataProviderNotFound(e);
    }
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
      Properties providerProperties) throws DataProviderNotFound
  {
    return getVectorDataProvider(name, accessMode, null, providerProperties);
  }

  public static VectorDataProvider getVectorDataProvider(final Configuration conf,
      final String name,
      AccessMode accessMode) throws DataProviderNotFound
  {
    return getVectorDataProvider(name, accessMode, conf, null);
  }

  private static VectorDataProvider getVectorDataProvider(final String name,
      AccessMode accessMode,
      final Configuration conf,
      final Properties providerProperties) throws DataProviderNotFound
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
        throw (DataProviderNotFound)e.getCause();
      }
      throw new DataProviderNotFound(e);
    }
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
    imageIngestProviderCache.invalidateAll();
    mrsImageProviderCache.invalidateAll();
    vectorProviderCache.invalidateAll();
  }

  /**
   * Similar to invalidateCache(), except it invalidates a specific resource instead
   * of all resources.
   * @param resource
   */
  public static void invalidateCache(final String resource)
  {
    if (resource != null && !resource.isEmpty())
    {
      adHocProviderCache.invalidate(resource);
      imageIngestProviderCache.invalidate(resource);
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
      final Properties providerProperties) throws IOException
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

    ImageIngestDataProvider ingestProvider = getImageIngestDataProvider(resource, AccessMode.OVERWRITE);
    if (ingestProvider != null)
    {
      ingestProvider.delete();
      imageIngestProviderCache.invalidate(resource);
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

  protected static void initialize(final Configuration conf, final Properties p)
  {
    if (adHocProviderFactories == null)
    {
      adHocProviderFactories = new HashMap<String, AdHocDataProviderFactory>();
      // Find the mrsImageProviders
      final ServiceLoader<AdHocDataProviderFactory> dataProviderLoader = ServiceLoader
          .load(AdHocDataProviderFactory.class);
      for (final AdHocDataProviderFactory dp : dataProviderLoader)
      {
        if (dp.isValid())
        {
          adHocProviderFactories.put(dp.getPrefix(), dp);
        }
        else
        {
          log.info("Skipping ad hoc data provider " + dp.getClass().getName() +
              " because isValid returned false");
        }
      }
    }

    if (imageIngestProviderFactories == null)
    {
      imageIngestProviderFactories = new HashMap<String, ImageIngestDataProviderFactory>();
      // Find the imageIngestProviderFactories

      final ServiceLoader<ImageIngestDataProviderFactory> dataProviderLoader = ServiceLoader
          .load(ImageIngestDataProviderFactory.class);
      for (final ImageIngestDataProviderFactory dp : dataProviderLoader)
      {
        try
        {
          if (dp.isValid())
          {
            imageIngestProviderFactories.put(dp.getPrefix(), dp);
          }
          else
          {
            log.info("Skipping image ingest data provider " + dp.getClass().getName() +
                " because isValid returned false");
          }
        }
        catch (Exception e)
        {
          // no op, just won't put the provider in the list
        }

      }
    }
    if (mrsImageProviderFactories == null)
    {
      mrsImageProviderFactories = new HashMap<String, MrsImageDataProviderFactory>();

      // Find the mrsImageProviders
      final ServiceLoader<MrsImageDataProviderFactory> dataProviderLoader = ServiceLoader
          .load(MrsImageDataProviderFactory.class);
      for (final MrsImageDataProviderFactory dp : dataProviderLoader)
      {
        try
        {
          if (dp.isValid())
          {
            mrsImageProviderFactories.put(dp.getPrefix(), dp);
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
        }
      }
    }
    if (vectorProviderFactories == null)
    {
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
            log.debug("Checking if vector factory is valid: " + dp.getClass().getName());
          }
          boolean valid = false;
          if (conf != null)
          {
            valid = dp.isValid(conf);
          }
          else
          {
            valid = dp.isValid();
          }
          if (valid)
          {
            if (debugEnabled)
            {
              log.debug("Factory " + dp.getClass().getName() + " is valid, uses prefix: " + dp.getPrefix());
            }
            vectorProviderFactories.put(dp.getPrefix(), dp);
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

    findPreferredProvider(conf, p);
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

    if (imageIngestProviderFactories != null)
    {
      for (final ImageIngestDataProviderFactory dp : imageIngestProviderFactories.values())
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

  } // end addDependencies
  
  
  private static void findPreferredProvider(Configuration conf, Properties p)
  {
    preferredAdHocProviderName = findValue(conf, p, PREFERRED_ADHOC_PROVIDER_NAME, PREFERRED_ADHOC_PROPERTYNAME);
    // no preferred provider, use the 1st one...
    if (preferredAdHocProviderName == null)
    {
      for (final AdHocDataProviderFactory factory : adHocProviderFactories.values())
      {
        preferredAdHocProviderName = factory.getPrefix();
        break;
      }
    }

    preferredImageProviderName = findValue(conf, p, PREFERRED_MRSIMAGE_PROVIDER_NAME, PREFERRED_MRSIMAGE_PROPERTYNAME);
    // no preferred provider, use the 1st one...
    if (preferredImageProviderName == null)
    {
      for (final MrsImageDataProviderFactory factory : mrsImageProviderFactories.values())
      {
        preferredImageProviderName = factory.getPrefix();
        break;
      }
    }

    preferredIngestProviderName = findValue(conf, p, PREFERRED_INGEST_PROVIDER_NAME, PREFERRED_INGEST_PROPERTYNAME);
    // no preferred provider, use the 1st one...
    if (preferredIngestProviderName == null)
    {
      for (final ImageIngestDataProviderFactory factory : imageIngestProviderFactories.values())
      {
        preferredIngestProviderName = factory.getPrefix();
        break;
      }
    }

    preferredVectorProviderName = findValue(conf, p, PREFERRED_VECTOR_PROVIDER_NAME, PREFERRED_VECTOR_PROPERTYNAME);
    // no preferred provider, use the 1st one...
    if (preferredVectorProviderName == null)
    {
      for (final VectorDataProviderFactory factory : vectorProviderFactories.values())
      {
        preferredVectorProviderName = factory.getPrefix();
        break;
      }
    }

  }

  private static String findValue(final Configuration conf, final Properties p, final String confName, final String propName)
  {
    String name = null;

    // 1st look in the config
    if (conf != null)
    {
      name = conf.get(confName, null);
    }

    if (name == null)
    {
      if (p != null)
      {
        // now look in the properties
        name = p.getProperty(propName, null);
      }

      if (name == null)
      {
        Properties mp = MrGeoProperties.getInstance();
        if (mp != null)
        {
          name = MrGeoProperties.getInstance().getProperty(propName, null);
        }
      }
      // look for the generic name
      if (name == null && p != null)
      {
        name = p.getProperty(PREFERRED_PROPERTYNAME, null);
      }

      if (name == null)
      {
        Properties mp = MrGeoProperties.getInstance();
        if (mp != null)
        {
          name = MrGeoProperties.getInstance().getProperty(PREFERRED_PROPERTYNAME, null);
        }
      }

    }

    return name;
  }

  protected static String getPrefix(String name)
  {
    int ndx = name.indexOf(PREFIX_CHAR);
    if (ndx > 0)
    {
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

  private static Configuration getBasicConfig()
  {
    if (basicConf == null)
    {
      basicConf = HadoopUtils.createConfiguration();
    }
    return basicConf;
  }
}
