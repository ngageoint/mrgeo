package org.mrgeo.publisher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by ericwood on 8/15/16.
 */
public class MrGeoPublisherFactory
{

static final String MRGEO_PUBLISHER_CLASS_PROP = "class";
static final String MRGEO_PUBLISHER_CONFIGURATOR_CLASS_PROP = "configuratorClass";
private static final Logger logger = LoggerFactory.getLogger(MrGeoPublisher.class);
private static Map<String, MrGeoPublisher> publishersCache = new HashMap<>();

public static List<MrGeoPublisher> getAllPublishers()
{
  if (publishersCache.isEmpty())
  {
    loadPublishersFromConfig();
  }
  return new ArrayList<>(publishersCache.values());
}

static MrGeoPublisher getPublisher(String profileName)
{
  if (publishersCache.isEmpty())
  {
    loadPublishersFromConfig();
  }
  MrGeoPublisher publisher = getPublisherFromCache(profileName);
  if (publisher != null)
  {
    return publisher;
  }
  else
  {
    throw new IllegalArgumentException("Publisher profile " + profileName + " not recognized.");
  }
}

static MrGeoPublisher getPublisherFromCache(String profileName)
{
  return publishersCache.get(profileName);
}

static MrGeoPublisher getPublisherFromConfig(String profileName)
{
  return null;
}

static void loadPublishersFromConfig()
{
  // Get all Publisher properties for each profile.  Put them in a map by profile name
  Map<String, Properties> mapPublisherProps = PublisherProfileConfigProperties.getPropertiesByProfileName();
  for (Map.Entry<String, Properties> publisherPropEntry : mapPublisherProps.entrySet())
  {
    String profileName = publisherPropEntry.getKey();
    Properties publisherProps = publisherPropEntry.getValue();
    String publisherClassKey = MRGEO_PUBLISHER_CLASS_PROP;
    String publisherConfiguratorClassKey = MRGEO_PUBLISHER_CONFIGURATOR_CLASS_PROP;
    String publisherClassName = (String) publisherProps.get(publisherClassKey);
    String publisherConfiguratorClassName = (String) publisherProps.get(publisherConfiguratorClassKey);
    if (publisherClassName == null || publisherClassName.isEmpty())
    {
      logger.error(String.format("Invalid publisher profile %1$s - Missing publisher proprty %2$s.",
          profileName, MRGEO_PUBLISHER_CLASS_PROP));
      continue;
    }
    if (publisherConfiguratorClassName == null || publisherConfiguratorClassName.isEmpty())
    {
      logger.error(String.format("Invalid publisher profile %1$s - Missing publisher configurator property %2$s.",
          profileName, MRGEO_PUBLISHER_CONFIGURATOR_CLASS_PROP));
      continue;
    }

    MrGeoPublisher publisher = null;
    try
    {
      // Create the publisher
      publisher = (MrGeoPublisher) Class.forName(publisherClassName).newInstance();
    }
    catch (IllegalAccessException | InstantiationException | ClassNotFoundException e)
    {
      logger.error("Error loading publisher " + publisherClassName, e);
      continue;
    }

    // Create the configurator
    MrGeoPublisherConfigurator configurator = null;
    try
    {
      configurator = (MrGeoPublisherConfigurator) Class.forName(publisherConfiguratorClassName).newInstance();
    }
    catch (IllegalAccessException | InstantiationException | ClassNotFoundException e)
    {
      logger.error("Error loading publisher configurator" + publisherConfiguratorClassName, e);
      continue;
    }

    configurator.configure(publisher, publisherProps);
    publishersCache.put(profileName, publisher);

  }

}

/**
 * Force a reload of all publishers.
 * <p>
 * Primarily used for testing, but could be useful if the configuration changes and we need to update the publishers
 */
static void clearCache()
{
  publishersCache.clear();
}
}
