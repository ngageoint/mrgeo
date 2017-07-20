/*
 * Copyright 2009-2017. DigitalGlobe, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.mrgeo.publisher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Map.Entry;

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
  for (Entry<String, Properties> publisherPropEntry : mapPublisherProps.entrySet())
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
