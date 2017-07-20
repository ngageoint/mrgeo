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

package org.mrgeo.data.vector;

import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.geometry.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public abstract class VectorDataProvider
{
static Logger log = LoggerFactory.getLogger(VectorDataProvider.class);
protected ProviderProperties providerProperties;
private String resourcePrefix;
private String resourceName;

public VectorDataProvider(String inputPrefix, String input)
{
  resourcePrefix = inputPrefix;
  resourceName = input;
}

public VectorDataProvider(String inputPrefix, String input, ProviderProperties providerProps)
{
  resourcePrefix = inputPrefix;
  resourceName = input;
  providerProperties = providerProps;
}

public String getResourceName()
{
  return resourceName;
}

public String getResourcePrefix()
{
  return resourcePrefix;
}

public ProviderProperties getProviderProperties()
{
  return providerProperties;
}

public String getPrefixedResourceName()
{
  if (resourcePrefix != null && !resourcePrefix.isEmpty())
  {
    return resourcePrefix + ":" + resourceName;
  }
  return resourceName;
}

/**
 * Return an instance of a class that can read metadata for this resource.
 * If this type of vector data does not support an overall schema of metadata
 * then return null. That would be the case when each feature could potentially
 * have a different set of attributes.
 *
 * @return
 */
public abstract VectorMetadataReader getMetadataReader();

/**
 * Return an instance of a class that can write metadata for this resource.
 * If this type of vector data does not support an overall schema of metadata
 * then return null. That would be the case when each feature could potentially
 * have a different set of attributes.
 *
 * @return
 */
public abstract VectorMetadataWriter getMetadataWriter();

public abstract VectorReader getVectorReader() throws IOException;

/**
 * Return an instance of a VectorReader class to be used for reading vector data. This
 * method may be invoked by callers regardless of whether they are running within a
 * map/reduce job or not.
 *
 * @return
 * @throws IOException
 */
public abstract VectorReader getVectorReader(VectorReaderContext context) throws IOException;

public abstract VectorWriter getVectorWriter() throws IOException;

/**
 * Return an instance of a RecordReader class to be used in map/reduce jobs for reading
 * vector data.
 *
 * @return
 */
public abstract RecordReader<FeatureIdWritable, Geometry> getRecordReader() throws IOException;

/**
 * Return an instance of a RecordWriter class to be used in map/reduce jobs for writing
 * vector data.
 *
 * @return
 */
public abstract RecordWriter<FeatureIdWritable, Geometry> getRecordWriter();

/**
 * Return an instance of an InputFormat class to be used in map/reduce jobs for processing
 * vector data.
 *
 * @return
 */
public abstract VectorInputFormatProvider getVectorInputFormatProvider(
    VectorInputFormatContext context) throws IOException;

/**
 * Return an instance of an OutputFormat class to be used in map/reduce jobs for producing
 * vector data.
 *
 * @return
 */
public abstract VectorOutputFormatProvider getVectorOutputFormatProvider(
    VectorOutputFormatContext context) throws IOException;

public abstract void delete() throws IOException;

public abstract void move(String toResource) throws IOException;

/**
 * Parses a settings string from a data source name. Settings
 * are separated by semi-colons. Settings thtemselves are formatted
 * like "name=value". The value can contain semi-colon's as long as
 * it is surrounded in double quotes. The value may contain embedded
 * double quotes as long as they are escaped with a \. For example
 * "my-setting="This is \" my; setting"
 *
 * @param strSettings
 * @param settings
 * @throws IOException
 */
public static void parseDataSourceSettings(
      String strSettings,
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
}
