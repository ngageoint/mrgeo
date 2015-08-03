/*
 * Copyright 2009-2015 DigitalGlobe, Inc.
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

package org.mrgeo.data.tile;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.utils.Bounds;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * This interface marks an implementing class as containing configuration
 * data that will be used for setting up InputFormat data for a
 * map/reduce job. The MrGeo core will construct an instance of a class
 * that implements this interface and pass it to data plugins to configure
 * the map/reduce job appropriately.
 */
public class TiledInputFormatContext
{
  private int zoomLevel;
  private int tileSize;
  private Set<String> inputs;
  private Bounds bounds;
  private boolean includeEmptyTiles = false;
  private double fillValue = Double.NaN;
  private Properties inputProviderProperties;

  private static final String className = TiledInputFormatContext.class.getSimpleName();
  private static final String ZOOM_LEVEL = className + ".zoomLevel";
  private static final String TILE_SIZE = className + ".tileSize";
  private static final String INPUTS = className + ".inputs";
  private static final String BOUNDS = className + ".bounds";
  private static final String INCLUDE_EMPTY_TILES = className + ".includeEmptyTiles";
  private static final String FILL_VALUE = className + ".fillValue";
  private static final String PROVIDER_PROPERTY_COUNT = className + "provPropCount";
  private static final String PROVIDER_PROPERTY_KEY = className + "provPropKey.";
  private static final String PROVIDER_PROPERTY_VALUE = className + "provPropValue.";

  /**
   * Use this constructor to include input at a zoom level from all of the specified
   * image pyramids. The map/reduce job will have access to all the tiles from those
   * input.
   * 
   * @param zoomlevel
   */
  public TiledInputFormatContext(final int zoomlevel, final int tileSize,
      final Set<String> inputs, final Properties inputProviderProperties)
  {
    this.zoomLevel = zoomlevel;
    this.tileSize = tileSize;
    this.inputs = inputs;
    this.bounds = null;
    this.inputProviderProperties = inputProviderProperties;
  }

  /**
   * Use this constructor to include input at a zoom level from all of the specified
   * image pyramids. The map/reduce job will have access to all the tiles from those
   * input.
   * 
   * @param zoomlevel
   */
  public TiledInputFormatContext(final int zoomlevel, final int tileSize,
      final Set<String> inputs, final Bounds bounds,
      final Properties inputProviderProperties)
  {
    this.zoomLevel = zoomlevel;
    this.tileSize = tileSize;
    this.inputs = inputs;
    this.bounds = bounds;
    this.inputProviderProperties = inputProviderProperties;
  }

  public TiledInputFormatContext(final int zoomlevel, final int tileSize,
      final Set<String> inputs, final Bounds bounds, final double fillValue,
      final Properties inputProviderProperties)
  {
    this.zoomLevel = zoomlevel;
    this.tileSize = tileSize;
    this.inputs = inputs;
    this.bounds = bounds;
    this.includeEmptyTiles = true;
    this.fillValue = fillValue;
    this.inputProviderProperties = inputProviderProperties;
  }

  protected TiledInputFormatContext()
  {
  }

  public Set<String> getInputs()
  {
    return inputs;
  }

  public Properties getProviderProperties()
  {
    return inputProviderProperties;
  }

  public String getFirstInput()
  {
    if (inputs.isEmpty())
    {
      throw new IllegalArgumentException("Cannot get first input - list is empty");
    }
    return inputs.iterator().next();
  }

  public int getZoomLevel()
  {
    return zoomLevel;
  }

  public int getTileSize()
  {
    return tileSize;
  }

  public Bounds getBounds()
  {
    return bounds;
  }

  public boolean getIncludeEmptyTiles()
  {
    return includeEmptyTiles;
  }

  public double getFillValue()
  {
    return fillValue;
  }

  public void save(final Configuration conf)
  {
    conf.set(INPUTS, StringUtils.join(inputs, ","));
    conf.setInt(ZOOM_LEVEL, zoomLevel);
    conf.setInt(TILE_SIZE, tileSize);
    if (bounds != null)
    {
      conf.set(BOUNDS, bounds.toDelimitedString());
    }
    conf.setBoolean(INCLUDE_EMPTY_TILES, includeEmptyTiles);
    if (includeEmptyTiles)
    {
      conf.setFloat(FILL_VALUE, (float)fillValue);
    }
    conf.setInt(PROVIDER_PROPERTY_COUNT,
        ((inputProviderProperties == null) ? 0 : inputProviderProperties.size()));
    if (inputProviderProperties != null)
    {
      Set<String> keySet = inputProviderProperties.stringPropertyNames();
      String[] keys = new String[keySet.size()];
      keySet.toArray(keys);
      for (int i=0; i < keys.length; i++)
      {
        conf.set(PROVIDER_PROPERTY_KEY + i, keys[i]);
        String v = inputProviderProperties.getProperty(keys[i]);
        if (v != null)
        {
          conf.set(PROVIDER_PROPERTY_VALUE + i, v);
        }
      }
    }
  }

  public static TiledInputFormatContext load(final Configuration conf)
  {
    TiledInputFormatContext context = new TiledInputFormatContext();
    context.inputs = new HashSet<String>();
    String strInputs = conf.get(INPUTS);
    if (strInputs != null)
    {
      String[] confInputs = strInputs.split(",");
      for (String confInput : confInputs)
      {
        context.inputs.add(confInput);
      }
    }
    context.zoomLevel = conf.getInt(ZOOM_LEVEL, 1);
    context.tileSize = conf.getInt(TILE_SIZE, MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT_INT);
    String confBounds = conf.get(BOUNDS);
    if (confBounds != null)
    {
      context.bounds = Bounds.fromDelimitedString(confBounds);
    }
    context.includeEmptyTiles = conf.getBoolean(INCLUDE_EMPTY_TILES, false);
    if (context.includeEmptyTiles)
    {
      context.fillValue = conf.getFloat(FILL_VALUE, Float.NaN);
    }
    int providerPropertyCount = conf.getInt(PROVIDER_PROPERTY_COUNT, 0);
    if (providerPropertyCount > 0)
    {
      context.inputProviderProperties = new Properties();
      for (int i=0; i < providerPropertyCount; i++)
      {
        String key = conf.get(PROVIDER_PROPERTY_KEY + i);
        String value = conf.get(PROVIDER_PROPERTY_VALUE + i);
        context.inputProviderProperties.setProperty(key, value);
      }
    }
    return context;
  }
}
