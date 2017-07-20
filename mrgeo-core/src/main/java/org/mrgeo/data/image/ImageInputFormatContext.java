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

package org.mrgeo.data.image;

import org.apache.hadoop.conf.Configuration;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.utils.tms.Bounds;

/**
 * This interface marks an implementing class as containing configuration
 * data that will be used for setting up InputFormat data for a
 * map/reduce job. The MrGeo core will construct an instance of a class
 * that implements this interface and pass it to data plugins to configure
 * the map/reduce job appropriately.
 */
public class ImageInputFormatContext
{
private static final String className = ImageInputFormatContext.class.getSimpleName();
private static final String ZOOM_LEVEL = className + ".zoomLevel";
private static final String TILE_SIZE = className + ".tileSize";
private static final String INPUT = className + ".input";
private static final String BOUNDS = className + ".bounds";
private static final String PROVIDER_PROPERTY_KEY = className + "provProps";
private int zoomLevel;
private int tileSize;
private String input;
private Bounds bounds;
private ProviderProperties inputProviderProperties;

/**
 * Use this constructor to include input at a zoom level from all of the specified
 * image pyramids. The map/reduce job will have access to all the tiles from those
 * input.
 */
public ImageInputFormatContext(int zoomlevel, int tileSize,
    String input, ProviderProperties inputProviderProperties)
{
  zoomLevel = zoomlevel;
  this.tileSize = tileSize;
  this.input = input;
  bounds = null;
  this.inputProviderProperties = inputProviderProperties;
}

/**
 * Use this constructor to include input at a zoom level from all of the specified
 * image pyramids. The map/reduce job will have access to all the tiles from those
 * input.
 */
public ImageInputFormatContext(int zoomlevel, int tileSize,
    String input, Bounds bounds,
    ProviderProperties inputProviderProperties)
{
  zoomLevel = zoomlevel;
  this.tileSize = tileSize;
  this.input = input;
  this.bounds = bounds;
  this.inputProviderProperties = inputProviderProperties;
}

protected ImageInputFormatContext()
{
}

public static ImageInputFormatContext load(Configuration conf)
{
  ImageInputFormatContext context = new ImageInputFormatContext();
  context.input = conf.get(INPUT);
  context.zoomLevel = conf.getInt(ZOOM_LEVEL, 1);
  context.tileSize = conf.getInt(TILE_SIZE, MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT_INT);
  String confBounds = conf.get(BOUNDS);
  if (confBounds != null)
  {
    context.bounds = Bounds.fromCommaString(confBounds);
  }
  String strProviderProperties = conf.get(PROVIDER_PROPERTY_KEY);
  if (strProviderProperties != null)
  {
    context.inputProviderProperties = ProviderProperties.fromDelimitedString(strProviderProperties);
  }
  return context;
}

public String getInput()
{
  return input;
}

public ProviderProperties getProviderProperties()
{
  return inputProviderProperties;
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

public void save(Configuration conf)
{
  conf.set(INPUT, input);
  conf.setInt(ZOOM_LEVEL, zoomLevel);
  conf.setInt(TILE_SIZE, tileSize);
  if (bounds != null)
  {
    conf.set(BOUNDS, bounds.toCommaString());
  }
  conf.set(PROVIDER_PROPERTY_KEY, ProviderProperties.toDelimitedString(inputProviderProperties));
}
}
