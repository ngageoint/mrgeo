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

package org.mrgeo.data.tile;

import org.mrgeo.utils.Bounds;

public class TiledOutputFormatContext
{
  private String output;
  private int zoomlevel;
  private int tilesize;
  private int bands;
  private Bounds bounds;
  private int tiletype;
  final private boolean calculatePartitions;
  private String name;
  final private boolean multipleOutputFormat;

  /**
   * This constructor is used when producing a single image output.
   * 
   * @param output
   */
  public TiledOutputFormatContext(final String output, Bounds bounds, int zoomlevel, int tilesize)
  {
    this.output = output;
    this.bounds = bounds.clone();
    this.zoomlevel = zoomlevel;
    this.tilesize = tilesize;
    // don't have bands, and tiletype, can't calculate partitions based on size
    this.calculatePartitions = false;
    
    this.multipleOutputFormat = false;
  }
  
  public TiledOutputFormatContext(final String output, Bounds bounds, int zoomlevel, int tilesize, int tiletype, int bands)
  {
    this.output = output;
    this.bounds = bounds.clone();
    this.bands = bands;
    this.zoomlevel = zoomlevel;
    this.tilesize =  tilesize;
    this.tiletype = tiletype;
    this.calculatePartitions = true;
    
    this.multipleOutputFormat = false;
  }

  /**
   * This constructor is used for producing multiple outputs to the specified output.
   * It adds "name" as one output to the "output" location.
   * @param output
   * @param name
   * @param keyClass
   * @param valueClass
   */
  public TiledOutputFormatContext(final String output, final String name,
    Bounds bounds, int zoomlevel, int tilesize, int tiletype, int bands)
  {
    this.output = output;
    this.name = name;
    
    this.bounds = bounds.clone();
    this.bands = bands;
    this.zoomlevel = zoomlevel;
    this.tilesize =  tilesize;
    this.tiletype = tiletype;

    
    // don't have tilesize, bands, and tiletype, can't calculate partitions based on size
    this.calculatePartitions = false;

    this.multipleOutputFormat = true;
  }
  /**
   * This constructor is used for producing multiple outputs to the specified output.
   * It adds "name" as one output to the "output" location.
   * @param output
   * @param name
   * @param keyClass
   * @param valueClass
   */
  public TiledOutputFormatContext(final String output, final String name,
    Bounds bounds, int zoomlevel, int tilesize)
  {
    this.output = output;
    this.name = name;
    
    this.bounds = bounds.clone();
    this.zoomlevel = zoomlevel;
    this.tilesize =  tilesize;

    // don't have tilesize, bands, and tiletype, can't calculate partitions based on size
    this.calculatePartitions = false;

    this.multipleOutputFormat = true;
  }

  public String getOutput()
  {
    return output;
  }

  public String getName()
  {
    return name;
  }
  
  public int getZoomlevel()
  {
    return zoomlevel;
  }
  
  public Bounds getBounds()
  {
    return bounds;
  }
  
  public int getBands()
  {
    return bands;
  }
  
  public int getTilesize()
  {
    return tilesize;
  }

  public int getTiletype()
  {
    return tiletype;
  }
  
  public boolean isCalculatePartitions()
  {
    return calculatePartitions;
  }

  public boolean isMultipleOutputFormat()
  {
    return multipleOutputFormat;
  }
}
