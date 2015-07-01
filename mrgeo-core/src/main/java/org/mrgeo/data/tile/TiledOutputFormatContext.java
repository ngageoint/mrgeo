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
  }


  public String getOutput()
  {
    return output;
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

}
