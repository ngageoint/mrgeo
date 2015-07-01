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

package org.mrgeo.vector.mrsvector;

import org.apache.hadoop.mapreduce.RecordReader;
import org.mrgeo.data.tile.MrsTileReader;
import org.mrgeo.data.tile.TileDataProvider;
import org.mrgeo.data.tile.TileIdWritable;

import java.awt.image.Raster;

public abstract class MrsVectorDataProvider extends TileDataProvider<Raster>
{
  protected MrsVectorDataProvider()
  {
    super();
  }

  public MrsVectorDataProvider(final String resourceName)
  {
    super(resourceName);
  }

  /**
   * Return an instance of a RecordReader class to be used in map/reduce
   * jobs for reading tiled data.
   * 
   * @return
   */
  public abstract RecordReader<TileIdWritable, VectorTileWritable> getRecordReader();

  /**
   * Return an instance of a MrsTileReader class to be used for reading
   * tiled data. This method may be invoked by callers regardless of
   * whether they are running within a map/reduce job or not.
   * 
   * @return
   */
  public abstract MrsTileReader<VectorTile> getMrsTileReader();
}
