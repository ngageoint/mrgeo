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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mrgeo.data.KVIterator;
import org.mrgeo.data.tile.MrsTileException;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.hdfs.tile.HdfsMrsTileReader;
import org.mrgeo.hdfs.tile.HdfsTileResultScanner;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.utils.LongRectangle;

import java.io.IOException;

public class HdfsMrsVectorReader extends HdfsMrsTileReader<VectorTile, VectorTileWritable>
  implements MrsVectorPyramidMetadataProvider
{
  private int zoomLevel = -1;
  private int tileSize = -1;

  public class MrsVectorResultScanner extends HdfsTileResultScanner<VectorTile, VectorTileWritable>
  {
    public MrsVectorResultScanner(final LongRectangle bounds,
      final HdfsMrsTileReader<VectorTile, VectorTileWritable> reader)
    {
      super(bounds, reader);
    }

    public MrsVectorResultScanner(final TileIdWritable startKey, final TileIdWritable endKey,
      final HdfsMrsTileReader<VectorTile, VectorTileWritable> reader)
    {
      super(startKey, endKey, reader);
    }

    @Override
    protected VectorTile toNonWritable(final VectorTileWritable val) throws IOException
    {
      return VectorTileWritable.toMrsVector(val);
    }
  }

  public HdfsMrsVectorReader(final String path) throws IOException
  {
    super(path, 1);
  }

  /**
   * The will locate the metadata for the data pyramid
   * 
   * @param base
   *          is the base directory of the pyramid in HDFS.
   * @return Path to the metadata file in HDFS.
   * @throws IOException
   */
  private static Path findMetadata(final Path base) throws IOException
  {
    final FileSystem fs = HadoopFileUtils.getFileSystem(base);
    Path meta = new Path(base, "metadata");

    // metadata file exists at this level
    if (fs.exists(meta))
    {
      return meta;
    }

    // try one level up (for a single map file case)
    meta = new Path(base, "../metadata");
    if (fs.exists(meta))
    {
      return meta;
    }

    // try two levels up (for the multiple map file case)
    meta = new Path(base, "../../metadata");
    if (fs.exists(meta))
    {
      return meta;
    }

    return null;
  } // end findMetadata

  @Override
  public KVIterator<TileIdWritable, VectorTile> get(final LongRectangle tileBounds)
  {
    return new MrsVectorResultScanner(tileBounds, this);
  }

  @Override
  public KVIterator<TileIdWritable, VectorTile> get(final TileIdWritable startKey,
    final TileIdWritable endKey)
  {
    return new MrsVectorResultScanner(startKey, endKey, this);
  } // end get

  /**
   * Load the MrsImagePyramidMetadata object the data store
   */
  @Override
  public MrsVectorPyramidMetadata loadMetadata()
  {
    // prepare the return object
    final MrsVectorPyramidMetadata metadata;

    try
    {
      // get the path for the metadata object
      final Path metapath = findMetadata(getTilePath());

      // load the file from HDFS
      metadata = MrsVectorPyramidMetadata.load(metapath);

    }
    catch (final IOException e)
    {
      throw new MrsTileException(e);
    }

    return metadata;
  } // end loadMetadata

  @Override
  protected int getWritableSize(final VectorTileWritable val)
  {
    return val.getSize();
  }

  @Override
  protected VectorTile toNonWritable(final VectorTileWritable val) throws IOException
  {
    return VectorTileWritable.toMrsVector(val);
  }

  @Override
  public int getZoomlevel()
  {
    if (zoomLevel < 0)
    {
      MrsVectorPyramidMetadata metadata = loadMetadata();
      zoomLevel = metadata.getMaxZoomLevel();
    }
    return zoomLevel;
  }

  @Override
  public int getTileSize()
  {
    if (tileSize < 0)
    {
      MrsVectorPyramidMetadata metadata = loadMetadata();
      tileSize = metadata.getTilesize();
    }
    return tileSize;
  }
}
