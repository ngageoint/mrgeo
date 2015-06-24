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

package org.mrgeo.hdfs.image;

import org.apache.hadoop.fs.Path;
import org.mrgeo.data.KVIterator;
import org.mrgeo.data.image.MrsImagePyramidReaderContext;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.hdfs.tile.HdfsMrsTileReader;
import org.mrgeo.hdfs.tile.HdfsTileResultScanner;
import org.mrgeo.utils.LongRectangle;

import java.awt.image.Raster;
import java.io.IOException;

public class HdfsMrsImageReader extends HdfsMrsTileReader<Raster, RasterWritable>
{
  @SuppressWarnings("unused")
  final private HdfsMrsImageDataProvider provider;
  final private MrsImagePyramidReaderContext context;
  final int tileSize;
  
  
  public HdfsMrsImageReader(HdfsMrsImageDataProvider provider,
    MrsImagePyramidReaderContext context) throws IOException
  {
    super(new Path(provider.getResourcePath(true), "" + context.getZoomlevel()).toString(),
          context.getZoomlevel());
    
    this.provider = provider;
    this.context = context;
    tileSize = provider.getMetadataReader().read().getTilesize();
  }

  @Override
  public int getZoomlevel()
  {
    return context.getZoomlevel();
  }

  @Override
  public int getTileSize()
  {
    return tileSize;
  }

  public class MrsImageResultScanner extends HdfsTileResultScanner<Raster, RasterWritable>
  {
    public MrsImageResultScanner(final LongRectangle bounds,
        final HdfsMrsTileReader<Raster, RasterWritable> reader)
    {
      super(bounds, reader);
    }

    public MrsImageResultScanner(final TileIdWritable startKey, final TileIdWritable endKey,
        final HdfsMrsTileReader<Raster, RasterWritable> reader)
    {
      super(startKey, endKey, reader);
    }

    @Override
    protected Raster toNonWritable(RasterWritable val) throws IOException
    {
      return RasterWritable.toRaster(val);
    }
  }

  @Override
  public KVIterator<TileIdWritable, Raster> get(final LongRectangle tileBounds)
  {
    return new MrsImageResultScanner(tileBounds, this);
  }

  @Override
  public KVIterator<TileIdWritable, Raster> get(final TileIdWritable startKey,
      final TileIdWritable endKey)
  {
    return new MrsImageResultScanner(startKey, endKey, this);
  }


  @Override
  protected int getWritableSize(RasterWritable val)
  {
    return val.getSize();
  }
  
  @Override
  protected Raster toNonWritable(RasterWritable val) throws IOException
  {
    return RasterWritable.toRaster(val);
  }

}
