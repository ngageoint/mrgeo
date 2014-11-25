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

package org.mrgeo.buildpyramid;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ReflectionUtils;
import org.mrgeo.aggregators.Aggregator;
import org.mrgeo.aggregators.MeanAggregator;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.mrgeo.mapreduce.formats.TileCollection;
import org.mrgeo.tile.TileIdZoomWritable;
import org.mrgeo.data.raster.RasterUtils;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.utils.HadoopUtils;
import org.mrgeo.utils.TMSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.image.Raster;
import java.awt.image.WritableRaster;
import java.io.IOException;
import java.util.Map;

public class BuildPyramidMapper extends Mapper<TileIdWritable, TileCollection<Raster>, TileIdZoomWritable, RasterWritable>
{
  @SuppressWarnings("unused")
  private static Logger log = LoggerFactory.getLogger(BuildPyramidMapper.class);

  private Counter tileCounter = null;

  private int tolevel;  // lowest level for the image we're building
  private int fromlevel;  // base level of the existing image

  private MrsImagePyramidMetadata metadata;
  private Aggregator aggregator;

  @SuppressWarnings("rawtypes")
  @Override
  public void setup(Mapper.Context context) 
  {
    Configuration conf = context.getConfiguration();

    tolevel = conf.getInt(BuildPyramidDriver.TO_LEVEL, 0);
    fromlevel = conf.getInt(BuildPyramidDriver.FROM_LEVEL, 0);

    try
    {
      Map<String, MrsImagePyramidMetadata> meta = HadoopUtils.getMetadata(context.getConfiguration());
      metadata =  meta.values().iterator().next();

      aggregator = (Aggregator)
          ReflectionUtils.newInstance(conf.getClass(BuildPyramidDriver.AGGREGATOR, MeanAggregator.class), conf);

    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    tileCounter = context.getCounter("Build Pyramid Mapper", "Source Tiles Processed");
  }


  @Override
  public void map(TileIdWritable key, TileCollection<Raster> value, Context context) throws IOException, InterruptedException
  {
    // calculate the parent tolevel tile id
    TMSUtils.Tile t = TMSUtils.tileid(key.get(), fromlevel);
    TMSUtils.Bounds bounds = TMSUtils.tileBounds(t.tx, t.ty, fromlevel, metadata.getTilesize());

    Raster raster = value.get();
    final int tilesize = metadata.getTilesize();
    tileCounter.increment(1);

    
    TileIdZoomWritable fromkey = new TileIdZoomWritable(key.get(), fromlevel);

    for (int level = fromlevel - 1; level >= tolevel; level--)
    {
      // create a compatible writable raster
      WritableRaster toraster  = raster.createCompatibleWritableRaster(raster.getWidth() / 2, raster.getHeight() / 2);

      RasterUtils.decimate(raster, toraster, aggregator, metadata);

      TMSUtils.Tile pt = TMSUtils.latLonToTile(bounds.s, bounds.w, level, tilesize);
      TileIdZoomWritable tokey = new TileIdZoomWritable(TMSUtils.tileid(pt.tx, pt.ty, level), level);
      
      RasterWritable rw = RasterWritable.toWritable(toraster, fromkey);

      context.write(tokey, rw);

      raster = toraster;

      }

  }

}

