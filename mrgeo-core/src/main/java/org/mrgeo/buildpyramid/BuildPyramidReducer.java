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
import org.apache.hadoop.mapreduce.Reducer;
import org.mrgeo.hadoop.multipleoutputs.DirectoryMultipleOutputs;
import org.mrgeo.image.ImageStats;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.mrgeo.tile.TileIdZoomWritable;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.adhoc.AdHocDataProvider;
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
import java.util.HashMap;
import java.util.Map;

public class BuildPyramidReducer extends
Reducer<TileIdZoomWritable, RasterWritable, TileIdWritable, RasterWritable>
{
  private static Logger log = LoggerFactory.getLogger(BuildPyramidReducer.class);

  private Counter totalTiles = null;
  private Counter newTiles = null;

  private Map<Integer, ImageStats[]> stats; // stats computed on the reducer tiles for the child image

  private MrsImagePyramidMetadata metadata;

  private DirectoryMultipleOutputs<TileIdWritable, RasterWritable> outputs;
  //  private TileIdPartitioner<TileIdWritable, RasterWritable> partitioner = null;
  //  private PartitionsWritten written = new PartitionsWritten();

  @Override
  public void reduce(final TileIdZoomWritable key, final Iterable<RasterWritable> it,
    final Context context) throws IOException, InterruptedException
    {
    WritableRaster toraster = null;

    final int tilesize = metadata.getTilesize();

    final int tolevel = key.getZoom();
    final TMSUtils.Tile pt = TMSUtils.tileid(key.get(), tolevel);
    final TMSUtils.Bounds pbounds = TMSUtils.tileBounds(pt.tx, pt.ty, tolevel, tilesize);

    // calculate the starting pixel for the to-tile (make sure to use the NW coordinate)
    final TMSUtils.Pixel corner = TMSUtils.latLonToPixelsUL(pbounds.n, pbounds.w, tolevel, tilesize);

    final TileIdZoomWritable payload = new TileIdZoomWritable();

    for (final RasterWritable raster : it)
    {
      totalTiles.increment(1);

      final Raster fromraster = RasterWritable.toRaster(raster, payload);

      // create a compatible writable raster
      if (toraster == null)
      {
        toraster = fromraster.createCompatibleWritableRaster(tilesize, tilesize);
        RasterUtils.fillWithNodata(toraster, metadata);
      }

      // get the from-tile's tileid & level from the payload
      final long fromid = payload.get();
      final int fromlevel = payload.getZoom();

      final TMSUtils.Tile t = TMSUtils.tileid(fromid, fromlevel);
      final TMSUtils.Bounds bounds = TMSUtils.tileBounds(t.tx, t.ty, fromlevel, tilesize);

      // calculate the starting pixel for the from-tile (make sure to use the NW coordinate)
      final TMSUtils.Pixel start = TMSUtils.latLonToPixelsUL(bounds.n, bounds.w, tolevel,
        tilesize);

      log.debug("from  tx: " + t.tx + " ty: " + t.ty + " (" + fromlevel + ") x: " + (start.px - corner.px) + " y: " + (start.py - corner.py) + " w: " + fromraster.getWidth() + " h: " + fromraster.getHeight());

      int tox = (int) (start.px - corner.px);
      int toy = (int) (start.py - corner.py);

      toraster.setDataElements(tox, toy, fromraster);
    }

    final RasterWritable value = RasterWritable.toWritable(toraster);

    newTiles.increment(1);

    //    if (partitioner != null)
    //    {
    //      int zoom = key.getZoom();
    //      int partition = partitioner.getPartitionNoOffset(key);
    //
    //      written.add(zoom, partition);
    //    }

    // write the tile
    outputs.write(Integer.toString(key.getZoom()), key, value);

    if (!stats.containsKey(tolevel))
    {
      stats.put(tolevel, ImageStats.initializeStatsArray(metadata.getBands()));
    }
    ImageStats[] s = stats.get(tolevel);
    ImageStats.computeAndUpdateStats(s, toraster, metadata.getDefaultValues());
    }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  public void setup(final Reducer.Context context)
  {
    outputs = new DirectoryMultipleOutputs(context);

    totalTiles = context.getCounter("Build Pyramid Reducer", "Tiles Processed");
    newTiles = context.getCounter("Build Pyramid Reducer", "Tiles Created");

    try
    {
      Map<String, MrsImagePyramidMetadata> meta = HadoopUtils.getMetadata(context.getConfiguration());
      metadata =  meta.values().iterator().next();
    }
    catch (final Exception e)
    {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    //Initialize ImageStats array
    stats = new HashMap<Integer, ImageStats[]>();

    //    stats = ImageStats.initializeStatsArray(metadata.getBands());

//    try
//    {
//      Class partitionerClass = context.getPartitionerClass();
//
//      if (partitionerClass == TileIdPartitioner.class)
//      {
//        partitioner = new TileIdPartitioner<TileIdWritable, RasterWritable>();
//        partitioner.setConf(context.getConfiguration());
//      }
//    }
//    catch (ClassNotFoundException e)
//    {
//      e.printStackTrace();
//    }
  }

  @Override
  public void cleanup(Context context) throws IOException
  {
    // close the multiple outputs.
    try
    {
      outputs.close();
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
    catch (InterruptedException e)
    {
      e.printStackTrace();
    }

    Configuration conf = context.getConfiguration();


    // write stats.
    for (int level: stats.keySet())
    {
      String name = conf.get(BuildPyramidDriver.ADHOC_PROVIDER + level);
      if (name != null)
      {
        AdHocDataProvider provider = DataProviderFactory.getAdHocDataProvider(name,
            AccessMode.WRITE, conf);

        ImageStats[] s = stats.get(level);
        ImageStats.writeStats(provider, s);
      }
    }

    //    int[] zooms = written.getZooms();
    //    if (zooms != null && zooms.length > 0)
    //    {
    //      for (int zoom: zooms)
    //      {
    //        PartitionsWritten w = new PartitionsWritten();
    //        w.add(zoom, written.getParts(zoom));
    //        
    //        PartitionsWrittenUtils.writeToFile(context, w, zoom);
    //      }
    //    }
  }
}
