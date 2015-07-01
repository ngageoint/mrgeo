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

package org.mrgeo.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.mrgeo.image.ImageStats;
import org.mrgeo.mapreduce.formats.TileCollection;
import org.mrgeo.opimage.MrsPyramidOpImage;
import org.mrgeo.opimage.TileLocator;
import org.mrgeo.rasterops.OpImageRegistrar;
import org.mrgeo.rasterops.OpImageUtils;
import org.mrgeo.data.raster.RasterUtils;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.utils.Base64Utils;
import org.mrgeo.utils.TMSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.media.jai.JAI;
import javax.media.jai.PlanarImage;
import javax.media.jai.RenderedOp;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;
import java.io.IOException;
import java.util.*;

public class OpChainMapper extends Mapper<TileIdWritable, TileCollection<Raster>, TileIdWritable, RasterWritable>
{
  public static final String ZOOM_LEVEL = "OpChainMapper.zoomLevel";
  public static final String TILE_SIZE = "OpChainMapper.tileSize";
  private static final String OPERATION = "OpChainMapper.Operation";

  private static Logger log = LoggerFactory.getLogger(OpChainMapper.class);
  private ImageStats[] stats;
//  private Map<String, MrsImagePyramidMetadata> metadata;
  private int zoomLevel;
  private int tileSize;

  RenderedOp optree;

  Map<String, List<MrsPyramidOpImage>> tileInputs;
  List<TileLocator> tileLocators;

  public static void setOperation(final Configuration conf, final Object operation) throws IOException
  {
    final String encodedOperation = Base64Utils.encodeObject(operation);
    conf.set(OPERATION, encodedOperation);
  }

  public static RenderedOp getOperation(final Configuration conf) throws ClassNotFoundException, IOException
  {
    return (RenderedOp) Base64Utils.decodeToObject(conf.get(OPERATION));
  }

  @Override
  public void setup(Mapper<TileIdWritable, TileCollection<Raster>, TileIdWritable, RasterWritable>.Context context)
  {
    Configuration conf = context.getConfiguration();

    OpImageRegistrar.registerMrGeoOps();
    JAI.disableDefaultTileCache();
    try
    {
      stats = ImageStats.initializeStatsArray(1);
      zoomLevel = conf.getInt(ZOOM_LEVEL, 0);
      tileSize = conf.getInt(TILE_SIZE, 0);

      optree = getOperation(conf);
      
      // Force the OpChain tree to be created - invokes create method on each of the descriptors
      // in the op chain tree.
      optree.getMinX();

      tileInputs = new HashMap<String, List<MrsPyramidOpImage>>();
      tileLocators = new ArrayList<TileLocator>();
      getInputsAndTileLocators(optree, tileInputs, tileLocators);

//      try
//      {
//        metadata = HadoopUtils.getMetadata(context.getConfiguration());
//      }
//      catch (Exception e)
//      {
//        e.printStackTrace();
//        throw new RuntimeException(e);
//      }

      log.debug("OpChain inputs :" + tileInputs.toString());
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
    catch (ClassNotFoundException e)
    {
      e.printStackTrace();
    }

  }

  @Override
  public void map(TileIdWritable key, TileCollection<Raster> value, Context context) throws IOException,
  InterruptedException
  {   
    log.debug("OpChainMapper.map():  tileid: " + key.get());

//    MrsImagePyramidMetadata meta = null;
    
    for (String tile:tileInputs.keySet())
    {
      List<MrsPyramidOpImage> images = tileInputs.get(tile);
      for (MrsPyramidOpImage image: images)
      {
        image.setInputInfo(key.get(), value.getCluster(tile));
      }

//      if (meta == null)
//      {
//        Raster raster = value.get(tile);
//        if (raster != null)
//        {
//          meta = metadata.get(tile);
//        }
//      }
    }

    TMSUtils.Tile tile = TMSUtils.tileid(key.get(), zoomLevel);
    for (TileLocator tl: tileLocators)
    {
      tl.setTileInfo(tile.tx, tile.ty, zoomLevel, tileSize);
    }

    // we may have no image inputs, which means we need to look if an "empty image" has
    // been added to the raster collection and use that for creating an output raster
//    if (meta == null)
//    {
//      Raster raster = value.get(EmptyTileInputFormat.EMPTY_IMAGE);
//      if (raster != null)
//      {
//        meta = metadata.get(EmptyTileInputFormat.EMPTY_IMAGE);
//      }
//    }
    
    SampleModel sm = optree.getSampleModel();
    double noData = OpImageUtils.getNoData(optree,  Double.NaN);
    WritableRaster output = RasterUtils.createEmptyRaster(sm.getWidth(), sm.getHeight(), sm.getNumBands(),
        sm.getDataType(), noData);
    optree.copyData(output);
    
    // compute stats on the tile and update reducer stats
//    if (meta != null)
//    {
//      ImageStats[] tileStats = ImageStats.computeStats(output, meta.getDefaultValues());
      ImageStats[] tileStats = ImageStats.computeStats(output, new double[] { noData });
      stats = ImageStats.aggregateStats(Arrays.asList(stats, tileStats));
//    }
    
    final RasterWritable rw = RasterWritable.toWritable(output);
    context.write(key, rw);
  }

  private void walkOpTree(RenderedImage op, Map<String, List<MrsPyramidOpImage>> sources,
      List<TileLocator> tl, int depth)
  {
    // walk the sources
//    String out = "";
//    for (int i = 0; i < depth; i++)
//    {
//      out += " ";
//    }
//    out += op.getClass().getSimpleName();
//    System.out.println(out);

    if (op.getSources() != null)
    {
      for (Object obj: op.getSources())
      {
        if (obj instanceof RenderedImage)
        {
          walkOpTree((RenderedImage)obj, sources, tl, depth+1);
        }
      }
    }
    if (op instanceof RenderedOp)
    {
      RenderedOp ro = (RenderedOp)op;
      PlanarImage image = ro.getCurrentRendering();

      if (image != null)
      {
        log.debug("[" + image.getClass().getSimpleName() + "]");
      }

      if (image instanceof MrsPyramidOpImage)
      {

        MrsPyramidOpImage mrsimage = (MrsPyramidOpImage)image;

        try
        {
          mrsimage.setZoomlevel(zoomLevel);
        }
        catch (IOException e)
        {
          e.printStackTrace();
        }
        String name = mrsimage.getDataProvider().getResourceName();

        if (!sources.containsKey(name))
        {
          sources.put(name, new LinkedList<MrsPyramidOpImage>());
          log.debug("found OpImage for: " + name);
        }

        sources.get(name).add(mrsimage);
      }
      if (image instanceof TileLocator)
      {
        tl.add((TileLocator)image);
      }
      walkOpTree(image, sources, tl, depth+1);
    }
  }

  private void getInputsAndTileLocators(RenderedImage op,
      Map<String, List<MrsPyramidOpImage>> sources,
      List<TileLocator> tl)
  {
    walkOpTree(op, sources, tl, 0);
  }

  @Override
  public void cleanup(Context context) throws IOException, InterruptedException
  {
    RasterWritable value = RasterWritable.toWritable(ImageStats.statsToRaster(stats));
    TileIdWritable key = new TileIdWritable(ImageStats.STATS_TILE_ID);
    
    context.write(key, value);
  }
}
