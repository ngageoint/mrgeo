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

package org.mrgeo.hdfs.ingest.format;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.io.AbstractGridCoverage2DReader;
import org.mrgeo.image.MrsImageException;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.mrgeo.image.MrsImagePyramidMetadata.Classification;
import org.mrgeo.image.geotools.GeotoolsRasterUtils;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.utils.HadoopUtils;
import org.mrgeo.utils.LongRectangle;
import org.mrgeo.utils.TMSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.image.Raster;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

public class IngestImageSplittingRecordReader extends RecordReader<TileIdWritable, RasterWritable>
{
  private static Logger log = LoggerFactory.getLogger(IngestImageSplittingRecordReader.class);

  private long currentTx;
  private long currentTy;

  private long endTx;
  private long endTy;

  private long minTx;
  // private long minTy;

  private long maxTx;
  // private long maxTy;

  private int zoomlevel;

  private float currentTile; // used for progress ONLY
  private float totalTiles; // used for progress ONLY

  private AbstractGridCoverage2DReader reader = null;
  private GridCoverage2D image = null;

  private int tilesize = -1;
  private Classification classification = null;
  private Double nodata = null;
  double[] defaults = null;

  private TileIdWritable key = new TileIdWritable();

  private RasterWritable value;

  // for TESTING ONLY
  static int tilecnt = 0;

  public IngestImageSplittingRecordReader()
  {
//    HdfsImageInputStreamSpi.orderInputStreamProviders();

  }
  @Override
  public void close()
  {
    try
    {
      GeotoolsRasterUtils.closeStreamFromReader(reader);
    }
    catch (final Exception e)
    {
      throw new RuntimeException(e);
    }
  }

  @Override
  public TileIdWritable getCurrentKey() throws IOException, InterruptedException
  {
    return key;
  }

  @Override
  public RasterWritable getCurrentValue() throws IOException, InterruptedException
  {
    return value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException
  {
    return currentTile / totalTiles;
  }

  @Override
  public void initialize(final InputSplit split, final TaskAttemptContext context)
      throws IOException, InterruptedException
  {

    if (!(split instanceof IngestImageSplit))
    {
      throw new IOException(
          "InputSplit for IngestImageRecordReader needs to be (or derived from) IngestImageSplit");
    }

    final IngestImageSplit isplit = (IngestImageSplit) split;

    final Configuration conf = context.getConfiguration();
    try
    {
      //metadata = HadoopUtils.getMetadata(conf);
      Map<String, MrsImagePyramidMetadata> meta = HadoopUtils.getMetadata(context.getConfiguration());
      if (!meta.isEmpty())
      {
        MrsImagePyramidMetadata metadata =  meta.values().iterator().next();
        tilesize = metadata.getTilesize();
        classification = metadata.getClassification();
        nodata = metadata.getDefaultValueDouble(0);
      }
    }
    catch (ClassNotFoundException e)
    {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    if (tilesize < 0)
    {
      tilesize = conf.getInt("tilesize", -1);
      if (tilesize < 0)
      {
        throw new MrsImageException(
            "Error, no \"tilesize\" or \"metadata\" parameter in configuration, tilesize needs to be calculated & set before map/reduce");
      }
    }
    
    if (classification == null)
    {
      String cl = conf.get("classification", null);
      if (cl == null)
      {
        throw new MrsImageException(
            "Error, no \"classification\" or \"metadata\" parameter in configuration, classification needs to be calculated & set before map/reduce");
      }
      classification = Classification.valueOf(cl);
    }
    
    if (nodata == null)
    {
      String nd = conf.get("nodata", null);
      if (nd == null)
      {
        throw new MrsImageException(
            "Error, no \"nodata\" or \"metadata\" parameter in configuration, nodata needs to be calculated & set before map/reduce");
      }
      nodata = Double.parseDouble(nd);
    }

    zoomlevel = isplit.getZoomlevel();

    final String input = isplit.getFilename();
    log.info("processing: " + input);

    reader = GeotoolsRasterUtils.openImage(input);

    if (zoomlevel < 1)
    {
      try
      {
        zoomlevel = GeotoolsRasterUtils.calculateZoomlevel(reader, tilesize);
      }
      catch (final Exception e)
      {
        throw new IOException("Error calculating zoomlevel", e);
      }
    }

    if (reader != null)
    {
      image = GeotoolsRasterUtils.getImageFromReader(reader, "EPSG:4326");

      // currentTx = minTx;
      // currentTy = minTy;
      //
      // currentTile = 0;
      // totalTiles = (maxTx + 1 - minTx) * (maxTy + 1 - minTy);
      currentTx = isplit.getStartTx();
      currentTy = isplit.getStartTy();

      endTx = isplit.getEndTx();
      endTy = isplit.getEndTy();

      final LongRectangle b = isplit.getImageBounds();
      minTx = b.getMinX();
      // minTy = b.getMinY();
      maxTx = b.getMaxX();
      // maxTy = b.getMaxY();

      currentTile = 0;
      totalTiles = isplit.getTotalTiles();
      
      defaults = new double[image.getNumSampleDimensions()];
      Arrays.fill(defaults, nodata);
    }
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException
  {
    // long startTime = System.currentTimeMillis();

    if (currentTx > maxTx)
    {
      currentTx = minTx;
      currentTy++;
    }

    // one row too high...
    if (currentTy > endTy)
    {
      return false;
    }
    // on the end row, and past the last tx;
    else if (currentTy == endTy && currentTx > endTx)
    {
      return false;
    }

    boolean categorical = (classification == Classification.Categorical);

    final Raster raster;
      raster = GeotoolsRasterUtils.cutTile(image, currentTx, currentTy, zoomlevel,
          tilesize, defaults, categorical);

    key = new TileIdWritable(TMSUtils.tileid(currentTx, currentTy, zoomlevel));
    value = RasterWritable.toWritable(raster);

    currentTx++;
    currentTile++;

    // long endTime = System.currentTimeMillis();
    // System.out.println("Tile read time: " + (endTime - startTime));
    return true;
  }

}
