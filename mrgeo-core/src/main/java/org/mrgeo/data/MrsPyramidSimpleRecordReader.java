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

package org.mrgeo.data;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.data.tile.TiledInputFormatContext;
import org.mrgeo.mapreduce.splitters.MrsPyramidInputSplit;
import org.mrgeo.pyramid.MrsPyramidMetadata;
import org.mrgeo.utils.Bounds;
import org.mrgeo.utils.TMSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public abstract class MrsPyramidSimpleRecordReader<T, TWritable> extends RecordReader<TileIdWritable, TWritable>
{
  private class TileSplit
  {
    public TileSplit()
    {
    }
    public long currentTx;
    public long currentTy;

    public long startTx;

    public long endTx;
    public long endTy;

    public T tile;
    public long id;

  }

  /**
   * A record reader that always returns a blank tile of imagery for every tile within
   * the split it is initialized with. When an image is used as input with a crop bounds
   * and in fill mode, the MrsPyramidInputFormat manufactures splits to includes the tiles
   * that the input image itself does not have tile data for. This record reader is used
   * for those manufactured splits to return blank tiles for them.
   */
  private class AllBlankTilesRecordReader extends RecordReader<TileIdWritable, TWritable>
  {
    private long nextTileId;
    private long endTileId;
    private long tileCount;
    private long totalTiles;
    private TileIdWritable currentKey;
    private TWritable currentValue;
    private T blankValue;
    private TiledInputFormatContext ifContext;
    private TMSUtils.TileBounds cropBounds;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException,
            InterruptedException
    {
      if (split instanceof MrsPyramidInputSplit)
      {
        MrsPyramidInputSplit mrsPyramidInputSplit = (MrsPyramidInputSplit)split;
        nextTileId = mrsPyramidInputSplit.getWrappedSplit().getStartTileId();
        endTileId = mrsPyramidInputSplit.getWrappedSplit().getEndTileId();
        totalTiles = endTileId - nextTileId + 1;
        ifContext = TiledInputFormatContext.load(context.getConfiguration());
        Bounds requestedBounds = ifContext.getBounds();
        if (requestedBounds == null)
        {
          requestedBounds = new Bounds();
        }
        cropBounds = TMSUtils.boundsToTile(requestedBounds.getTMSBounds(),
                ifContext.getZoomLevel(), ifContext.getTileSize());
      }
      else
      {
        throw new IOException("Invalid InputSplit passed to AllBlankTilesRecordReader.initialize: " + split.getClass().getName());
      }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException
    {
      TMSUtils.Tile t = TMSUtils.tileid(nextTileId, zoomLevel);
      while (!cropBounds.contains(t) && nextTileId < endTileId)
      {
        nextTileId++;
        tileCount++;
        t = TMSUtils.tileid(nextTileId, zoomLevel);
      }
      if (nextTileId > endTileId)
      {
        return false;
      }
      currentKey = new TileIdWritable(nextTileId);
      currentValue = toWritable(getBlankTile());
      nextTileId++;
      tileCount++;
      return true;
    }

    @Override
    public TileIdWritable getCurrentKey() throws IOException, InterruptedException
    {
      return currentKey;
    }

    @Override
    public TWritable getCurrentValue() throws IOException, InterruptedException
    {
      return currentValue;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException
    {
      return ((float)tileCount / (float)totalTiles);
    }

    @Override
    public void close() throws IOException
    {
      // Nothing to close
    }

    private T getBlankTile()
    {
      if (blankValue == null)
      {
        blankValue = createBlankTile(ifContext.getFillValue());
      }
      return blankValue;
    }
  }

  /**
   * A wrapper record reader used within the MrsPyramidRecordReader to handle the case
   * where the caller specified a bounds as well as wanting to return empty tiles
   * in cases where the input image does not have imagery.
   *
   * This class iterates over all the tiles in the split. It also reads records
   * from the delegate sequentially. For each tile iterated, if the delegate provides
   * a tile record, that is returned, otherwise a blank tile is returned.
   *
   * Splits passed to this record reader must be instances of MrsPyramidInputSplit
   * because that is where the tile bounds for iteration is obtained. The input
   * format that uses this record reader is responsible for creating the proper
   * splits.
   *
   * If empty tiles are not to be included, then this record reader is not used.
   */
  private class AllTilesRecordReader extends RecordReader<TileIdWritable, TWritable>
  {
    private RecordReader<TileIdWritable, TWritable> delegate;
    private TiledInputFormatContext ifContext;
    private long nextTileId;
    private long endTileId;
    private long totalTiles;
    private long tileCount;
    private TMSUtils.TileBounds cropBounds;
    private TileIdWritable currentKey;
    private TWritable currentValue;
    private T blankValue;
    private TileIdWritable delegateKey;

    @Override
    public void close() throws IOException
    {
      delegate.close();
    }

    @Override
    public TileIdWritable getCurrentKey() throws IOException, InterruptedException
    {
      return currentKey;
    }

    @Override
    public TWritable getCurrentValue() throws IOException, InterruptedException
    {
      return currentValue;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException
    {
      return ((float)tileCount / (float)totalTiles);
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException,
            InterruptedException
    {
      if (split instanceof MrsPyramidInputSplit)
      {
        delegate = getRecordReader(((MrsPyramidInputSplit)split).getName(),
                context.getConfiguration());
        delegate.initialize(((MrsPyramidInputSplit) split).getWrappedSplit(), context);
        ifContext = TiledInputFormatContext.load(context.getConfiguration());
        Bounds requestedBounds = ifContext.getBounds();
        if (requestedBounds == null)
        {
          requestedBounds = new Bounds();
        }
        cropBounds = TMSUtils.boundsToTile(requestedBounds.getTMSBounds(),
                ifContext.getZoomLevel(), ifContext.getTileSize());
        endTileId = ((MrsPyramidInputSplit)split).getWrappedSplit().getEndTileId();
        nextTileId = ((MrsPyramidInputSplit)split).getWrappedSplit().getStartTileId();
        totalTiles = endTileId - nextTileId + 1;
        blankValue = null;
      }
      else
      {
        throw new IOException("Invalid split type " + split.getClass().getCanonicalName() +
                ". Expected " + MrsPyramidInputSplit.class.getCanonicalName());
      }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException
    {
      TMSUtils.Tile t = TMSUtils.tileid(nextTileId, zoomLevel);
      while (!cropBounds.contains(t) && nextTileId < endTileId)
      {
        nextTileId++;
        tileCount++;
        t = TMSUtils.tileid(nextTileId, zoomLevel);
      }
      if (nextTileId > endTileId)
      {
        return false;
      }
      currentKey = new TileIdWritable(nextTileId);
      // See if the key from the delegate matches the current tile.
      if (delegateKey == null)
      {
        // Since native splits can begin with tiles that precede the nextTileId
        // that is required, we need to skip over tiles returned from the
        // delegate if they are less than the nextTileId. This can happen when
        // the crop bounds are near the right and/or top sides of a source image.
        boolean delegateResult = delegate.nextKeyValue();
        while (delegateResult)
        {
          TileIdWritable tempKey = delegate.getCurrentKey();
          if (tempKey != null && tempKey.get() >= nextTileId)
          {
            delegateKey = delegate.getCurrentKey();
            break;
          }
          delegateResult = delegate.nextKeyValue();
        }
      }
      // If the current tile does not exist in the delegate, then return
      // a blank value, otherwise return the delegate's tile.
      if (delegateKey != null && delegateKey.equals(currentKey))
      {
        currentValue = delegate.getCurrentValue();
        // We've returned the current delegate key, so set the delegateKey
        // to null in order to trigger a call to delegate.nextKeyValue() the
        // next time this method is called.
        delegateKey = null;
      }
      else
      {
        currentValue = toWritable(getBlankTile());
      }
      nextTileId++;
      tileCount++;
      return true;
    }

    private T getBlankTile()
    {
      if (blankValue == null)
      {
        blankValue = createBlankTile(ifContext.getFillValue());
      }
      return blankValue;
    }
  }

  private static final Logger log = LoggerFactory.getLogger(MrsPyramidSimpleRecordReader.class);
  private RecordReader<TileIdWritable, TWritable> scannedInputReader;
  private TiledInputFormatContext ifContext;
  private TileIdWritable key;
  private TWritable value;
  private Bounds inputBounds = Bounds.world; // bounds of the map/reduce (either the image bounds or cropped though map algebra)

  private int tilesize;
  private int zoomLevel;

  protected abstract T toNonWritableTile(TWritable tileValue) throws IOException;

  protected abstract Map<String, MrsPyramidMetadata> readMetadata(final Configuration conf)
          throws ClassNotFoundException, IOException;

  protected abstract T createBlankTile(final double fill);
  protected abstract TWritable toWritable(T val) throws IOException;
  protected abstract TWritable copyWritable(TWritable val);

  protected abstract RecordReader<TileIdWritable,TWritable> getRecordReader(final String input,
                                                                            final Configuration conf) throws DataProviderNotFound;

  private RecordReader<TileIdWritable,TWritable> createRecordReader(
          final MrsPyramidInputSplit split, final TaskAttemptContext context)
          throws DataProviderNotFound, IOException
  {
    InputSplit initializeWithSplit = null;
    RecordReader<TileIdWritable,TWritable> recordReader = null;
    if (ifContext.getIncludeEmptyTiles())
    {
      if (split.getWrappedSplit().getWrappedSplit() == null)
      {
        recordReader = new AllBlankTilesRecordReader();
      }
      else
      {
        recordReader = new AllTilesRecordReader();
      }
      // The all tiles record readers need the MrsPyramidInputSplit which
      // wraps the native split returned from the data plugin.
      initializeWithSplit = split;
    }
    else
    {
      // The standard record reader needs the native split returned from
      // the data plugin.
      recordReader = getRecordReader(split.getName(), context.getConfiguration());
      initializeWithSplit = split.getWrappedSplit();
    }
    try
    {
      recordReader.initialize(initializeWithSplit, context);
    }
    catch(Throwable t)
    {
      throw new IOException(t);
    }
    return recordReader;
  }

  @Override
  public void close() throws IOException
  {
    if (scannedInputReader != null)
    {
      scannedInputReader.close();
    }
  }

  @Override
  public TileIdWritable getCurrentKey()
  {
    return key;
  }

  @Override
  public TWritable getCurrentValue()
  {
    return value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException
  {
    return scannedInputReader.getProgress();
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException,
          InterruptedException
  {
    if (split instanceof MrsPyramidInputSplit)
    {
      final MrsPyramidInputSplit fsplit = (MrsPyramidInputSplit) split;
      final Configuration conf = context.getConfiguration();

      ifContext = TiledInputFormatContext.load(context.getConfiguration());
      if (ifContext.getBounds() != null)
      {
        inputBounds = ifContext.getBounds();
      }
      scannedInputReader = createRecordReader(fsplit, context);
      final String pyramidName = fsplit.getName();

      try
      {
        final Map<String, MrsPyramidMetadata> metadataMap = readMetadata(conf);
        MrsPyramidMetadata metadata = metadataMap.get(pyramidName);
        if (metadata == null)
        {
//          final String unqualifiedPyramidName = HadoopFileUtils.unqualifyPath(pyramidPath)
//              .toString();
          metadata = metadataMap.get(pyramidName);
        }
        if (metadata == null)
        {
          throw new IOException(
                  "Internal error: Unable to fetch pyramid metadata from job config for " +
                          pyramidName);
        }
        tilesize = metadata.getTilesize();
        zoomLevel = fsplit.getZoomlevel();
      }
      catch (final ClassNotFoundException e)
      {
        e.printStackTrace();
        throw new IOException(e);
      }
    }
    else
    {
      throw new IOException("Wrong InputSplit type: " + split.getClass().getName());
    }
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException
  {
    while (scannedInputReader.nextKeyValue())
    {
      final long id = scannedInputReader.getCurrentKey().get();
//      log.info("scannedInputReader returned key " + id);

      final TMSUtils.Tile tile = TMSUtils.tileid(id, zoomLevel);
      final TMSUtils.Bounds tb = TMSUtils.tileBounds(tile.tx, tile.ty, zoomLevel, tilesize);
      if (inputBounds.intersects(tb.w, tb.s, tb.e, tb.n))
      {
        // RasterWritable.toRaster(scannedInputReader.getCurrentValue())
        setNextKeyValue(id, scannedInputReader.getCurrentValue());
//        log.info("Returning at point 3 after " + (System.currentTimeMillis() - start));
        return true;
      }
    }
//    log.info("Returning at point 4 after " + (System.currentTimeMillis() - start));
    return false;
  }

  private void setNextKeyValue(final long tileid, final TWritable tileValue)
  {
//    long start = System.currentTimeMillis();
//    log.info("setNextKeyValue");
//    try
//    {
//      TMSUtils.Tile t = TMSUtils.tileid(tileid, zoomLevel);
//      QuickExport.saveLocalGeotiff("/export/home/dave.johnson/splits", raster, t.tx, t.ty,
//          zoomLevel, tilesize, -9999);
//    }
//    catch (NoSuchAuthorityCodeException e)
//    {
//      e.printStackTrace();
//    }
//    catch (IOException e)
//    {
//      e.printStackTrace();
//    }
//    catch (FactoryException e)
//    {
//      e.printStackTrace();
//    }
    key = new TileIdWritable(tileid);
    // The copy operation is required below for Spark RDD creation to prevent all the
    // raster tiles in an RDD (for one split) looking like the last tile in the split.
    value = copyWritable(tileValue);
//    log.info("After setNextKeyValue took" + (System.currentTimeMillis() - start));
  }
}
