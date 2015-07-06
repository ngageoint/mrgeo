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

import org.apache.commons.collections.map.LRUMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.mrgeo.mapreduce.formats.TileClusterInfo;
import org.mrgeo.mapreduce.formats.TileCollection;
import org.mrgeo.mapreduce.splitters.MrsPyramidInputSplit;
import org.mrgeo.pyramid.MrsPyramidMetadata;
import org.mrgeo.data.tile.MrsTileReader;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.data.tile.TiledInputFormatContext;
import org.mrgeo.utils.Bounds;
import org.mrgeo.utils.HadoopUtils;
import org.mrgeo.utils.TMSUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * This is the base class Hadoop RecordReader to be used for all pyramid-based
 * input to MrGeo map/reduce jobs. It "wraps" an instance of a RecordReader
 * supplied by the data access layer corresponding to the input source. It is
 * responsible for returning records for map/reduce that are TileCollection
 * objects, while data access layers have no knowledge of TileCollection. The
 * TileCollection will contain the tiles for all of the input pyramids, and
 * it will include the tiles for tile neighborhood defined for the job. It also
 * handles returning empty tiles when the caller requests empty tiles in the
 * TiledInputFormatContext.
 * 
 * @param <T>
 * @param <TWritable>
 */
public abstract class MrsPyramidRecordReader<T, TWritable> extends RecordReader<TileIdWritable, TileCollection<T>>
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

  private RecordReader<TileIdWritable, TWritable> scannedInputReader;
  private MrsTileReader<T> primaryReader;
  private String scannedInput;
  private HashMap<String, MrsTileReader<T>> readers;
  private TiledInputFormatContext ifContext;
  private TileIdWritable key;
  private TileCollection<T> value;

  private Bounds[] preBounds; // bounds of any inputs processed "before" this one
  //private Bounds[] postBounds;
  private int splitZoomLevel;  // zoom level of the split (may be different than the needed level)
  private Bounds inputBounds = Bounds.world; // bounds of the map/reduce (either the image bounds or cropped though map algebra)
  private TMSUtils.TileBounds inputTileBounds;
  private TMSUtils.Tile initialTile;

  // Be careful here.  The "Split" here is used to split up a single tile into multiple smaller tiles,
  // not the Hadoop concept of an InputSplit.  That is also used extensively in this RecordReader.
  private TileSplit tilesplit = null;
  private HashMap<String, Map<TileIdWritable, T> > tilecache;

  private int tilesize;
  private int zoomLevel;
  private TileClusterInfo tileClusterInfo;
  private long[] neighborTileIds;

  protected abstract T splitTile(final T tile, final long id,
      final int zoom,
      final long childTileid, final int childZoomLevel, final int tilesize);

  protected abstract T toNonWritableTile(TWritable tileValue) throws IOException;

  protected abstract Map<String, MrsPyramidMetadata> readMetadata(final Configuration conf)
      throws ClassNotFoundException, IOException;

  protected abstract T createBlankTile(final double fill);
  protected abstract TWritable toWritable(T val) throws IOException;

  protected abstract MrsTileReader<T> getMrsTileReader(final String input, int zoomlevel,
      final Configuration conf) throws DataProviderNotFound, IOException;

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
    if (primaryReader != null)
    {
      primaryReader.close();
    }
    if (readers != null)
    {
      for (final MrsTileReader<T> reader : readers.values())
      {
        reader.close();
      }
    }
  }

  @Override
  public TileIdWritable getCurrentKey()
  {
    return key;
  }

  @Override
  public TileCollection<T> getCurrentValue()
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
      tileClusterInfo = HadoopUtils.getTileClusterInfo(conf);

      readers = new HashMap<String, MrsTileReader<T>>();
      tilecache = new HashMap<String, Map<TileIdWritable, T>>();
      for (final String inputPyramidName : ifContext.getInputs())
      {
        // If it's the same pyramid as us, we instantiate primaryReader so
        // that we can perform a lookup of neighborhood tiles in the primary
        // input as needed. For other inputs, those lookups are done via the
        // readers.
        // if (unqualifiedPyramidName.equals(inputPyramidName))
        if (pyramidName.equals(inputPyramidName))
        {
          scannedInput = inputPyramidName;
          if (tileClusterInfo != null && (tileClusterInfo.getNeighborCount() > 0))
          {
            primaryReader = getMrsTileReader(inputPyramidName,
                ifContext.getZoomLevel(), conf);
          }
        }
        else
        {
          readers.put(inputPyramidName, getMrsTileReader(inputPyramidName,
              ifContext.getZoomLevel(), conf));
        }
      }

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

      if (split instanceof MrsPyramidInputSplit)
      {
//        final MrsPyramidInputSplit mpsplit = (MrsPyramidInputSplit) split;
        preBounds = fsplit.getPreBounds();
        //postBounds = mpsplit.getPostBounds();
        splitZoomLevel = fsplit.getZoomlevel();
      }
      else
      {
        // only need to set this for single images...
        splitZoomLevel = zoomLevel;
      }

      inputTileBounds = TMSUtils.boundsToTile(inputBounds.getTMSBounds(), zoomLevel, tilesize);
      if (tileClusterInfo != null)
      {
        final int neighborCount = tileClusterInfo.getNeighborCount();
        if (neighborCount > 0)
        {
          neighborTileIds = new long[neighborCount];
          // Compute the "center" tile of the neighborhood to be read first. This only
          // applies to the case where a neighborhood of tiles is being read. The initial
          // tile should be positioned so that the bottom left tile of the image is also
          // the bottom left tile of the first neighborhood read.
          initialTile = new TMSUtils.Tile(
              inputTileBounds.w - tileClusterInfo.getOffsetX(),
              inputTileBounds.s - tileClusterInfo.getOffsetY());
        }
      }

      value = new TileCollection<T>();
      key = new TileIdWritable();
    }
    else
    {
      throw new IOException("Wrong InputSplit type: " + split.getClass().getName());
    }
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException
  {
    // this handles the case where we have a scanned input with a zoom level lower than the target
    // and we've already calculated the sub-tiles to use...
    while (tilesplit != null)
    {
      if (splitTile())
      {
//        log.info("Returning at point 1 after " + (System.currentTimeMillis() - start));
        return true;
      }
    }

    while (scannedInputReader.nextKeyValue())
    {
      final long id = scannedInputReader.getCurrentKey().get();
//      log.info("scannedInputReader returned key " + id);

      // If initialTile is non-null, then we are skipping tiles in order
      // to return tile neighborhoods that do not overlap one another.
      if (initialTile != null)
      {
        // Check to see if we need to skip this tile based on the provided
        // stride.
        final TMSUtils.Tile tile = TMSUtils.tileid(id, zoomLevel);
        if ((((tile.tx - initialTile.tx) % tileClusterInfo.getStride()) != 0) ||
            (((tile.ty - initialTile.ty) % tileClusterInfo.getStride()) != 0))
        {
          continue;
        }
      }
      // need to split one tile into many
      if (zoomLevel > splitZoomLevel)
      {
        TMSUtils.Tile t = TMSUtils.tileid(id, splitZoomLevel);
        TMSUtils.Bounds tb = TMSUtils.tileBounds(t.tx, t.ty, splitZoomLevel, tilesize);
        if (inputBounds.intersects(tb.w, tb.s, tb.e, tb.n))
        {
          // RasterWritable.toRaster(scannedInputReader.getCurrentValue()
          calculateTileSplit(id, toNonWritableTile(scannedInputReader.getCurrentValue()));
          if (splitTile())
          {
//            log.info("Returning at point 2 after " + (System.currentTimeMillis() - start));
            return true;
          }
        }
      }
      // need to join many tiles into one
      else if (zoomLevel < splitZoomLevel)
      {

      }
      // one-to-one tile
      else
      {
        final TMSUtils.Tile tile = TMSUtils.tileid(id, zoomLevel);
        final TMSUtils.Bounds tb = TMSUtils.tileBounds(tile.tx, tile.ty, zoomLevel, tilesize);
        if (inputBounds.intersects(tb.w, tb.s, tb.e, tb.n))
        {
          boolean intersects = intersectsPrebounds(id, zoomLevel);
          if (!intersects)
          {
            // RasterWritable.toRaster(scannedInputReader.getCurrentValue())
            setNextKeyValue(id, toNonWritableTile(scannedInputReader.getCurrentValue()));
//            log.info("Returning at point 3 after " + (System.currentTimeMillis() - start));
            return true;
          }
        }
      }
    }
//    log.info("Returning at point 4 after " + (System.currentTimeMillis() - start));
    return false;
  }

  private boolean intersectsPrebounds(final long id, final int zoom)
  {
    boolean intersects = false;
    if (preBounds != null)
    {
      final TMSUtils.Tile tile = TMSUtils.tileid(id, zoom);

      final TMSUtils.Bounds tb = TMSUtils.tileBounds(tile.tx, tile.ty, zoom, tilesize);
      final Bounds tileBounds = new Bounds(tb.w, tb.s, tb.e, tb.n);

      for (final Bounds bounds : preBounds)
      {
        if (tileBounds.intersects(bounds))
        {
          intersects = true;
          break;
        }
      }
    }
    return intersects;
  }

  private void calculateTileSplit(final long id, final T tile)
  {
    tilesplit = new TileSplit();
    tilesplit.id = id;
    tilesplit.tile = tile;

    TMSUtils.Tile t = TMSUtils.tileid(id, splitZoomLevel);

    // calculate the bounds for the tile
    TMSUtils.Bounds bounds = TMSUtils.tileBounds(t.tx, t.ty, splitZoomLevel, tilesize);

    // now use those bounds to calculate the tilebounds of the split level
    TMSUtils.TileBounds tb = TMSUtils.boundsToTile(bounds, zoomLevel, tilesize);

    tilesplit.startTx = tb.w;
    tilesplit.endTx = tb.e;

    tilesplit.endTy = tb.n;

    tilesplit.currentTx = tilesplit.startTx - 1;  // subtract 1 so we'll pick up the 1st tile
    tilesplit.currentTy = tb.s;
  }

  private boolean splitTile()
  {
    // use a while true here to avoid recursion.
    while (true)
    {
      tilesplit.currentTx++;
      if (tilesplit.currentTx > tilesplit.endTx)
      {
        tilesplit.currentTx = tilesplit.startTx;
        tilesplit.currentTy++;
      }

      // one row too many, we're done
      if (tilesplit.currentTy > tilesplit.endTy)
      {
        tilesplit = null;
        return false;
      }

      final long id = TMSUtils.tileid(tilesplit.currentTx, tilesplit.currentTy, zoomLevel);

      final TMSUtils.Tile tile = TMSUtils.tileid(id, zoomLevel);
      final TMSUtils.Bounds tb = TMSUtils.tileBounds(tile.tx, tile.ty, zoomLevel, tilesize);
      if (inputBounds.intersects(tb.w, tb.s, tb.e, tb.n))
      {
        // still need to check the intersection...
        boolean intersects = intersectsPrebounds(id, zoomLevel);
        if (!intersects)
        {
          final T tileValue = splitTile(tilesplit.tile, tilesplit.id, splitZoomLevel, id,
              zoomLevel, tilesize);

          setNextKeyValue(id, tileValue);

          return true;
        }
      }
    }  // do the next one...
  }

  private void setNextKeyValue(final long tileid, final T tileValue)
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
    key.set(tileid);
    value.clear();
    value.setTileid(tileid);

    // Add tile(s) from the primary/first image input.  We know this is the proper zoom level,
    // so we don't need to do anything weird.
    value.set(scannedInput, tileid, tileValue);
    // Add neighboring tiles to the cluster if needed
    int neighborCountForTile = 0;
    if (tileClusterInfo != null && neighborTileIds != null)
    {
      neighborCountForTile = tileClusterInfo.getNeighbors(tileid, zoomLevel,
          tilesize, inputBounds, neighborTileIds);
      if (tileClusterInfo != null && neighborTileIds != null)
      {
        for (int index = 0; index < neighborCountForTile; index++)
        {
          setTile(scannedInput, primaryReader, neighborTileIds[index]);
        }
      }
    }

    // Add tiles from additional inputs
    for (final Entry<String, MrsTileReader<T>> entry : readers.entrySet())
    {
      final String inputKey = entry.getKey();
      final MrsTileReader<T> inputReader = entry.getValue();
      
      // we'll need to make sure this base raster is in the correct zoomlevel.
      setTile(inputKey, inputReader, tileid);
      
      // Add neighboring tiles from additional inputs to the cluster if needed
      if (tileClusterInfo != null && neighborTileIds != null)
      {
        for (int index = 0; index < neighborCountForTile; index++)
        {
          setTile(inputKey, inputReader, neighborTileIds[index]);
        }
      }
    }
//    log.info("After setNextKeyValue took" + (System.currentTimeMillis() - start));
  }

  private void setTile(final String k, final MrsTileReader<T> reader, final long tileid)
  {
    // need to split one tile into many
    if (zoomLevel > reader.getZoomlevel())
    {
      int zl = reader.getZoomlevel();
      
      TMSUtils.Tile t = TMSUtils.calculateTile(TMSUtils.tileid(tileid, zoomLevel), 
          zoomLevel, zl, tilesize);
      TileIdWritable id = new TileIdWritable(TMSUtils.tileid(t.tx, t.ty, zl));

      T r = getTileFromCache(k, reader, id);

      T split = splitTile(r, id.get(), zl, tileid, zoomLevel, tilesize);
      
      value.set(k, tileid, split);
    }
    // need to join many tiles into one
    else if (zoomLevel < splitZoomLevel)
    {

    }
    // one-to-one tile
    else
    {
      final T r = reader.get(new TileIdWritable(tileid));
      if (r != null)
      {
        value.set(k, tileid, r);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private T getTileFromCache(final String k, final MrsTileReader<T> reader,
      TileIdWritable id)
  {
    // see if the lower level tile is in our cache...
    if (!tilecache.containsKey(k))
    {
      tilecache.put(k, new LRUMap(4));
    }
    Map<TileIdWritable, T> cache = tilecache.get(k);
    if (cache.containsKey(id))
    {
      return cache.get(id);
    }
    T r = reader.get(id);
    cache.put(id,  r);

    return r;
  }
}
