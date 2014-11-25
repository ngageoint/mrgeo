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

package org.mrgeo.mapreduce.formats;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.data.raster.RasterUtils;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.utils.LongRectangle;
import org.mrgeo.utils.TMSUtils;

import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class EmptyTileInputFormat extends InputFormat<TileIdWritable, TileCollection<Raster>>
{
  static public final String EMPTY_IMAGE = "EmptyImage";
  
  static public class EmptyTileRecordReader extends
  RecordReader<TileIdWritable, TileCollection<Raster>>
  {
    private TileIdWritable key;
    private TileCollection<Raster> value;

    // private List<TileIdWritable> tiles;
    private float processed;
    private float total;
    private Iterator<TileIdWritable> iterator;

    private Raster emptyImage;

    @Override
    public void close() throws IOException
    {
    }

    @Override
    public TileIdWritable getCurrentKey() throws IOException, InterruptedException
    {
      return key;
    }

    @Override
    public TileCollection<Raster> getCurrentValue() throws IOException, InterruptedException
    {
      return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException
    {
      return (processed / total);
    }

    @Override
    public void initialize(final InputSplit split, final TaskAttemptContext context)
        throws IOException,
        InterruptedException
        {
      final EmptyTileSplit tilesplit = (EmptyTileSplit) split;
      final List<TileIdWritable> tiles = tilesplit.getTiles();

      processed = 0;
      total = tiles.size();

      iterator = tiles.iterator();

      value = new TileCollection<Raster>();

      emptyImage = RasterUtils.createEmptyRaster(tilesplit.getTilesize(), tilesplit.getTilesize(),
          tilesplit.getBands(), tilesplit.getDatatype(), tilesplit.getNodata());
        }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException
    {
      if (iterator.hasNext())
      {
        key = iterator.next();

        value.setTileid(key.get());
        value.set(EMPTY_IMAGE, key.get(), emptyImage);

        processed += 1.0;

        return true;
      }
      return false;
    }

  }

  protected static class EmptyTileSplit extends InputSplit implements Writable
  {
    private int level = 0;
    private List<TileIdWritable> tiles = null;
    private int tilesize;
    private int bands;
    private int datatype;
    private double nodata;

    public EmptyTileSplit()
    {
    }

    EmptyTileSplit(final int level, final List<TileIdWritable> tiles, final int tilesize,
        final int bands, final int datatype, final double nodata)
        {
      this.level = level;
      this.tiles = new LinkedList<TileIdWritable>(tiles);

      this.tilesize = tilesize;
      this.bands = bands;
      this.datatype = datatype;
      this.nodata = nodata;
        }

    public int getBands()
    {
      return bands;
    }

    public int getDatatype()
    {
      return datatype;
    }

    @Override
    public long getLength() throws IOException, InterruptedException
    {
      return tiles.size();
    }

    public int getLevel()
    {
      return level;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException
    {
      return new String[0];
    }

    public double getNodata()
    {
      return nodata;
    }

    public List<TileIdWritable> getTiles()
    {
      return tiles;
    }

    public int getTilesize()
    {
      return tilesize;
    }

    @Override
    public void readFields(final DataInput in) throws IOException
    {
      level = in.readInt();
      final int tilesLen = in.readInt();
      tiles = new LinkedList<TileIdWritable>();
      for (int i = 0; i < tilesLen; i++)
      {
        tiles.add(new TileIdWritable(in.readLong()));
      }
      
      tilesize = in.readInt();
      bands = in.readInt();
      datatype = in.readInt();
      nodata = in.readDouble();

    }

    @Override
    public void write(final DataOutput out) throws IOException
    {

      out.writeInt(level);
      out.writeInt(tiles.size());
      for (final TileIdWritable tile : tiles)
      {
        out.writeLong(tile.get());
      }
      out.writeInt(tilesize);
      out.writeInt(bands);
      out.writeInt(datatype);
      out.writeDouble(nodata);

    }
  }

  private static final String PREFIX = "EmptyTileInputFormat";
  public static final String TILE_BOUNDS = PREFIX + ".tilebounds";
  public static final String ZOOMLEVEL = PREFIX + ".zoomlevel";
  public static final String TILESIZE = PREFIX + ".tilesize";
  public static final String BANDS = PREFIX + ".bands";

  public static final String DATATYPE = PREFIX + ".datatype";

  public static final String NODATA = PREFIX + ".nodata";

  private static final long minTilesPerSplit = 100;

  public static void setInputInfo(final Job job, final LongRectangle tilebounds, final int zoomlevel)
  {
    job.setInputFormatClass(EmptyTileInputFormat.class);

    final Configuration conf = job.getConfiguration();
    conf.set(TILE_BOUNDS, tilebounds.toDelimitedString());
    conf.setInt(ZOOMLEVEL, zoomlevel);
  }

  public static void setRasterInfo(final Job job, final int tilesize,
      final int bands, final int datatype, final double nodata)
  {
    job.setInputFormatClass(EmptyTileInputFormat.class);

    final Configuration conf = job.getConfiguration();

    conf.setInt(TILESIZE, tilesize);
    conf.setInt(BANDS, bands);
    conf.setInt(DATATYPE, datatype);
    conf.setFloat(NODATA, (float)nodata);  // there is no setDouble!?
  }

  @Override
  public RecordReader<TileIdWritable, TileCollection<Raster>> createRecordReader(final InputSplit split,
      final TaskAttemptContext context)
          throws IOException, InterruptedException
          {
    final EmptyTileRecordReader rr = new EmptyTileRecordReader();
    rr.initialize(split, context);

    return rr;
          }

  @Override
  public List<InputSplit> getSplits(final JobContext context) throws IOException, InterruptedException
  {
    final List<InputSplit> splits = new LinkedList<InputSplit>();

    final Configuration conf = context.getConfiguration();

    final LongRectangle tilebounds = LongRectangle.fromDelimitedString(conf.get(TILE_BOUNDS));

    final int zoom = conf.getInt(ZOOMLEVEL, 0);
    final int tilesize = conf.getInt(TILESIZE,
        Integer.valueOf(MrGeoProperties.getInstance().getProperty("mrsimage.tilesize", "512")));
    final int bands = conf.getInt(BANDS, 1);
    final int datatype = conf.getInt(DATATYPE, DataBuffer.TYPE_DOUBLE);
    final double nodata = conf.getFloat(NODATA, Float.NaN);

    final long maxSplits = conf.getInt("mapred.map.tasks", 1);
    final long totalTiles = tilebounds.getWidth() * tilebounds.getHeight();

    long tilesPerSplit = totalTiles / maxSplits;

    if (tilesPerSplit < minTilesPerSplit)
    {
      tilesPerSplit = minTilesPerSplit;
    }

    int cnt = 0;
    List<TileIdWritable> tiles = new LinkedList<TileIdWritable>();

    for (long ty = tilebounds.getMinY(); ty <= tilebounds.getMaxY(); ty++)
    {
      for (long tx = tilebounds.getMinX(); tx <= tilebounds.getMaxX(); tx++)
      {
        tiles.add(new TileIdWritable(TMSUtils.tileid(tx, ty, zoom)));
        if (cnt++ > tilesPerSplit)
        {
          final EmptyTileSplit split = new EmptyTileSplit(zoom, tiles, tilesize, bands, datatype,
              nodata);
          splits.add(split);

          tiles = new LinkedList<TileIdWritable>();

          cnt = 0;
        }
      }
    }

    if (tiles.size() > 0)
    {
      final EmptyTileSplit split = new EmptyTileSplit(zoom, tiles, tilesize, bands, datatype,
          nodata);
      splits.add(split);
    }

    return splits;
  }

}
