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

package org.mrgeo.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.utils.Bounds;
import org.mrgeo.utils.TMSUtils;

import java.io.IOException;

public class MapGeometryToTiles extends Mapper<LongWritable, GeometryWritable, TileIdWritable, GeometryWritable>
{
  static final private String base = MapGeometryToTiles.class.getSimpleName();

  static final public String ZOOMLEVEL = base + ".zoom";
  static final public String TILESIZE = base + ".tilesize";
  static final public String BOUNDS = base + ".bounds";

  static protected String VALIDTILES = "Valid geometries";
  static protected String NOGEOMETRY = "No geometry";
  static protected String TOTALTILES = "Total geometries";
  static protected String OUTBOUNDS = "Out of image bounds";

  protected int zoom;
  protected int tilesize;

  protected TMSUtils.TileBounds bounds;

  @Override
  protected void cleanup(Context context) throws IOException,
  InterruptedException
  {
    super.cleanup(context);
  }

  @Override
  protected void map(LongWritable key, GeometryWritable value, Context context) throws IOException, InterruptedException
  {
    if (isValid(value, context))
    {
      Geometry geometry = value.getGeometry();

      TMSUtils.TileBounds tb = calculateTileBounds(geometry);

      if (inBounds(tb, context))
      {

        // emit an key/value pair for every tile this geometry intersects
        for (long tx = tb.w; tx <= tb.e; tx++)
        {
          for (long ty = tb.s; ty <= tb.n; ty++)
          {
            context.write(new TileIdWritable(TMSUtils.tileid(tx, ty, zoom)), value);
          }
        }
      }
    }
  }

  protected boolean inBounds(TMSUtils.TileBounds tb, Context context)
  {
    // if the tiles are completely out of bounds, ignore it and move on.
    if (bounds != null)
    {
      if ((tb.e > bounds.e) || // feature is right of the right edge of the image
          (tb.w < bounds.w) || // feature is left of the left edge of the image
          (tb.s < bounds.s) || // feature is below the bottom edge of the image
          (tb.n > bounds.n))   // feature is above the top edge of the image
      {
        context.getCounter("Tiles", OUTBOUNDS).increment(1);
        return false;
      }
    }

    return true;
  }

  protected static boolean isValid(GeometryWritable value, Context context)
  {
    context.getCounter("Tiles", TOTALTILES).increment(1);

    if (value == null || value.getGeometry() == null)
    {
      context.getCounter("Tiles", NOGEOMETRY).increment(1);
      return false;
    }

    context.getCounter("Tiles", VALIDTILES).increment(1);

    return true;
  }

  protected TMSUtils.TileBounds calculateTileBounds(Geometry geometry)
  {
    // find the min and max tiles that this geometry intersect.
    Bounds bbox = geometry.getBounds();

    // For some features, the bounds can fall outside of standard world bounds, so we trim it.
    Bounds b = new Bounds(Math.min(180.0, Math.max(-180.0, bbox.getMinX())),
      Math.min(90.0, Math.max(-90.0, bbox.getMinY())),
      Math.min(180.0, Math.max(-180.0, bbox.getMaxX())),
      Math.min(90.0, Math.max(-90.0, bbox.getMaxY())));

    return TMSUtils.boundsToTileExact(b.getTMSBounds(), zoom, tilesize);
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException
  {
    super.setup(context);

    Configuration conf = context.getConfiguration();

    zoom = conf.getInt(ZOOMLEVEL, 20);
    tilesize = conf.getInt(TILESIZE, 512);

    String bstr = conf.get(BOUNDS, null);
    if (bstr == null)
    {
      bstr = Bounds.world.toDelimitedString();
    }

    Bounds b = Bounds.fromDelimitedString(bstr);

    bounds = TMSUtils.boundsToTileExact(b.getTMSBounds(), zoom, tilesize);
  }

}
