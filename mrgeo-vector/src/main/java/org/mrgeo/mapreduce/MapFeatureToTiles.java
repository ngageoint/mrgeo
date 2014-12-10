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

import com.vividsolutions.jts.geom.Envelope;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.mrgeo.featurefilter.FeatureFilter;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.rasterops.OpImageRegistrar;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.utils.Base64Utils;
import org.mrgeo.utils.LongRectangle;
import org.mrgeo.utils.TMSUtils;
import org.mrgeo.utils.TMSUtils.Bounds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MapFeatureToTiles extends Mapper<LongWritable, Geometry, TileIdWritable, Geometry>
{
  @SuppressWarnings("unused")
  private static final Logger _log = LoggerFactory.getLogger(MapFeatureToTiles.class);

  public static final String IMAGE_TILE_BOUNDS = "MapFeatureToTiles.imageTileBounds";
  public static final String FEATURE_FILTER = "MapFeatureToTiles.geometryFilter";
  public static final String ZOOM = "MapFeatureToTiles.zoom";
  public static final String TILE_SIZE = "MapFeatureToTiles.tileSize";
  
  private TileIdWritable tileId = new TileIdWritable();
  private LongRectangle imageTileBounds;
  private FeatureFilter filter = null;
  private int zoom;
  private int tileSize;
  
  static String VALIDTILES = "Valid tiles";
  static String NOFEATURE = "No feature";
  static String NOGEOMETRYATTR = "No geometry attribute";
  static String NOGEOMETRY = "No geometry";
  static String TOTALTILES = "Total tiles";
  static String INVALIDGEOMETRY = "Invalid geometry";
  static String OUTBOUNDS = "Out of image bounds";

  protected Envelope _calculateEnvelope(Geometry g)
  {
    return g.toJTS().getEnvelopeInternal();
  }
  
  @Override
  public void setup(Context context)
  {
    OpImageRegistrar.registerMrGeoOps();
    try
    {
      Configuration conf = context.getConfiguration();
      zoom = conf.getInt(ZOOM, 0);
      tileSize = conf.getInt(TILE_SIZE, 512);
      if (conf.get(FEATURE_FILTER) != null)
      {
        filter = (FeatureFilter) Base64Utils.decodeToObject(conf.get(FEATURE_FILTER));
      }
      if (conf.get(IMAGE_TILE_BOUNDS) != null)
      {
        imageTileBounds = (LongRectangle) Base64Utils.decodeToObject(conf.get(IMAGE_TILE_BOUNDS));
      }
    }
    catch (Exception e)
    {
      throw new IllegalArgumentException("Error parsing configuration", e);
    }
  }

  @Override
  public void map(LongWritable key, Geometry value, Context context) throws IOException,
      InterruptedException
  {
    context.getCounter("Tiles", TOTALTILES).increment(1);
    // if this is an invalid geometry ignore it.
    if (value == null)
    {
      // ignore this row
      context.getCounter("Tiles", NOFEATURE).increment(1);
      return;
    }

    if (value.isEmpty())
    {
      context.getCounter("Tiles", NOGEOMETRY).increment(1);
      return;
    }
    
    if (!value.isValid())
    {
      context.getCounter("Tiles", INVALIDGEOMETRY).increment(1);
      return;
    }
    context.getCounter("Tiles", VALIDTILES).increment(1);

    if (filter != null)
    {
      value = filter.filterInPlace(value);
    }

    // find the min and max tiles that this geometry intersect.
    Envelope envelope = _calculateEnvelope(value);
    // For some features, the envelope can fall outside of standard world bounds, so we trim it.
    Bounds bounds = new Bounds(Math.min(180.0, Math.max(-180.0, envelope.getMinX())),
        Math.min(90.0, Math.max(-90.0, envelope.getMinY())),
        Math.min(180.0, Math.max(-180.0, envelope.getMaxX())),
        Math.min(90.0, Math.max(-90.0, envelope.getMaxY())));
    TMSUtils.TileBounds featureTileBounds = TMSUtils.boundsToTile(bounds, zoom, tileSize);
    // if the tiles are completely out of image bounds, ignore it and move on.
    if (imageTileBounds != null)
    {
      if ((featureTileBounds.e > imageTileBounds.getMaxX()) || // feature is right of the right edge of the image
          (featureTileBounds.w < imageTileBounds.getMinX()) || // feature is left of the left edge of the image
          (featureTileBounds.s > imageTileBounds.getMaxY()) || // feature is above the top edge of the image
          (featureTileBounds.n < imageTileBounds.getMinY()))   // feature is below the bottom edge of the image
      {
        context.getCounter("Tiles", OUTBOUNDS).increment(1);
        return;
      }
    }

    // emit an key/value pair for every tile this geometry intersects
    // TODO: Should we pass on emitting tiles the feature intersects that
    // do not intersect the image?
    for (long tx = featureTileBounds.w; tx <= featureTileBounds.e; tx++)
    {
      for (long ty = featureTileBounds.s; ty <= featureTileBounds.n; ty++)
      {
        tileId.set(TMSUtils.tileid(tx, ty, zoom));
        context.write(tileId, value);
      }
    }
  }
}
