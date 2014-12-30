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
import com.vividsolutions.jts.operation.valid.IsValidOp;
import com.vividsolutions.jts.operation.valid.TopologyValidationError;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.mrgeo.featurefilter.FeatureFilter;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.rasterops.OpImageRegistrar;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.utils.Base64Utils;
import org.mrgeo.utils.Bounds;
import org.mrgeo.utils.TMSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class FeatureToTilesMapper extends Mapper<LongWritable, Geometry, TileIdWritable, GeometryWritable>
{
  @SuppressWarnings("unused")
  private static final Logger _log = LoggerFactory.getLogger(FeatureToTilesMapper.class);

  public static String classname = FeatureToTilesMapper.class.getSimpleName();

  public static String FEATURE_FILTER = classname + ".geometryFilter";
  public static String ZOOM = classname + ".zoom";
  public static String TILE_SIZE = classname + ".tileSize";
  public static String BOUNDS = classname + ".bounds";

  private int zoom;
  private int tileSize;
  private TMSUtils.Bounds bounds;
  private FeatureFilter filter = null;

  static String VALIDFEATURES = "Valid features";
  static String NOFEATURE = "No feature";
  static String NOGEOMETRYATTR = "No geometry attribute";
  static String NOGEOMETRY = "No geometry";
  static String OUTPUTTILES = "Output tiles";
  static String TOTALFEATURES = "Total features";
  static String INVALIDGEOMETRY = "Invalid geometry";
  static String OUTBOUNDS = "Out of image bounds";

  public static List<TileIdWritable> getOverlappingTiles(final Geometry feature, final int zoom,
    final int tileSize)
  {
    //final List<TileIdWritable> tiles = new ArrayList<TileIdWritable>();
    final Envelope envelope = _calculateEnvelope(feature);
    final TMSUtils.Bounds b = new TMSUtils.Bounds(envelope.getMinX(), envelope.getMinY(), envelope
      .getMaxX(), envelope.getMaxY());

    return getOverlappingTiles(zoom, tileSize, b);
  }

  public static List<TileIdWritable> getOverlappingTiles(final int zoom,
    final int tileSize, final TMSUtils.Bounds bounds)
  {
    final List<TileIdWritable> tiles = new ArrayList<TileIdWritable>();
    final TMSUtils.TileBounds tb = TMSUtils.boundsToTile(bounds, zoom, tileSize);

    // emit an key/value pair for every tile this geometry intersects
    for (long tx = tb.w; tx <= tb.e; tx++)
    {
      for (long ty = tb.s; ty <= tb.n; ty++)
      {
        final TileIdWritable tile = new TileIdWritable(TMSUtils.tileid(tx, ty, zoom));
        tiles.add(tile);
      }
    }
    return tiles;
  }

  protected static Envelope _calculateEnvelope(final Geometry f)
  {
    return f.toJTS().getEnvelopeInternal();
  }

  @Override
  public void map(final LongWritable key, Geometry value, final Context context) throws IOException,
    InterruptedException
  {
    context.getCounter("Tiles", TOTALFEATURES).increment(1);
    // if this is an invalid geometry ignore it.
    if (value == null)
    {
      // ignore this row
      context.getCounter("Tiles", NOFEATURE).increment(1);
      return;
    }

    if (!value.isValid())
    {
      context.getCounter("Tiles", NOGEOMETRY).increment(1);
      return;
    }

    if (value.isEmpty())
    {
      context.getCounter("Tiles", NOGEOMETRYATTR).increment(1);
      return;
    }


    final IsValidOp isValidOp = new IsValidOp(value.toJTS());
    if (!isValidOp.isValid())
    {
      final TopologyValidationError tve = isValidOp.getValidationError();
      if (tve.getErrorType() != TopologyValidationError.SELF_INTERSECTION)
      {
        context.getCounter("Tiles", INVALIDGEOMETRY).increment(1);
        return;
      }
    }

    final Envelope envelope = _calculateEnvelope(value);
    final TMSUtils.Bounds b = new TMSUtils.Bounds(envelope.getMinX(), envelope.getMinY(), envelope
      .getMaxX(), envelope.getMaxY());

    if (bounds.intersect(b))
    {
      context.getCounter("Tiles", VALIDFEATURES).increment(1);

      if (filter != null)
      {
        value = filter.filterInPlace(value);
      }

      final List<TileIdWritable> tiles = getOverlappingTiles(zoom, tileSize, b);
      for (final TileIdWritable tileId : tiles)
      {
        context.getCounter("Tiles", OUTPUTTILES).increment(1);
        context.write(tileId, new GeometryWritable(value));
      }
    }
    else
    {
      context.getCounter("Tiles", OUTBOUNDS).increment(1);
    }
  }

  @Override
  public void setup(final Context context)
  {
    OpImageRegistrar.registerMrGeoOps();
    try
    {
      final Configuration conf = context.getConfiguration();
      if (conf.get(FEATURE_FILTER) != null)
      {
        filter = (FeatureFilter) Base64Utils.decodeToObject(conf.get(FEATURE_FILTER));
      }
      zoom = Integer.valueOf(conf.get(FeatureToTilesMapper.ZOOM));
      tileSize = Integer.valueOf(conf.get(FeatureToTilesMapper.TILE_SIZE));

      bounds = Bounds.fromDelimitedString(
        conf.get(FeatureToTilesMapper.BOUNDS, Bounds.world.toDelimitedString())).getTMSBounds();
    }
    catch (final Exception e)
    {
      throw new IllegalArgumentException("Error parsing configuration", e);
    }
  }
}
