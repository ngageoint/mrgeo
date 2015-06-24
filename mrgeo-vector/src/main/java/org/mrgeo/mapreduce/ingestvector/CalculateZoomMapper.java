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

package org.mrgeo.mapreduce.ingestvector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.geometry.*;
import org.mrgeo.mapreduce.GeometryWritable;
import org.mrgeo.tile.TileIdZoomWritable;
import org.mrgeo.utils.TMSUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CalculateZoomMapper extends Mapper<LongWritable, GeometryWritable, TileIdZoomWritable, LongWritable>
{
  int maxzoom = 20;
  int tilesize;
  
  Map<TileIdZoomWritable, LongWritable> counts;

  public CalculateZoomMapper()
  {
  }

  @Override
  public void setup(Context context) throws IOException, InterruptedException
  {
    super.setup(context);
    
    counts = new HashMap<TileIdZoomWritable, LongWritable>();

    Configuration conf = context.getConfiguration();

    maxzoom = conf.getInt(IngestVectorDriver.MAX_ZOOM, 20);
    tilesize = conf.getInt(IngestVectorDriver.TILESIZE, MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT_INT);
  }

  @Override
  public void map(LongWritable key, GeometryWritable value, Context context) throws IOException, InterruptedException
  {
    Geometry geometry = value.getGeometry();

    calculateCounts(geometry);
  }

  private void calculateCounts(Geometry geometry)
  {

    switch (geometry.type())
    {
    case COLLECTION:
      for (Geometry g: ((GeometryCollection)geometry).getGeometries())
      {
        calculateCounts(g);
      }
      break;
    case LINEARRING:
      LinearRing ring = (LinearRing)geometry;
      // starting at 1 so we don't count the 1st pt (which is the same as the last)
      for (int i = 1; i < ring.getNumPoints(); i++)
      {
        Point pt = ring.getPoint(i);
        addPoint(pt);
      }
      break;
    case LINESTRING:
      LineString string = (LineString)geometry;
      for (Point pt: string.getPoints())
      {
        addPoint( pt);
      }
      break;
    case POINT:
      Point pt = (Point)geometry;
      addPoint(pt);
      break;
    case POLYGON:
      Polygon polygon = (Polygon)geometry;
      calculateCounts(polygon.getExteriorRing());
      for (int i = 0; i < polygon.getNumInteriorRings(); i++)
      {
        calculateCounts(polygon.getInteriorRing(i));
      }
      break;
    default:
      break;

    }
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException
  {
    super.cleanup(context);
    
    for (Map.Entry<TileIdZoomWritable, LongWritable> entry : counts.entrySet())
    {
      context.write(entry.getKey(), entry.getValue());
    }
  }

  private void addPoint(Point pt)
  {
    for (int zoom = 1; zoom <= maxzoom; zoom++)
    {
      TMSUtils.Tile tile = TMSUtils.latLonToTile(pt.getY(), pt.getX(), zoom, tilesize);

      TileIdZoomWritable id = new TileIdZoomWritable(TMSUtils.tileid(tile.tx, tile.ty, zoom), zoom);

      if (counts.containsKey(id))
      {
        counts.put(id, new LongWritable(counts.get(id).get() + 1));
      }
      else
      {
        counts.put(id, new LongWritable(1));
      }
    }
  }

}
