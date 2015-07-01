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

package org.mrgeo.mapreduce.ingestvector;

import org.apache.hadoop.io.LongWritable;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.mapreduce.GeometryWritable;
import org.mrgeo.mapreduce.MapGeometryToTiles;
import org.mrgeo.utils.geotools.GeotoolsVectorUtils;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.utils.Bounds;
import org.mrgeo.utils.TMSUtils;

import java.io.IOException;

public class IngestGeometryMapper extends MapGeometryToTiles
{

  public IngestGeometryMapper()
  {
    super();
  }

  @Override
  protected void map(LongWritable key, GeometryWritable value, Context context) throws IOException, InterruptedException
  {
    if (isValid(value, context))
    {
      Geometry geometry = value.getGeometry();

      TMSUtils.TileBounds tiles = calculateTileBounds(geometry);

      if (inBounds(tiles, context))
      {
        // emit an key/value pair for every tile this geometry intersects
        for (long tx = tiles.w; tx <= tiles.e; tx++)
        {
          for (long ty = tiles.s; ty <= tiles.n; ty++)
          {
            Bounds gb = geometry.getBounds();
            Bounds tb = TMSUtils.tileBounds(tx, ty, zoom, tilesize).asBounds();

            // is the geometry fully contained in the tile...
            if (tb.contains(gb))
            {
              context.write(new TileIdWritable(TMSUtils.tileid(tx, ty, zoom)), value);
            }
            else
            {
              // clip the geometry to the bounds...
              Geometry clipped = geometry.clip(tb);
              if (clipped != null)
              {
                context.write(new TileIdWritable(TMSUtils.tileid(tx, ty, zoom)), new GeometryWritable(clipped));
              }
            }
          }

        }
      }
    }
  }
  
  @Override
  protected void setup(Context context) throws IOException, InterruptedException
  {
    super.setup(context);
    GeotoolsVectorUtils.initialize();
  }
}
