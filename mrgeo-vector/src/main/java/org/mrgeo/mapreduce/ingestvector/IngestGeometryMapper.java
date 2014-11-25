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
