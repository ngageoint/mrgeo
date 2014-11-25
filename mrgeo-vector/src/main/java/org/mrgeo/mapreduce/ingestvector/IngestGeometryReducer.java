package org.mrgeo.mapreduce.ingestvector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.mapreduce.GeometryWritable;
import org.mrgeo.mapreduce.MapGeometryToTiles;
import org.mrgeo.vector.mrsvector.VectorTileWritable;
import org.mrgeo.vector.mrsvector.WritableVectorTile;
import org.mrgeo.utils.geotools.GeotoolsVectorUtils;
import org.mrgeo.data.tile.TileIdWritable;

import java.io.IOException;

public class IngestGeometryReducer extends Reducer<TileIdWritable, GeometryWritable, TileIdWritable, VectorTileWritable>
{
  int zoom;
  int tilesize;

  public IngestGeometryReducer()
  {
  }

  @Override
  protected void cleanup(Context context) throws IOException,
  InterruptedException
  {
    super.cleanup(context);
  }

  @Override
  protected void reduce(TileIdWritable tileId, Iterable<GeometryWritable> geometries,
    Context context) throws IOException, InterruptedException
    {
    //TMSUtils.Tile tid = TMSUtils.tileid(tileId.get(), zoom);
    //Bounds tb = TMSUtils.tileBounds(tid.tx, tid.ty, zoom, tilesize).asBounds();

    WritableVectorTile tile = null;

    //boolean hasgeometry = false;
    for (GeometryWritable gw: geometries)
    {

      Geometry geometry = gw.getGeometry();
      //if (geometry.hasAttribute("OBJECTID", "603"))
      {
        if (tile == null)
        {
          tile = new WritableVectorTile();
          tile.beginWriting();
        }
        tile.add(geometry);
      }
    }

    if (tile != null) //  && hasgeometry)
    {
      tile.endWriting();
      context.write(tileId, VectorTileWritable.toWritable(tile));
    }

    }

  @Override
  protected void setup(Context context) throws IOException,InterruptedException
  {
    super.setup(context);

    GeotoolsVectorUtils.initialize();

    Configuration conf = context.getConfiguration();

    zoom = conf.getInt(MapGeometryToTiles.ZOOMLEVEL, 20);
    tilesize = conf.getInt(MapGeometryToTiles.TILESIZE, 512);
  }

}
