package org.mrgeo.vector.mrsvector;

import org.apache.hadoop.mapreduce.RecordReader;
import org.mrgeo.data.tile.MrsTileReader;
import org.mrgeo.data.tile.TileDataProvider;
import org.mrgeo.data.tile.TileIdWritable;

import java.awt.image.Raster;

public abstract class MrsVectorDataProvider extends TileDataProvider<Raster>
{
  protected MrsVectorDataProvider()
  {
    super();
  }

  public MrsVectorDataProvider(final String resourceName)
  {
    super(resourceName);
  }

  /**
   * Return an instance of a RecordReader class to be used in map/reduce
   * jobs for reading tiled data.
   * 
   * @return
   */
  public abstract RecordReader<TileIdWritable, VectorTileWritable> getRecordReader();

  /**
   * Return an instance of a MrsTileReader class to be used for reading
   * tiled data. This method may be invoked by callers regardless of
   * whether they are running within a map/reduce job or not.
   * 
   * @return
   */
  public abstract MrsTileReader<VectorTile> getMrsTileReader();
}
