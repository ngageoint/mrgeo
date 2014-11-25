package org.mrgeo.vector.mrsvector;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordReader;
import org.mrgeo.pyramid.MrsPyramidMetadata;
import org.mrgeo.data.DataProviderNotFound;
import org.mrgeo.data.MrsPyramidRecordReader;
import org.mrgeo.data.tile.MrsTileReader;
import org.mrgeo.data.tile.TileIdWritable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MrsVectorPyramidRecordReader extends MrsPyramidRecordReader<VectorTile, VectorTileWritable>
{
  @Override
  protected VectorTile splitTile(VectorTile tile, long id, int zoom, long childTileid, int childZoomLevel,
      int tilesize)
  {
    throw new NotImplementedException("Mixed zoom levels is not yet supported for vector map/reduce");
  }

  @Override
  protected VectorTile toNonWritableTile(VectorTileWritable tileValue) throws IOException
  {
    return VectorTileWritable.toMrsVector(tileValue);
  }

  @Override
  protected Map<String, MrsPyramidMetadata> readMetadata(Configuration conf)
      throws ClassNotFoundException, IOException
  {
    Map<String, MrsVectorPyramidMetadata> m = org.mrgeo.utils.HadoopVectorUtils.getVectorMetadata(conf);
    Map<String, MrsPyramidMetadata> results = new HashMap<String, MrsPyramidMetadata>(m.size());
    for (String key : m.keySet())
    {
      results.put(key, (MrsPyramidMetadata)m.get(key));
    }
    return results;
  }

  @Override
  protected MrsTileReader<VectorTile> getMrsTileReader(String input, int zoomlevel,
      Configuration conf) throws DataProviderNotFound
  {
    MrsVectorDataProvider dp = VectorDataProviderFactory.getMrsVectorDataProvider(input);
    return dp.getMrsTileReader();
  }

  @Override
  protected RecordReader<TileIdWritable, VectorTileWritable> getRecordReader(String name,
      final Configuration conf) throws DataProviderNotFound
  {
    MrsVectorDataProvider dp = VectorDataProviderFactory.getMrsVectorDataProvider(name);
    return dp.getRecordReader();
  }

  @Override
  protected VectorTile createBlankTile(double fill)
  {
    return new VectorTile();
  }

  @Override
  protected VectorTileWritable toWritable(VectorTile val) throws IOException
  {
    return VectorTileWritable.toWritable(val);
  }
}
