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

package org.mrgeo.data.image;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordReader;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderNotFound;
import org.mrgeo.data.MrsPyramidSimpleRecordReader;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.mrgeo.pyramid.MrsPyramidMetadata;
import org.mrgeo.utils.HadoopUtils;

import java.awt.image.Raster;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MrsImagePyramidSimpleRecordReader extends MrsPyramidSimpleRecordReader<Raster, RasterWritable>
{
  @Override
  protected Raster toNonWritableTile(RasterWritable tileValue) throws IOException
  {
    return RasterWritable.toRaster(tileValue);
  }

  @Override
  protected Map<String, MrsPyramidMetadata> readMetadata(Configuration conf)
          throws ClassNotFoundException, IOException
  {
    Map<String, MrsImagePyramidMetadata> m = HadoopUtils.getMetadata(conf);
    Map<String, MrsPyramidMetadata> results = new HashMap<String, MrsPyramidMetadata>(m.size());
    for (String key : m.keySet())
    {
      results.put(key, m.get(key));
    }
    return results;
  }

  @Override
  protected RecordReader<TileIdWritable, RasterWritable> getRecordReader(
          final String name, final Configuration conf) throws DataProviderNotFound
  {
    MrsImageDataProvider dp = DataProviderFactory.getMrsImageDataProvider(name,
            DataProviderFactory.AccessMode.READ, conf);
    return dp.getRecordReader();
  }

  @Override
  protected RasterWritable toWritable(Raster val) throws IOException
  {
    return RasterWritable.toWritable(val);
  }

  @Override
  protected RasterWritable copyWritable(RasterWritable val)
  {
    return new RasterWritable(val);
  }
}
