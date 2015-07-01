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

package org.mrgeo.mapreduce;

import org.apache.hadoop.mapreduce.Reducer;
import org.mrgeo.image.ImageStats;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.adhoc.AdHocDataProvider;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.mrgeo.image.ImageStats.rasterToStats;

public class OpChainReducer extends
Reducer<TileIdWritable, RasterWritable, TileIdWritable, RasterWritable>

{
  public static final String STATS_PROVIDER = "stats.provider";

  /** Writes stats tile ids to metadata, others to output */
  @Override
  public void reduce(final TileIdWritable key, final Iterable<RasterWritable> values, final Context context)
          throws IOException, InterruptedException
  {
    if (key.get() == ImageStats.STATS_TILE_ID)
    {
      List<ImageStats[]> listOfStats = new ArrayList<ImageStats[]>();
      for (final RasterWritable raster : values)
      {
        listOfStats.add(rasterToStats(RasterWritable.toRaster(raster)));
      }
      ImageStats[] stats = ImageStats.aggregateStats(listOfStats);
      String adhoc = context.getConfiguration().get(STATS_PROVIDER, null);
      if (adhoc == null)
      {
        throw new IOException("Stats provider not set");
        
      }
      AdHocDataProvider provider = DataProviderFactory.getAdHocDataProvider(adhoc,
          AccessMode.WRITE, context.getConfiguration());
      ImageStats.writeStats(provider, stats);
    } else {
      context.write(key, values.iterator().next());
    }
  }
}
