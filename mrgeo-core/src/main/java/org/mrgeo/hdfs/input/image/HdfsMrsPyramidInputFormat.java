/*
 * Copyright 2009-2017. DigitalGlobe, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.mrgeo.hdfs.input.image;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.mrgeo.data.image.ImageInputFormatContext;
import org.mrgeo.data.image.MrsPyramidMetadataReader;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.hdfs.image.HdfsMrsImageDataProvider;
import org.mrgeo.hdfs.input.MapFileFilter;
import org.mrgeo.hdfs.tile.FileSplit.FileSplitInfo;
import org.mrgeo.image.MrsPyramid;
import org.mrgeo.image.MrsPyramidMetadata;
import org.mrgeo.mapreduce.splitters.TiledInputSplit;
import org.mrgeo.utils.tms.Bounds;
import org.mrgeo.utils.tms.TMSUtils;
import org.mrgeo.utils.tms.Tile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class HdfsMrsPyramidInputFormat extends InputFormat<TileIdWritable, RasterWritable>
{
private static Logger log = LoggerFactory.getLogger(HdfsMrsPyramidInputFormat.class);
private String input;

/**
 * This constructor should only be used by the Hadoop framework on the data
 * node side when the InputFormat is constructed in order to get a RecordReader.
 * On the "Driver" side, developers should use the DataProviderFactory to get
 * and InputFormatProvider, from which the InputFormat can be obtained.
 */
public HdfsMrsPyramidInputFormat()
{
}

public HdfsMrsPyramidInputFormat(String input)
{
  this.input = input;
}

public static String getZoomName(HdfsMrsImageDataProvider dp,
    int zoomLevel)
{
  try
  {
    MrsPyramid pyramid = MrsPyramid.open(dp);
    MrsPyramidMetadata metadata = pyramid.getMetadata();
    String zoomName = metadata.getName(zoomLevel);
    if (zoomName != null)
    {
      return new Path(dp.getResourcePath(true), zoomName).toUri().toString();
    }
  }
  catch (IOException e)
  {
    log.error("Error getting zoom name", e);
  }
  return null;
}

public static void setInputInfo(Job job, String inputWithZoom) throws IOException
{
//    job.setInputFormatClass(HdfsMrsPyramidInputFormat.class);

  //final String scannedInput = inputs.get(0);
  //FileInputFormat.addInputPath(job, new Path(scannedInput));

  FileInputFormat.addInputPath(job, new Path(inputWithZoom));
  FileInputFormat.setInputPathFilter(job, MapFileFilter.class);
}

@Override
public RecordReader<TileIdWritable, RasterWritable> createRecordReader(InputSplit split,
    TaskAttemptContext context)
    throws IOException
{
  return new HdfsMrsPyramidRecordReader();
}

@Override
public List<InputSplit> getSplits(JobContext context) throws IOException
{
  long start = System.currentTimeMillis();

  Configuration conf = context.getConfiguration();

  // In order to be used in MrGeo, this InputFormat must return instances
  // of TiledInputSplit. To do that, we need to determine the start and end
  // tile id's for each split. First we read the splits file and get the
  // partition info, then we break the partition into blocks, which become the
  // actual splits used.
  ImageInputFormatContext ifContext = ImageInputFormatContext.load(conf);
  int zoom = ifContext.getZoomLevel();
  int tilesize = ifContext.getTileSize();

  HdfsMrsImageDataProvider dp = createHdfsMrsImageDataProvider(context.getConfiguration());
  Path inputWithZoom = new Path(dp.getResourcePath(true), "" + zoom);

  // This appears to never be used
//  org.mrgeo.hdfs.tile.FileSplit splitfile = createFileSplit();
//  splitfile.readSplits(inputWithZoom);

  MrsPyramidMetadataReader metadataReader = dp.getMetadataReader();
  MrsPyramidMetadata metadata = metadataReader.read();

  org.mrgeo.hdfs.tile.FileSplit fsplit = createFileSplit();
  fsplit.readSplits(inputWithZoom);

  FileSplitInfo[] splits =
      (FileSplitInfo[]) fsplit.getSplits();

  List<InputSplit> result = new ArrayList<>(splits.length);

  Bounds requestedBounds = ifContext.getBounds();
  for (FileSplitInfo split : splits)
  {
    Path part = new Path(inputWithZoom, split.getName());
    Path dataFile = new Path(part, MapFile.DATA_FILE_NAME);

    long endTileId = split.getEndId();
    long startTileId = split.getStartId();

    if (requestedBounds != null)
    {
      // Do not include splits that can't possibly intersect the requested bounds. This
      // is an HDFS-specific efficiency to avoid needlessly processing splits.
      Tile startTile = TMSUtils.tileid(startTileId, zoom);
      Bounds startTileBounds = TMSUtils.tileBounds(startTile, zoom, tilesize);
      Tile endTile = TMSUtils.tileid(endTileId, zoom);
      Bounds endTileBounds = TMSUtils.tileBounds(endTile, zoom, tilesize);

      if (startTileBounds.s > requestedBounds.n || endTileBounds.n < requestedBounds.s)
      {
        // Ignore the split because it's either completely above or completey below
        // the requested bounds.
      }
      else
      {
        result.add(new TiledInputSplit(new FileSplit(dataFile, 0, 0, null), startTileId, endTileId,
            zoom, metadata.getTilesize()));
      }
    }
    else
    {
      // If no bounds were specified by the caller, then we include
      // all splits.
      result.add(new TiledInputSplit(new FileSplit(dataFile, 0, 0, null),
          startTileId, endTileId, zoom, metadata.getTilesize()));
    }
  }

  // The following code is useful for debugging. The gaps can be compared against the
  // contents of the actual index file for the partition to see if there are any gaps
  // in areas where there actually is tile information.
//    long lastEndTile = -1;
//    for (InputSplit split: result)
//    {
//      if (lastEndTile >= 0)
//      {
//        long startTileId = ((TiledInputSplit)split).getStartId();
//        if (startTileId > lastEndTile + 1)
//        {
//          log.error("Gap in splits: " + lastEndTile + " - " + startTileId);
//        }
//        lastEndTile = ((TiledInputSplit)split).getEndId();
//      }
//    }

  long end = System.currentTimeMillis();
  log.info("Time to generate splits: " + (end - start) + " ms");

  return result;
}

protected HdfsMrsImageDataProvider createHdfsMrsImageDataProvider(Configuration config)
{
  return new HdfsMrsImageDataProvider(config, input, null);
}

protected org.mrgeo.hdfs.tile.FileSplit createFileSplit()
{
  return new org.mrgeo.hdfs.tile.FileSplit();
}

}
