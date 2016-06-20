/*
 * Copyright 2009-2016 DigitalGlobe, Inc.
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
 *
 */

package org.mrgeo.hdfs.input.image;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.mrgeo.data.image.MrsPyramidMetadataReader;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.data.image.ImageInputFormatContext;
import org.mrgeo.hdfs.image.HdfsMrsImageDataProvider;
import org.mrgeo.hdfs.input.MapFileFilter;
import org.mrgeo.image.MrsPyramid;
import org.mrgeo.mapreduce.splitters.TiledInputSplit;
import org.mrgeo.image.MrsPyramidMetadata;
import org.mrgeo.utils.tms.Bounds;
import org.mrgeo.utils.tms.TMSUtils;
import org.mrgeo.utils.tms.TileBounds;
import org.mrgeo.utils.tms.Tile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class HdfsMrsPyramidInputFormat extends InputFormat<TileIdWritable,RasterWritable>
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

public HdfsMrsPyramidInputFormat(final String input)
{
    this.input = input;
}

@Override
public RecordReader<TileIdWritable, RasterWritable> createRecordReader(final InputSplit split,
    final TaskAttemptContext context)
    throws IOException
{
  return new HdfsMrsPyramidRecordReader();
}

public static String getZoomName(final HdfsMrsImageDataProvider dp,
    final int zoomLevel)
{
  try
  {
    MrsPyramid pyramid = MrsPyramid.open(dp);
    MrsPyramidMetadata metadata = pyramid.getMetadata();
    String zoomName = pyramid.getMetadata().getName(zoomLevel);
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
  final int zoom = ifContext.getZoomLevel();
  final int tilesize = ifContext.getTileSize();

  HdfsMrsImageDataProvider dp = createHdfsMrsImageDataProvider(context.getConfiguration());
  Path inputWithZoom = new Path(dp.getResourcePath(true), "" + zoom);

  // This appears to never be used
//  org.mrgeo.hdfs.tile.FileSplit splitfile = createFileSplit();
//  splitfile.readSplits(inputWithZoom);

  MrsPyramidMetadataReader metadataReader = dp.getMetadataReader();
  MrsPyramidMetadata metadata = metadataReader.read();

  org.mrgeo.hdfs.tile.FileSplit fsplit = createFileSplit();
  fsplit.readSplits(inputWithZoom);

  org.mrgeo.hdfs.tile.FileSplit.FileSplitInfo[] splits =
      (org.mrgeo.hdfs.tile.FileSplit.FileSplitInfo[]) fsplit.getSplits();

  List<InputSplit> result = new ArrayList<>(splits.length);

  final Bounds requestedBounds = ifContext.getBounds();
  for (org.mrgeo.hdfs.tile.FileSplit.FileSplitInfo split : splits)
  {
    final Path part = new Path(inputWithZoom, split.getName());
    final Path dataFile = new Path(part, MapFile.DATA_FILE_NAME);

    final long endTileId = split.getEndId();
    final long startTileId = split.getStartId();

    final Tile startTile = TMSUtils.tileid(startTileId, zoom);
    final Tile endTile = TMSUtils.tileid(endTileId, zoom);

    final TileBounds partFileTileBounds =
        new TileBounds(startTile.tx, startTile.ty, endTile.tx, endTile.ty);

    final Bounds partFileBounds = TMSUtils.tileToBounds(partFileTileBounds, zoom, tilesize);

    if (requestedBounds != null)
    {
      // Only include the split if it intersects the requested bounds.
      if (requestedBounds.intersects(partFileBounds, false /* include adjacent splits */))
      {
        Bounds intersected = requestedBounds.intersection(partFileBounds, false);

        TileBounds tb = TMSUtils.boundsToTile(intersected, zoom, tilesize);

        // If the tile bounds of the actual split intersects the user bounds,
        // then return the actual split bounds (instead of the full theoretical
        // range allowed).
        long s = TMSUtils.tileid(tb.w, tb.s, zoom);
        long e = TMSUtils.tileid(tb.e, tb.n, zoom);
        result.add(new TiledInputSplit(new FileSplit(dataFile, 0, 0, null), s, e,
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

  public static void setInputInfo(final Job job, final String inputWithZoom) throws IOException
{
//    job.setInputFormatClass(HdfsMrsPyramidInputFormat.class);

  //final String scannedInput = inputs.get(0);
  //FileInputFormat.addInputPath(job, new Path(scannedInput));

  FileInputFormat.addInputPath(job, new Path(inputWithZoom));
  FileInputFormat.setInputPathFilter(job, MapFileFilter.class);
}



private static void findInputs(final FileStatus status, final FileSystem fs,
    final PathFilter inputFilter, List<FileStatus> result) throws IOException
{
  if (status.isDirectory()) {
    for(FileStatus childStat: fs.listStatus(status.getPath(), inputFilter)) {
      if (childStat.isDirectory())
      {
        findInputs(childStat, fs, inputFilter, result);
      }
      else
      {
        result.add(childStat);
      }
    }
  } else {
    result.add(status);
  }
}

  protected HdfsMrsImageDataProvider createHdfsMrsImageDataProvider(Configuration config) {
    return new HdfsMrsImageDataProvider(config, input, null);
  }

  protected org.mrgeo.hdfs.tile.FileSplit createFileSplit() {
    return new org.mrgeo.hdfs.tile.FileSplit();
  }

}
