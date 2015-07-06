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

package org.mrgeo.hdfs.input.image;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.InvalidInputException;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.mrgeo.data.image.MrsImagePyramidMetadataReader;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.data.tile.TiledInputFormatContext;
import org.mrgeo.hdfs.image.HdfsMrsImageDataProvider;
import org.mrgeo.hdfs.input.MapFileFilter;
import org.mrgeo.hdfs.tile.Splits;
import org.mrgeo.image.MrsImagePyramid;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.mrgeo.mapreduce.splitters.TiledInputSplit;
import org.mrgeo.utils.TMSUtils;
import org.mrgeo.utils.TMSUtils.Bounds;
import org.mrgeo.utils.TMSUtils.TileBounds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;


public class HdfsMrsImagePyramidInputFormat extends SequenceFileInputFormat<TileIdWritable,RasterWritable>
{
  private static Logger LOG = LoggerFactory.getLogger(HdfsMrsImagePyramidInputFormat.class);
  private String input;
  private int inputZoom;

  // Copied from Hadoop 1.2.1 FileInputFormat
  private static final PathFilter hiddenFileFilter = new PathFilter(){
    public boolean accept(Path p){
      String name = p.getName();
      return !name.startsWith("_") && !name.startsWith(".");
    }
  };

  // Copied from Hadoop 1.2.1 FileInputFormat
  private static class MultiPathFilter implements PathFilter {
    private List<PathFilter> filters;

    public MultiPathFilter(List<PathFilter> filters) {
      this.filters = filters;
    }

    public boolean accept(Path path) {
      for (PathFilter filter : filters) {
        if (!filter.accept(path)) {
          return false;
        }
      }
      return true;
    }
  }

  /**
   * This constructor should only be used by the Hadoop framework on the data
   * node side when the InputFormat is constructed in order to get a RecordReader.
   * On the "Driver" side, developers should use the DataProviderFactory to get
   * and InputFormatProvider, from which the InputFormat can be obtained.
   */
  public HdfsMrsImagePyramidInputFormat()
  {
  }

  public HdfsMrsImagePyramidInputFormat(final String input, final int zoom)
  {
    this.input = input;
    this.inputZoom = zoom;
  }

  @Override
  public RecordReader<TileIdWritable, RasterWritable> createRecordReader(final InputSplit split,
      final TaskAttemptContext context)
      throws IOException
  {
    return new HDFSMrsImagePyramidRecordReader();
  }

  public static String getZoomName(final HdfsMrsImageDataProvider dp,
      final int zoomLevel)
  {
    try
    {
      MrsImagePyramid pyramid = MrsImagePyramid.open(dp);
      String zoomName = pyramid.getMetadata().getName(zoomLevel);
      if (zoomName != null)
      {
        return new Path(dp.getResourcePath(true), zoomName).toUri().toString();
      }
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException
  {
    Configuration conf = context.getConfiguration();

    // In order to be used in MrGeo, this InputFormat must return instances
    // of TiledInputSplit. To do that, we need to determine the start and end
    // tile id's for each split. First we read the splits.

    TiledInputFormatContext ifContext = TiledInputFormatContext.load(conf);
    final int zoom = ifContext.getZoomLevel();
    final int tilesize = ifContext.getTileSize();

    HdfsMrsImageDataProvider dp = new HdfsMrsImageDataProvider(context.getConfiguration(),
        input, null);
    Path inputWithZoom = new Path(dp.getResourcePath(true), "" + zoom);

    org.mrgeo.hdfs.tile.FileSplit splitfile = new org.mrgeo.hdfs.tile.FileSplit();
    splitfile.readSplits(inputWithZoom);

    MrsImagePyramidMetadataReader metadataReader = dp.getMetadataReader();
    MrsImagePyramidMetadata metadata = metadataReader.read();

    // We need to read the splits file which contains the ending tile id for
    // each split. For each split, read its corresponding index file to
    // determine the starting tile id for that split.
    List<InputSplit> actualSplits = super.getSplits(context);
    List<InputSplit> result = new ArrayList<InputSplit>(actualSplits.size());
    for (InputSplit actualSplit : actualSplits)
    {
      // Read the HDFS index file for this split to 
      if (!(actualSplit instanceof FileSplit))
      {
        throw new IOException("Expected (Hadoop) FileSplit, got " + actualSplit.getClass().getCanonicalName());
      }
      FileSplit fileSplit = (FileSplit)actualSplit;

      String partFile = fileSplit.getPath().getParent().getName();
      if (!partFile.startsWith("part-"))
      {
        throw new IOException("Invalid (Hadoop) FileSplit, expected path to start with 'part-': " + partFile);
      }

      org.mrgeo.hdfs.tile.FileSplit.FileSplitInfo splitinfo;
      try
      {
        splitinfo =
            (org.mrgeo.hdfs.tile.FileSplit.FileSplitInfo) splitfile.getSplitByName(partFile);
      }
      catch (Splits.SplitException e)
      {
        // Example file name is "part-r-00000". Parse the number from the end because that
        // will be the index of the corresponding entry in the splits file
        String indexStr = partFile.substring(partFile.lastIndexOf('-') + 1, partFile.length());
        int partition = Integer.valueOf(indexStr);

        splitinfo =
            (org.mrgeo.hdfs.tile.FileSplit.FileSplitInfo) splitfile.getSplitByPartition(partition);
      }

      long endTileId = splitinfo.getEndId();
      long startTileId = splitinfo.getStartId();

      TMSUtils.Tile startTile = TMSUtils.tileid(startTileId, zoom);
      TMSUtils.Tile endTile = TMSUtils.tileid(endTileId, zoom);

      TileBounds partFileTileBounds = new TileBounds(startTile.tx, startTile.ty, endTile.tx,
          endTile.ty);
      Bounds partFileBounds = TMSUtils.tileToBounds(partFileTileBounds, zoom, tilesize);

      if (ifContext.getBounds() != null)
      {
        Bounds requestedBounds = ifContext.getBounds().getTMSBounds();

        // Only include the split if it intersects the requested bounds.
        if (requestedBounds.intersect(partFileBounds, false /* include adjacent splits */))
        {
          Bounds intersected = requestedBounds.intersection(partFileBounds, false);

          TMSUtils.TileBounds tb = TMSUtils.boundsToTile(intersected, zoom, tilesize);

          // If the tile bounds of the actual split intersects the user bounds,
          // then return the actual split bounds (instead of the full theoretical
          // range allowed).
          long s = TMSUtils.tileid(tb.w, tb.s, zoom);
          long e = TMSUtils.tileid(tb.e, tb.n, zoom);
          result.add(new TiledInputSplit(actualSplit, s, e, zoom, metadata.getTilesize()));
        }
      }
      else
      {
        // If no bounds were specified by the caller, then we include
        // all splits.
        result.add(new TiledInputSplit(actualSplit, startTileId, endTileId,
            ifContext.getZoomLevel(), metadata.getTilesize()));
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
//          LOG.error("Gap in splits: " + lastEndTile + " - " + startTileId);
//        }
//        lastEndTile = ((TiledInputSplit)split).getEndId();
//      }
//    }
    return result;
  }


  public static void setInputInfo(final Job job, final int zoomlevel,
      final Set<String> inputsWithZoom) throws IOException
  {
//    job.setInputFormatClass(HdfsMrsImagePyramidInputFormat.class);

    //final String scannedInput = inputs.get(0);
    //FileInputFormat.addInputPath(job, new Path(scannedInput));

    for (String inputWithZoom: inputsWithZoom)
    {
      FileInputFormat.addInputPath(job, new Path(inputWithZoom));
    }
    FileInputFormat.setInputPathFilter(job, MapFileFilter.class);
  }


  // Overriding the following method results in the SequenceFileInputFormat
  // using the returned FileStatus objects to split on, rather than using
  // the results of FileInputStatus.getInputPaths() which would return all
  // of the inputs being used rather than just the one we want splits for.
  // Copied from Hadoop 1.2.1 FileInputFormat and modified to use only the
  // input data member, and it recurses through the directories below it.
  @Override
  protected List<FileStatus> listStatus(JobContext job) throws IOException
  {
    List<FileStatus> result = new ArrayList<FileStatus>();
    HdfsMrsImageDataProvider dp = new HdfsMrsImageDataProvider(job.getConfiguration(), input, null);
    String inputWithZoom = getZoomName(dp, inputZoom);

    // We are going to read all of the input dirs
    Path[] dirs = new Path[] { new Path(inputWithZoom)} ;

    // get tokens for all the required FileSystems..
    TokenCache.obtainTokensForNamenodes(job.getCredentials(), dirs,
        job.getConfiguration());

    List<IOException> errors = new ArrayList<IOException>();

    // creates a MultiPathFilter with the hiddenFileFilter and the
    // user provided one (if any).
    List<PathFilter> filters = new ArrayList<PathFilter>();
    filters.add(hiddenFileFilter);
    PathFilter jobFilter = getInputPathFilter(job);
    if (jobFilter != null) {
      filters.add(jobFilter);
    }
    PathFilter inputFilter = new MultiPathFilter(filters);

    for (int i=0; i < dirs.length; ++i) {
      Path p = dirs[i];
      FileSystem fs = p.getFileSystem(job.getConfiguration());
      FileStatus[] matches = fs.globStatus(p, inputFilter);
      if (matches == null) {
        errors.add(new IOException("Input path does not exist: " + p));
      } else if (matches.length == 0) {
        errors.add(new IOException("Input Pattern " + p + " matches 0 files"));
      } else {
        for (FileStatus globStat: matches) {
          findInputs(globStat, fs, inputFilter, result);
        }
      }
    }

    if (!errors.isEmpty()) {
      throw new InvalidInputException(errors);
    }
    LOG.info("Total input paths to process : " + result.size());
    return result;
  }

  private static void findInputs(final FileStatus status, final FileSystem fs,
      final PathFilter inputFilter, List<FileStatus> result) throws IOException
  {
    if (status.isDir()) {
      for(FileStatus childStat: fs.listStatus(status.getPath(),
          inputFilter)) {
        if (childStat.isDir())
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
}
