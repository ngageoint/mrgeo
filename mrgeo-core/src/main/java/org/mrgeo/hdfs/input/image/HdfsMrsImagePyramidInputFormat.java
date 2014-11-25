/*
 * Copyright 2009-2014 DigitalGlobe, Inc.
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

import org.apache.commons.lang.math.LongRange;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.InvalidInputException;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.mrgeo.image.MrsImagePyramid;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.mrgeo.mapreduce.splitters.TiledInputSplit;
import org.mrgeo.data.image.MrsImagePyramidMetadataReader;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.data.tile.TiledInputFormatContext;
import org.mrgeo.hdfs.image.HdfsMrsImageDataProvider;
import org.mrgeo.hdfs.input.MapFileFilter;
import org.mrgeo.hdfs.tile.SplitFile;
import org.mrgeo.utils.LongRectangle;
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

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException
  {
    Configuration conf = context.getConfiguration();
    // In order to be used in MrGeo, this InputFormat must return instances
    // of TiledInputSplit. To do that, we need to determine the start and end
    // tile id's for each split. First we read the splits.
    SplitFile sf = new SplitFile(conf);
    TiledInputFormatContext ifContext = TiledInputFormatContext.load(conf);
    HdfsMrsImageDataProvider dp = new HdfsMrsImageDataProvider(context.getConfiguration(),
        input, null);
    Path inputWithZoom = new Path(dp.getResourcePath(true), "" + ifContext.getZoomLevel());
    Path splitPath = new Path(inputWithZoom, SplitFile.SPLIT_FILE);
    FileSystem fs = splitPath.getFileSystem(conf);
    if (!fs.exists(splitPath))
    {
      // we renamed the split file, so if we can't find the new one, try the old name.
      splitPath = new Path(inputWithZoom, SplitFile.OLD_SPLIT_FILE);
    }
    List<Long> splitFileSplits = sf.readSplits(fs.makeQualified(splitPath).toString());
    // TODO: Not sure if the following really makes sense. I left it in here
    // commented out just in case.
//    MrsImagePyramidMetadata metadata = getSingleMetadata(conf);
    MrsImagePyramidMetadataReader metadataReader = dp.getMetadataReader();
    MrsImagePyramidMetadata metadata = metadataReader.read();
    LongRectangle tileBounds = metadata.getTileBounds(ifContext.getZoomLevel());
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
        throw new IOException("Expected FileSplit, got " +
            actualSplit.getClass().getCanonicalName());
      }
      FileSplit fileSplit = (FileSplit)actualSplit;
      String partFile = fileSplit.getPath().getParent().getName();
      if (!partFile.startsWith("part-"))
      {
        throw new IOException("Invalid FileSplit, expected path to start with 'part-': " + partFile);
      }
      // Example file name is "part-r-00000". Parse the number from the end because that
      // will be the index of the corresponding entry in the splits file
      String indexStr = partFile.substring(partFile.lastIndexOf('-')+1, partFile.length());
      int index = Integer.valueOf(indexStr);
      if (index < 0 || index > splitFileSplits.size())
      {
        throw new IOException("Invalid split index " + index + " obtained from split " +
            partFile + ". Value should be inclusively between 0 and " + (splitFileSplits.size() - 1));
      }

      long endTileId = -1;
      if (index < splitFileSplits.size())
      {
        endTileId = splitFileSplits.get(index);
      }
      else
      {
        endTileId = TMSUtils.tileid(tileBounds.getMaxX(), tileBounds.getMaxY(), ifContext.getZoomLevel());
      }
      TMSUtils.Tile endTile = TMSUtils.tileid(endTileId, ifContext.getZoomLevel());
      // The splits file contains only the end tile id. Getting the start tile id is more
      // complicated.
      // note we can't just do below because it assumes that each partition contains only a single 
      // row of data - it can contain more than a row
      //long startTileId = TMSUtils.tileid(tileBounds.getMinX(), endTile.ty, zoomLevel);
      
      // We read the first entry from the index for the partition corresponding to the split
      // to get the start tile id
      Path indexPath = new Path(fileSplit.getPath().getParent(), "index");
      SequenceFile.Reader reader = new SequenceFile.Reader(fs, indexPath, conf);
      TileIdWritable key;
      try
      {
        key = (TileIdWritable) reader.getKeyClass().newInstance();
        reader.next(key);
      }
      catch (InstantiationException e)
      {
        throw new IOException(e);
      }
      catch (IllegalAccessException e)
      {
        throw new IOException(e);
      }
      finally
      {
        if (reader != null)
        {
          reader.close();
        }
      }
      long startTileId = key.get();
      TMSUtils.Tile startTile = TMSUtils.tileid(startTileId, ifContext.getZoomLevel());
      TileBounds partFileTileBounds = new TileBounds(startTile.tx, startTile.ty, endTile.tx,
          endTile.ty);
      Bounds partFileBounds = TMSUtils.tileToBounds(partFileTileBounds, ifContext.getZoomLevel(),
          metadata.getTilesize());

      org.mrgeo.utils.Bounds requestedBounds = ifContext.getBounds();
      if (requestedBounds != null)
      {
        // Only include the split if it intersects the requested bounds.
        Bounds userBounds = TMSUtils.Bounds.convertOldToNewBounds(requestedBounds);
        if (userBounds.intersect(partFileBounds, false /* include adjacent splits */))
        {
          // Now that we know the split intersects the user bounds, get the start and
          // end tile id's of the actual tile
          LongRange splitTileIdRange = getSplitBounds(fileSplit, partFileTileBounds, ifContext.getZoomLevel(), fs, conf);
          TileBounds userTileBounds = TMSUtils.boundsToTile(userBounds, ifContext.getZoomLevel(),
              metadata.getTilesize());
          TMSUtils.Tile partStartTile = TMSUtils.tileid(splitTileIdRange.getMinimumLong(),
              ifContext.getZoomLevel()); 
          TMSUtils.Tile partEndTile = TMSUtils.tileid(splitTileIdRange.getMaximumLong(),
              ifContext.getZoomLevel());
          TileBounds splitTileBounds = new TileBounds(partStartTile.tx,
              partStartTile.ty, partEndTile.tx, partEndTile.ty);
          Bounds splitBounds = TMSUtils.tileToBounds(splitTileBounds,
              ifContext.getZoomLevel(), metadata.getTilesize());

          if (userBounds.intersect(splitBounds, false /* include adjacent splits */))
          {
            // If the tile bounds of the actual tile intersects the user bounds,
            // then return the actual tile bounds (instead of the full theoretical
            // range allowed 
            LOG.info(String
                .format(
                    "Split check passed split %s with splitBounds=%s, splitTileIdRange=%s, userBounds=%s, userTileBounds=%s",
                    fileSplit, splitBounds, splitTileIdRange, userBounds, userTileBounds));
            result.add(new TiledInputSplit(actualSplit, splitTileIdRange.getMinimumLong(),
                splitTileIdRange.getMaximumLong(), ifContext.getZoomLevel(),
                metadata.getTilesize()));
          }
          else
          {
            /*log.info(String.format(
                "Split check failed: split %s with splitBounds=%s, splitTileBounds=%s, userBounds=%s", split,
                splitBounds, splitTileBounds, userBounds));*/
          }
        }
        else
        {
          /*log.info(String.format(
              "Part file check failed: file %s with bounds=%s, tileBounds=%s, userBounds=%s", split,
              partFileBounds, partFileTileBounds, userBounds));*/
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
    return result;
  }

  private static LongRange getSplitBounds(FileSplit split, TileBounds partFileTileBounds, int zoomLevel, FileSystem fs,
      Configuration conf) throws IOException
  {
    long startOffset = split.getStart();
    long endOffset = startOffset + split.getLength();

    Path indexPath = new Path(split.getPath().getParent(), "index");
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, indexPath, conf);
    try
    {
      TileIdWritable tileId;
      try
      {
        tileId = (TileIdWritable) reader.getKeyClass().newInstance();
      }
      catch (InstantiationException e)
      {
        throw new IOException(e);
      }
      catch (IllegalAccessException e)
      {
        throw new IOException(e);
      }
      LongWritable tileOffset = new LongWritable();
  
      long startTileId = -1, endTileId = -1;

      // find startTileId
      while (reader.next(tileId, tileOffset))
      {
        if (tileOffset.get() >= startOffset)
        {
          startTileId = tileId.get();
          break;
        }
      }
  
      assert (startTileId != -1);
  
      // find endTileId
      while (reader.next(tileId, tileOffset) && tileOffset.get() < endOffset)
      {
        endTileId = tileId.get();
      }
      return new LongRange(startTileId, endTileId);
    }
    finally
    {
      reader.close();
    }
//    TMSUtils.Tile startTile = TMSUtils.tileid(startTileId, zoomLevel);
//    TMSUtils.Tile endTile = TMSUtils.tileid(endTileId, zoomLevel);
//    
//    long startTx = (startTile.ty < endTile.ty) ? partFileTileBounds.w : startTile.tx;
//    long endTx = (startTile.ty < endTile.ty) ? partFileTileBounds.e : endTile.tx;
//
//    TileBounds tb = new TileBounds(startTx, startTile.ty, endTx, endTile.ty); 
//
//    /*log.info(String.format("startTileId=%d, tx=%d, ty=%d, endTileId=%d, tx=%d, ty=%d, startTx=%d, endTx=%d", 
//        startTileId, startTile.tx, startTile.ty, endTileId, endTile.tx, endTile.ty, startTx, endTx)); */
//
//    return tb;    
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
    HdfsMrsImageDataProvider dp = new HdfsMrsImageDataProvider(job.getConfiguration(),
        input, null);
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
