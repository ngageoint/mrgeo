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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.hdfs.tile.SplitFile;
import org.mrgeo.image.MrsImagePyramid;
import org.mrgeo.utils.LongRectangle;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class HdfsMrsImagePyramidSimpleInputFormat extends SequenceFileInputFormat<TileIdWritable, RasterWritable>
{
  @Override
  public RecordReader<TileIdWritable, RasterWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException
  {
    return new HdfsMrsImagePyramidSimpleRecordReader();
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException
  {
    //return super.getSplits(context);

    Configuration conf = context.getConfiguration();

    Path[] inputs = FileInputFormat.getInputPaths(context);

    for (Path input : inputs)
    {
      MrsImagePyramid pyramid = MrsImagePyramid.open(input.getParent().toString(), conf);

      int zoom = Integer.decode(input.getName());
      int tilesize = pyramid.getTileSize();

      LongRectangle tileBounds = pyramid.getTileBounds(zoom);

      SplitFile sf = new SplitFile(conf);
      Path splitPath = new Path(input, SplitFile.SPLIT_FILE);
      FileSystem fs = splitPath.getFileSystem(conf);
      if (!fs.exists(splitPath))
      {
        // we renamed the split file, so if we can't find the new one, try the old name.
        splitPath = new Path(input, SplitFile.OLD_SPLIT_FILE);
      }

      List<Long> splitFileSplits = new ArrayList<Long>();
      List<String> splitFilePartitions = new ArrayList<String>();


      sf.readSplits(fs.makeQualified(splitPath).toString(), splitFileSplits, splitFilePartitions);

      // We need to read the splits file which contains the ending tile id for
      // each split. For each split, read its corresponding index file to
      // determine the starting tile id for that split.

      List<InputSplit> actualSplits = super.getSplits(context);
      List<InputSplit> result = new ArrayList<InputSplit>(actualSplits.size());

      Path lastFile = null;
      SequenceFile.Reader reader = null;

      for (InputSplit actualSplit : actualSplits)
      {
        // Read the HDFS index file for this split to
        if (!(actualSplit instanceof FileSplit))
        {
          throw new IOException("Expected FileSplit, got " +
              actualSplit.getClass().getCanonicalName());
        }

        FileSplit fileSplit = (FileSplit) actualSplit;
        if (lastFile != fileSplit.getPath())
        {
          String partFile = fileSplit.getPath().getParent().getName();
          if (!partFile.startsWith("part-"))
          {
            throw new IOException("Invalid FileSplit, expected path to start with 'part-': " + partFile);
          }

          if (reader != null)
          {
            reader.close();
          }
          lastFile = fileSplit.getPath();

          //reader = new SequenceFile.Reader(fs, fileSplit.getPath(), conf);
          reader = new SequenceFile.Reader(conf,
              SequenceFile.Reader.file(fileSplit.getPath()));
        }

        if (reader == null)
        {
          throw new IOException("Everything is messed up!");
        }

        TileIdWritable startTileId = new TileIdWritable();

        reader.sync(fileSplit.getStart());
        RasterWritable value = new RasterWritable();
        if (reader.next(startTileId, value))
        {
          //System.out.println("start: " + startTileId);
        }

        // seek to just before the last record...
        long seeker = fileSplit.getStart() + (fileSplit.getLength() - (long) (value.getLength() * 1.05));
        if (seeker < 0)
        {
          seeker = 0;
        }
        reader.sync(seeker);

        TileIdWritable endTileId = new TileIdWritable();
        while (reader.next(endTileId) &&
            reader.getPosition() <= (fileSplit.getStart() + fileSplit.getLength()))
        {
          //System.out.println("skipping: " + endTileId);
        }
        //System.out.println("end: " + endTileId);

        result.add(new SimplePyramidInputSplit(pyramid.getName(), startTileId.get(), endTileId.get(), zoom, tilesize));
      }

      if (reader != null)
      {
        reader.close();
      }

      return result;

    }

    return new ArrayList<InputSplit>();
  }

}
