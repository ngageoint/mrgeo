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

package org.mrgeo.hdfs.partitioners;

import org.mrgeo.hdfs.tile.FileSplit;
import org.mrgeo.hdfs.tile.PartitionerSplit;
import org.mrgeo.hdfs.tile.SplitInfo;
import org.mrgeo.utils.LongRectangle;
import org.mrgeo.utils.TMSUtils;

import java.util.ArrayList;
import java.util.List;

public class ImageSplitGenerator implements SplitGenerator {
  final long minTileX;
  final long minTileY;
  final long maxTileX;
  final long maxTileY;
  final int zoomLevel;
  final int increment;

  public ImageSplitGenerator(final long minTileX, final long minTileY, 
                final long maxTileX, final long maxTileY,
                final int zoomLevel, final int increment) {
    this.minTileX = minTileX;
    this.minTileY = minTileY;
    this.maxTileX = maxTileX;
    this.maxTileY = maxTileY;
    this.zoomLevel = zoomLevel;
    this.increment = increment;
  }

  public ImageSplitGenerator(final LongRectangle tileBounds,
      final int zoomLevel, final int tileSizeBytes, final long blockSizeBytes)
  {
    this(tileBounds.getMinX(), tileBounds.getMinY(),
        tileBounds.getMaxX(), tileBounds.getMaxY(),
        zoomLevel,
        computeIncrement(tileBounds, tileSizeBytes, blockSizeBytes));
  }

  public ImageSplitGenerator(final LongRectangle tileBounds,
      final int zoomLevel, final int tileSizeBytes,
      final long blockSizeBytes, final int maxPartitions)
  {
    this(tileBounds.getMinX(), tileBounds.getMinY(),
        tileBounds.getMaxX(), tileBounds.getMaxY(),
        zoomLevel,
        computeIncrement(tileBounds, tileSizeBytes, blockSizeBytes, maxPartitions));
  }

  private static int computeIncrement(final LongRectangle tileBounds,
      final int tileSizeBytes, final long blockSizeBytes)
  {
    long tilesPerBlock = (int) (blockSizeBytes / tileSizeBytes);
    long tileCount = tileBounds.getHeight() * tileBounds.getWidth();
    if (tilesPerBlock >= tileCount)
    {
      return -1;
    }
    int increment = (int) (tilesPerBlock / tileBounds.getWidth());
    if (increment == 0)
    {
      increment = 1;
    }
    return increment;
  }

  private static int computeIncrement(final LongRectangle tileBounds,
      final int tileSizeBytes, final long blockSizeBytes, final int maxPartitions)
  {
    int increment = computeIncrement(tileBounds, tileSizeBytes, blockSizeBytes);
    long partitions = tileBounds.getHeight() / increment;
    if (partitions > maxPartitions)
    {
      increment = (int)Math.ceil((double)tileBounds.getHeight() / (double)maxPartitions);
    }
    return increment;
  }

  @Override
  public SplitInfo[] getSplits()
  {
    List<FileSplit.FileSplitInfo> splits = new ArrayList<FileSplit.FileSplitInfo>();

    // If increment < 0, then that means no splits are required because all of
    // the tiles will fit in a single block.
    if (increment > 0)
    {
      // The first split is increment rows above the minTileY, and there
      // are subsequent splits increment rows high up until maxTileY. The
      // final split may be <= increment rows high.
      int partition = 0;
      for(long i = minTileY + increment - 1; i < maxTileY; i += increment, partition++)
      {
        splits.add(new FileSplit.FileSplitInfo(
            TMSUtils.tileid(minTileX, i, zoomLevel),
            TMSUtils.tileid(maxTileX, i, zoomLevel),
            "", partition));
      }
      // Add the last split
      splits.add(new FileSplit.FileSplitInfo(
          TMSUtils.tileid(minTileX, maxTileY, zoomLevel),
          TMSUtils.tileid(maxTileX, maxTileY, zoomLevel),
          "", partition));
    }

    return splits.toArray(new FileSplit.FileSplitInfo[splits.size()]);
  }

  @Override
  public SplitInfo[] getPartitions()
  {
    List<PartitionerSplit.PartitionerSplitInfo> splits =
        new ArrayList<PartitionerSplit.PartitionerSplitInfo>();

    // If increment < 0, then that means no splits are required because all of
    // the tiles will fit in a single block.
    if (increment > 0)
    {
      // The first split is increment rows above the minTileY, and there
      // are subsequent splits increment rows high up until maxTileY. The
      // final split may be <= increment rows high.
      int partition = 0;
      for(long i = minTileY + increment - 1; i < maxTileY; i+=increment, partition++)
      {
        splits.add(new PartitionerSplit.PartitionerSplitInfo(
            TMSUtils.tileid(maxTileX, i, zoomLevel),
            partition));
      }
      // Add the last split
      splits.add(new PartitionerSplit.PartitionerSplitInfo(
          TMSUtils.tileid(maxTileX, maxTileY, zoomLevel),
          partition));
    }

    return splits.toArray(new PartitionerSplit.PartitionerSplitInfo[splits.size()]);
  }

}
