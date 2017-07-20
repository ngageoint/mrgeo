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

package org.mrgeo.data;

import org.mrgeo.mapreduce.splitters.TiledInputSplit;
import org.mrgeo.utils.tms.TMSUtils;
import org.mrgeo.utils.tms.Tile;
import org.mrgeo.utils.tms.TileBounds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegionSplitVisitor implements SplitVisitor
{
static Logger log = LoggerFactory.getLogger(SplitVisitor.class);
private TileBounds region;

public RegionSplitVisitor(TileBounds region)
{
  this.region = region;
  log.debug("Created RegionSplitVisitor with region: " + region);
}

@Override
public boolean accept(TiledInputSplit split)
{
  int zoom = split.getZoomLevel();
//    Tile startTile = TMSUtils.tileid(split.getStartTileId(), zoom);
//    Tile endTile = TMSUtils.tileid(split.getEndTileId(), zoom);
  boolean result = splitOverlapsTileBounds(
      TMSUtils.tileid(split.getStartTileId(), zoom),
      TMSUtils.tileid(split.getEndTileId(), zoom),
      region);
  if (!result)
  {
    log.info("Skipping split starting at tile " + split.getStartTileId());
  }
  return result;
}

boolean splitOverlapsTileBounds(Tile splitStartTile,
    Tile splitEndTile, TileBounds cropBounds)
{
  // If the split is either before or beyond the crop, then skip this split
  if (splitEndTile.ty < cropBounds.s || (splitEndTile.ty == cropBounds.s && splitEndTile.tx < cropBounds.w))
  {
    return false;
  }
  else if (splitStartTile.ty > cropBounds.n || (splitStartTile.ty == cropBounds.n && splitStartTile.tx > cropBounds.e))
  {
    return false;
  }

  // Check the more complicated overlap cases now. If the vertical difference
  // between the start tile and end tile is more than one row, then than
  // split physically has to overlap the crop area because there is at
  // least one full row of tiles included in the split.
  long yDelta = splitEndTile.ty - splitStartTile.ty;
  boolean intersect = false;
  if (yDelta > 1)
  {
    intersect = true;
  }
  else if (yDelta == 0)
  {
    if (splitEndTile.tx < cropBounds.w || splitStartTile.tx > cropBounds.e)
    {
      intersect = false;
    }
    else
    {
      intersect = true;
    }
  }
  else
  {
    // In this case, the split starts in one row, and ends in the row
    // immediately above it. Now we need to check on where the start tile
    // x position and end tile x position are relative to the crop area
    // to determine if there is an intersection
    if (splitStartTile.tx <= cropBounds.e || splitEndTile.tx >= cropBounds.w)
    {
      intersect = true;
    }
  }
  return intersect;
}
}
