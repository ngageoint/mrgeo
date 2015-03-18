package org.mrgeo.data;

import org.mrgeo.mapreduce.splitters.TiledInputSplit;
import org.mrgeo.utils.TMSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegionSplitVisitor implements SplitVisitor
{
  static Logger log = LoggerFactory.getLogger(SplitVisitor.class);
  private TMSUtils.TileBounds region;

  public RegionSplitVisitor(TMSUtils.TileBounds region)
  {
    this.region = region;
    log.debug("Created RegionSplitVisitor with region: " + region.toString());
  }

  @Override
  public boolean accept(TiledInputSplit split)
  {
    int zoom = split.getZoomLevel();
    TMSUtils.Tile startTile = TMSUtils.tileid(split.getStartTileId(), zoom);
    TMSUtils.Tile endTile = TMSUtils.tileid(split.getEndTileId(), zoom);
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

  boolean splitOverlapsTileBounds(final TMSUtils.Tile splitStartTile,
                                  final TMSUtils.Tile splitEndTile, final TMSUtils.TileBounds cropBounds)
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
