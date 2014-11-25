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

package org.mrgeo.opimage;

import org.mrgeo.rasterops.OpImageUtils;
import org.mrgeo.utils.TMSUtils;

import javax.media.jai.OpImage;
import javax.media.jai.PlanarImage;
import java.awt.*;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.WritableRaster;
import java.util.Vector;

@SuppressWarnings({ "unchecked" })
public class CropRasterOpImage extends OpImage implements TileLocator
{
  public static final String FAST = "FAST";
  public static final String EXACT = "EXACT";
  private static final double EPSILON = 1e-8;

  private RenderedImage src;
  
  /**
   * The bottom-left crop point in pixel coordinates (with 0,0 being UL in the tile)
   * within one tile. In
   * the left-most column of tiles, all the pixels to the left of px should
   * be "cropped" = e.g. set to NoData. In the bottom-most row of tiles,
   * all the pixels greater than py should be "cropped" - e.g. set to NoData.
   */
  TMSUtils.Pixel bottomLeftCropPixel;
  
  /**
   * The top-right crop point in pixel coordinates (with 0,0 being UL in the tile)
   * within one tile. In
   * the right-most column of tiles, all the pixels to the right of px should
   * be "cropped" = e.g. set to NoData. And in the top-most row of tiles,
   * all the pixels less than than py should be "cropped" - e.g. set to NoData.
   */
  TMSUtils.Pixel topRightCropPixel;
  
  private boolean passThrough;
  private long tileX;
  private long tileY;
  private int zoomlevel;
  private int tileSize;
  private TMSUtils.TileBounds cropToTileBounds;
  private TMSUtils.Bounds cropBounds;
  private double noDataValue;

  public static CropRasterOpImage create(RenderedImage src, double x,
      double y, double width, double height,
      int zoomLevel, int tileSize, String cropType)
  {
    @SuppressWarnings("rawtypes")
    Vector sources = new Vector();
    sources.add(src);
    if (!cropType.equals(FAST) && !cropType.equals(EXACT))
    {
      throw new IllegalArgumentException(String.format("Invalid crop type '%s'", cropType));
    }
    return new CropRasterOpImage(sources, x, y, width, height,
        zoomLevel, tileSize, cropType);
  }

  @SuppressWarnings("rawtypes")
  private CropRasterOpImage(Vector sources, double x,
      double y, double width, double height,
      int zoomlevel, int tileSize, String cropType)
  {
    super(sources, null, null, false);

    cropBounds = new TMSUtils.Bounds(x, y, x + width, y + height);
    
    src = (RenderedImage) sources.get(0);
    this.zoomlevel = zoomlevel;
    this.tileSize = tileSize;
    
    
    calculateCrop();
    passThrough = cropType.equals(FAST);
    noDataValue = OpImageUtils.getNoData(src, Double.NaN);
  }

  private void calculateCrop()
  {
    // Compute the pixel boundaries within which we want to crop the source image.
    TMSUtils.Pixel bottomRightWorldPixel = TMSUtils.latLonToPixelsUL(cropBounds.s, 
        cropBounds.e, zoomlevel, tileSize);
    
    // Because the world coordinates of a pixel are anchored at the
    // top-left corner of the pixel, if the bottom-right lat or lon are
    // exactly on a pixel boundary, then we need to exclude the last
    // pixel on the right and/or bottom of the crop area because
    // the actual area of the pixel is actually outside of the crop box.
    TMSUtils.LatLon bottomRightAtPixelBoundary = TMSUtils.pixelToLatLonUL(bottomRightWorldPixel.px, bottomRightWorldPixel.py, zoomlevel, tileSize);
    if (Math.abs(bottomRightAtPixelBoundary.lat - cropBounds.n) < EPSILON)
    {
      bottomRightWorldPixel = new TMSUtils.Pixel(bottomRightWorldPixel.px,
          bottomRightWorldPixel.py - 1);
    }
    if (Math.abs(bottomRightAtPixelBoundary.lon - cropBounds.e) < EPSILON)
    {
      bottomRightWorldPixel = new TMSUtils.Pixel(bottomRightWorldPixel.px - 1,
          bottomRightWorldPixel.py);
    }
    TMSUtils.LatLon bottomRightPt = TMSUtils.pixelToLatLonUL(
        bottomRightWorldPixel.px, bottomRightWorldPixel.py, zoomlevel, tileSize);

    // DESIGN NOTE:
    // For efficiency in computeRect, we want to limit the amount of TMSUtils
    // computations are required while looping through the pixels of each
    // tile. As we process a tile, we know the tileX and tileY coordinates of
    // that tile, and of course we know the coordinates of the pixel within
    // that tile as we process them. Rather than converting each pixel to
    // worldwide coordinates, we compute the coordinates of the edge tiles as
    // well as the local pixel coordinates for where the image will crop and
    // use those values in computeRect to determine whether to copy the source
    // pixel or crop it (e.g. assign NoData to it). See the logic in
    // computeRect.
    
    // Compute the tile bounds for the actual crop area.
    TMSUtils.Bounds b = new TMSUtils.Bounds(cropBounds.w, bottomRightPt.lat, bottomRightPt.lon, 
        cropBounds.n);
    cropToTileBounds = TMSUtils.boundsToTile(b, zoomlevel, tileSize);
    // Find the pixel coordinates within the edge tiles that correspond to
    // where the crop should occur. During processing, if the current tile
    // is one of the edge tiles, then we can use the pixel coordinates below
    // to determine which pixels should be copied from the source or set to
    // NoData, and it doesn't require converting each pixel to world
    // pixel coordinates to do so.
    topRightCropPixel = TMSUtils.latLonToTilePixelUL(cropBounds.n, bottomRightPt.lon,
        cropToTileBounds.e, cropToTileBounds.n, zoomlevel, tileSize);
    bottomLeftCropPixel = TMSUtils.latLonToTilePixelUL(bottomRightPt.lat, cropBounds.w,
        cropToTileBounds.w, cropToTileBounds.s, zoomlevel, tileSize);
  }

  private void crop(Raster r, WritableRaster dest, Rectangle destRect,
      int minCopyX, int minCopyY, int maxCopyX, int maxCopyY)
  {
    for (int y = destRect.y; y < destRect.y + destRect.height; y++)
    {
      for (int x = destRect.x; x < destRect.x + destRect.width; x++)
      {
        if (x < minCopyX || x > maxCopyX || y < minCopyY || y > maxCopyY)
        {
          dest.setSample(x, y, 0, noDataValue);
        }
        else
        {
          final double v = r.getSampleDouble(x, y, 0);
          dest.setSample(x, y, 0, v);
        }
      }
    }
  }

  private void fillNoData(WritableRaster dest, Rectangle destRect)
  {
    for (int y = destRect.y; y < destRect.y + destRect.height; y++)
    {
      for (int x = destRect.x; x < destRect.x + destRect.width; x++)
      {
        dest.setSample(x, y, 0, noDataValue);
      }
    }
  }

  @Override
  protected void computeRect(PlanarImage[] sources, WritableRaster dest, Rectangle destRect)
  {
    // If the crop bounds do not intersect the tile being worked on, fill
    // with NoData
    if (tileX < cropToTileBounds.w || tileX > cropToTileBounds.e ||
        tileY < cropToTileBounds.s || tileY > cropToTileBounds.n)
    {
      fillNoData(dest, destRect);
      return;
    }
    if (passThrough)
    {
      dest.setRect(destRect.x, destRect.y, src.getData());
    }
    else // EXACT
    {
      int minCopyX = destRect.x;
      int maxCopyX = destRect.x + destRect.width;
      int minCopyY = destRect.y;
      int maxCopyY = destRect.y + destRect.height;
      boolean doCrop = false;
      
      // The following logic is dependent on the pixel coordinates
      // starting with 0,0 in the top-left of the tile.
      if (tileX == cropToTileBounds.e)
      {
        // Processing the right-most column of tiles.
        maxCopyX = (int)topRightCropPixel.px;
        doCrop = true;
      }
      
      if (tileX == cropToTileBounds.w)
      {
        // Processing the left-most column of tiles.
        minCopyX = (int)bottomLeftCropPixel.px;
        doCrop = true;
      }
      
      if (tileY == cropToTileBounds.n)
      {
        // Processing the top-most column of tiles.
        minCopyY = (int)topRightCropPixel.py;
        doCrop = true;
      }
      
      if (tileY == cropToTileBounds.s)
      {
        // Processing the bottom-most column of tiles.
        maxCopyY = (int)bottomLeftCropPixel.py;
        doCrop = true;
      }
      
      // Now that the pixel-based crop bounds are set up
      // if needed, we perform the crop if necessary.
      if (doCrop)
      {
        final Raster r = src.getData(destRect);
        crop(r, dest, destRect, minCopyX, minCopyY, maxCopyX, maxCopyY);
      }
      else
      {
        dest.setRect(destRect.x, destRect.y, src.getData());
      }
    }
  }

  @Override
  public Rectangle mapDestRect(Rectangle destRect, int sourceIndex)
  {
    return destRect;
  }

  @Override
  public Rectangle mapSourceRect(Rectangle sourceRect, int sourceIndex)
  {
    return sourceRect;
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName();
  }

  @Override
  public void setTileInfo(long tx, long ty, int zoom, int ts)
  {
    if (zoom != zoomlevel ||  ts != tileSize)
    {
      zoomlevel = zoom;
      tileSize = ts;
      
      calculateCrop();
    }
    
    tileX = tx;
    tileY = ty;
  }
}
