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

package org.mrgeo.image;

import org.mrgeo.utils.LongRectangle;
import org.mrgeo.utils.tms.Bounds;
import org.mrgeo.utils.tms.Pixel;
import org.mrgeo.utils.tms.TMSUtils;
import org.mrgeo.utils.tms.TileBounds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class BoundsCropper
{

@SuppressWarnings("unused")
private static final Logger log = LoggerFactory.getLogger(BoundsCropper.class);

/**
 * BoundsCropper takes an image (actually, its metadata), a list of user bounds to crop, and
 * a zoom level, and gives back the metadata corresponding to the cropped image with the
 * maximum zoom level set to the one provided.
 * <p>
 * Additional details:
 * <p>
 * If there is only one user bounds, then the cropped image bounds corresponds to that singleton
 * bounds. If there is a list of user bounds, then the cropped image bounds corresponds to an
 * envelope around the list of user bounds.
 * <p>
 * The new image bounds are computed from the old using the following two transformations
 * - first, the user bounds are capped to the nearest tile boundaries.
 * - second, the bounds from above are then intersected with the original image boundaries
 * <p>
 * The new tile bounds are computed from the new image bounds.
 * <p>
 * If the user bounds are outside the image bounds (i.e., they have no overlap), then an
 * exception is thrown
 * <p>
 * If the provided zoom level is different from the maximum zoom level in the input metadata,
 * then care is taken to set the maximum zoom level of the returned data to the provided zoom
 * level.
 */
public static MrsPyramidMetadata getCroppedMetadata(MrsPyramidMetadata origMetadata,
    List<Bounds> userBounds, int zoomLevel)
{

  double minX = Double.POSITIVE_INFINITY, minY = Double.POSITIVE_INFINITY;
  double maxX = Double.NEGATIVE_INFINITY, maxY = Double.NEGATIVE_INFINITY;

  // if there is a list of user bounds, find the envelope surrounding it
  for (Bounds bounds : userBounds)
  {
    if (bounds.w < minX)
    {
      minX = bounds.w;
    }
    if (bounds.s < minY)
    {
      minY = bounds.s;
    }
    if (bounds.e > maxX)
    {
      maxX = bounds.e;
    }
    if (bounds.n > maxY)
    {
      maxY = bounds.n;
    }
  }

  Bounds cropBounds = new Bounds(minX, minY, maxX, maxY);

  // now cap it to the nearest outside tile boundaries
  Bounds cropTileBounds = TMSUtils.tileBounds(cropBounds, zoomLevel, origMetadata.getTilesize());

  // get the original image bounds
  Bounds imageBounds = origMetadata.getBounds();

  // complain if the user bounds has no overlap with
  if (!imageBounds.intersects(cropTileBounds))
  {
    throw new IllegalArgumentException(String.format("Oops - cropped bounds and image bounds "
        + "do not overlap: cropped bounds=%s, image bounds=%s", cropTileBounds, imageBounds));
  }

  // now find the intersection of the cropped bounds and image bounds - this becomes the new
  // image bounds
  Bounds newImageBounds = new Bounds(Math.max(cropTileBounds.w, imageBounds.w),
      Math.max(cropTileBounds.s, imageBounds.s),
      Math.min(cropTileBounds.e, imageBounds.e),
      Math.min(cropTileBounds.n, imageBounds.n));

  // find the tile bounds corresponding to the new image bounds - this becomes the new tile bounds
  TileBounds newTileBounds = TMSUtils.boundsToTile(newImageBounds,
      zoomLevel,
      origMetadata.getTilesize());

  final Pixel lowerPx = TMSUtils.latLonToPixels(newImageBounds.s, newImageBounds.w,
      zoomLevel, origMetadata.getTilesize());
  final Pixel upperPx = TMSUtils.latLonToPixels(newImageBounds.n, newImageBounds.e,
      zoomLevel, origMetadata.getTilesize());

  final LongRectangle pixelBounds = new LongRectangle(0, 0, upperPx.px - lowerPx.px,
      upperPx.py - lowerPx.py);

  // time to create the new metadata
  MrsPyramidMetadata croppedMetadata = new MrsPyramidMetadata();
  croppedMetadata.setPyramid(origMetadata.getPyramid());
  croppedMetadata.setBounds(newImageBounds);
  croppedMetadata.setTilesize(origMetadata.getTilesize());
  croppedMetadata.setTileType(origMetadata.getTileType());
  croppedMetadata.setBands(1);
  croppedMetadata.setDefaultValues(origMetadata.getDefaultValues());
  // max zoom should be input zoom not origMetadata.getMaxZoomLevel
  croppedMetadata.setMaxZoomLevel(zoomLevel);
  croppedMetadata.setName(zoomLevel, "" + zoomLevel);
  croppedMetadata.setTileBounds(zoomLevel, TileBounds.convertToLongRectangle(newTileBounds));
  croppedMetadata.setPixelBounds(zoomLevel, pixelBounds);
  croppedMetadata.setProtectionLevel(origMetadata.getProtectionLevel());
  return croppedMetadata;
}
}
