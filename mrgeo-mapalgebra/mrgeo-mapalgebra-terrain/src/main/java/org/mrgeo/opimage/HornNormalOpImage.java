/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */

package org.mrgeo.opimage;

import org.mrgeo.rasterops.OpImageUtils;
import org.mrgeo.utils.LatLng;
import org.mrgeo.utils.TMSUtils;

import javax.media.jai.FloatDoubleColorModel;
import javax.media.jai.JAI;
import javax.media.jai.OpImage;
import javax.media.jai.PlanarImage;
import javax.vecmath.Vector3d;
import java.awt.*;
import java.awt.color.ColorSpace;
import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.WritableRaster;
import java.util.Vector;

/**
 * Calculates the normal to a given pixel. This normal is calculated using
 * Horn's formula [1]. The normal is then reported as a two dimensional layer.
 * The first layer is the vertical component in the x dimension and the second
 * layer is the vertical component in the y dimension.
 * 
 * For performance reasons, this class is not re-entrant. It could easily be
 * made re-entrant.
 * 
 * 1. Horn, B. K. P. (1981). Hill Shading and the Reflectance Map, Proceedings
 * of the IEEE, 69(1):14-47.
 * http://scholar.google.com/scholar?cluster=13504326307708658108&hl=en
 * 
 */
@SuppressWarnings("unchecked")
public class HornNormalOpImage extends OpImage implements TileLocator
{
  private RenderedImage src;
  private double noDataValue;
  private boolean isNoDataNan;
  private float outputNoData = Float.NaN;
  private long tileX;
  private long tileY;
  private int zoom;
  private TMSUtils.Bounds bounds;
  private double resolution;
  private int startX;
  private int startY;

  private int lastY = -Integer.MAX_VALUE;

  static private final int[] dpx = { -1, 0, 1, -1, 0, 1, -1,  0,  1 };
  static private final int[] dpy = {  1, 1, 1,  0, 0, 0, -1, -1, -1 };

  private double dx;
  private double dy;

  @SuppressWarnings("unused")
  static private final int np = 0, zp = 1, pp = 2, nz = 3, zz = 4, pz = 5, nn = 6, zn = 7, pn = 8;

  private final double[] z = new double[9];
  private Vector3d vx = new Vector3d();
  private Vector3d vy = new Vector3d();
  private Vector3d normal = new Vector3d();

  public static HornNormalOpImage create(RenderedImage src, RenderingHints hints)
  {
    @SuppressWarnings("rawtypes")
    Vector sources = new Vector();
    sources.add(src);

    return new HornNormalOpImage(sources, hints);
  }

  @SuppressWarnings("rawtypes")
  private HornNormalOpImage(Vector sources, RenderingHints hints)
  {
    super(sources, null, null, false);

    if (hints == null)
    {
      hints = (RenderingHints) JAI.getDefaultInstance().getRenderingHints().clone();
    }

    src = (RenderedImage) sources.get(0);
    noDataValue = OpImageUtils.getNoData(src, Double.NaN);
    isNoDataNan = Double.isNaN(noDataValue);
    OpImageUtils.setNoData(this, outputNoData);

    colorModel = new FloatDoubleColorModel(ColorSpace.getInstance(ColorSpace.CS_CIEXYZ), false,
        false, Transparency.OPAQUE, DataBuffer.TYPE_FLOAT);
    sampleModel = colorModel.createCompatibleSampleModel(src.getSampleModel().getWidth(), src
        .getSampleModel().getHeight());
  }

  @Override
  final protected void computeRect(PlanarImage[] sources, WritableRaster dest, Rectangle destRect)
  {
    PlanarImage source = sources[0];
    Raster elevation = source.getData();
    float[] nml = new float[3];
    bounds = TMSUtils.tileBounds(tileX, tileY, zoom, getTileWidth());
    resolution = TMSUtils.resolution(zoom, getTileWidth());
    startX = destRect.x;
    startY = destRect.y;
    for (int y = destRect.y; y < destRect.y + destRect.height; y++)
    {
      for (int x = destRect.x; x < destRect.x + destRect.width; x++)
      {
        calculateNormal(elevation, x, y, nml);
        dest.setPixel(x, y, nml);
      }
    }
  }

  /**
   * Calculates the normal for a given pixel.
   * 
   * @param elevation
   * @param x
   * @param y
   * @return
   */
  final private void calculateNormal(Raster elevation, int x, int y, float[] result)
  {
    double eo = elevation.getSampleDouble(x, y, 0);

    if (OpImageUtils.isNoData(eo, noDataValue, isNoDataNan))
    {
      result[0] = outputNoData;
      result[1] = outputNoData;
      result[2] = outputNoData;
    }
    else
    {
      if (lastY != y)
      {
        double latitude = bounds.n - (resolution * (y - startY));
        double longitude = bounds.w + (resolution * (x - startX));
        LatLng o = new LatLng(latitude, longitude);

        // there will be very slight changes in the distance to the upper left
        // pixel and lower left pixel, but the changes should be insignificant.
        latitude = bounds.n - (resolution * (y - startY));
        longitude = bounds.w + (resolution * (x + 1 - startX));
        LatLng n = new LatLng(latitude, longitude);
        dx = LatLng.calculateGreatCircleDistance(o, n);

        latitude = bounds.n - (resolution * (y + 1 - startY));
        longitude = bounds.w + (resolution * (x - startX));
        n.setY(latitude);
        n.setX(longitude);
        dy = LatLng.calculateGreatCircleDistance(o, n);

        lastY = y;
      }

      int elevWidth = elevation.getWidth();
      int elevHeight = elevation.getHeight();
      int elevMinX = elevation.getMinX();
      int elevMinY = elevation.getMinY();
      // if any of the surrounding eight neighbors is null, set their value to
      // be the same as the center pixel. This is less hokie than making pixels
      // with null neighbors null.
      for (int i = 0; i < 9; i++)
      {
        int xNeighbor = x + dpx[i];
        int yNeighbor = y + dpy[i];
        double v = 0.0;
        if ((xNeighbor < elevMinX) || (xNeighbor >= elevMinX + elevWidth) ||
            (yNeighbor < elevMinY) || (yNeighbor >= elevMinY + elevHeight))
        {
          v = eo;
        }
        else
        {
          v = elevation.getSampleDouble(xNeighbor, yNeighbor, 0);
          if (OpImageUtils.isNoData(v, noDataValue, isNoDataNan))
          {
            v = eo;
          }
        }
        z[i] = v;
      }

      vx.x = dx;
      vx.y = 0.0;
      vx.z = ((z[pp] + z[pz] * 2 + z[pn]) - (z[np] + z[nz] * 2 + z[nn])) / 8.0;

      vy.x = 0.0;
      vy.y = dy;
      vy.z = ((z[pp] + z[zp] * 2 + z[np]) - (z[pn] + z[zn] * 2 + z[nn])) / 8.0;
      
      normal.cross(vx, vy);
      normal.normalize();
      // we want the normal to always point up.
      normal.z = Math.abs(normal.z);

      result[0] = (float) normal.x;
      result[1] = (float) normal.y;
      result[2] = (float) normal.z;
    }
  }

  @Override
  public Rectangle mapSourceRect(Rectangle sourceRect, int sourceIndex)
  {
    return sourceRect;
  }

  @Override
  public Rectangle mapDestRect(Rectangle destRect, int sourceIndex)
  {
    // We need one additional pixel above, below and on either side of
    // source rectangle to compute the normal for the edge pixels.
    Rectangle newRect = new Rectangle(destRect);
    newRect.grow(1, 1);
    return newRect;
  }

  /**
   * This method is called when the OpImage chain is initialized to work on
   * a specific tile.
   * 
   * @param tx
   * @param ty
   * @param zoom
   * @param tileSize
   */
  @Override
  public void setTileInfo(long tx, long ty, int zoom, int tileSize)
  {
    this.tileX = tx;
    this.tileY = ty;
    this.zoom = zoom;
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName();
  }
}
