/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */

package org.mrgeo.opimage.geographickernel;

import org.mrgeo.utils.LatLng;
import org.mrgeo.utils.TMSUtils;

/**
 * A Kernel for running a Laplacian filter. See
 * http://homepages.inf.ed.ac.uk/rbf/HIPR2/log.htm for a technical run down.
 */
public class LaplacianGeographicKernel implements GeographicKernel
{
private static final double MAX_LATITUDE = 60.0;

/**
 * Taken from the LoG defined here:
 * http://homepages.inf.ed.ac.uk/rbf/HIPR2/log.htm
 *
 */
public final double LoG(double d, double sigma)
{
  double s = (d * d) / (2 * sigma * sigma);
  double f2 = 1 - s;
  double f3 = Math.exp(-s);
  return f1 * f2 * f3;
}

private double kernelSize;

private double sigma;
private double f1;

private int kernelWidth = -1;
private int kernelHeight = -1;

/**
 * Edge effects near the poles are undefined. Don't do that.
 *
 * @param sigma
 *          Standard deviation of the Gaussian kernel in meters
 */
public LaplacianGeographicKernel(double sigma)
{
  this.sigma = sigma;
  kernelSize = sigma * 4;
  f1 = -1.0 / (Math.PI * Math.pow(this.sigma, 4.0));
}

@Override
public float[] createMaxSizeKernel(int zoom, int tileSize)
{
  double resolution = TMSUtils.resolution(zoom, tileSize);
  return createKernel(MAX_LATITUDE, resolution, resolution);
}

/*
 * (non-Javadoc)
 *
 * @see com.spadac.MrGis.RasterOps.GeographicKernel#createKernel(double)
 */
@Override
public float[] createKernel(double latitude, double pixelWidth, double pixelHeight)
{
  // pixel height in meters
  double pixelHeightM = LatLng.calculateGreatCircleDistance(new LatLng(latitude, 0),
      new LatLng(latitude + pixelHeight, 0));

  // kernel radius in pixels
  int halfKernelHeight = (int) (Math.ceil(kernelSize / pixelHeightM));
  // kernel height in pixels
  kernelHeight = halfKernelHeight * 2 + 1;

  // the smallest lat in terms of circumference of the earth
  double smallestLat = Math.abs(latitude) + halfKernelHeight * pixelHeight;
  // the minimum pixel width of the kernel in meters
  double minPixelWidthM = LatLng.calculateGreatCircleDistance(
      new LatLng(smallestLat, 0), new LatLng(smallestLat, pixelWidth));

  kernelWidth = (int) (Math.ceil(kernelSize / minPixelWidthM)) * 2 + 1;

  kernelWidth = Math.max(1, kernelWidth);
  kernelHeight = Math.max(1, kernelHeight);

  LatLng ll = new LatLng();
  LatLng origin = new LatLng(latitude, 0);
  float[] data = new float[kernelWidth * kernelHeight];
  int i = 0;
  float sum = 0.0f;
  for (int py = -kernelHeight / 2; py <= kernelHeight / 2; py++)
  {
    ll.setY(latitude + py * pixelHeight);
    for (int px = -kernelWidth / 2; px <= kernelWidth / 2; px++)
    {
      ll.setX(px * pixelWidth);
      double distance = LatLng.calculateGreatCircleDistance(origin, ll);
      float v;
      // doing this avoids a squared off kernel effect. (makes it prettier
      // when using
      // transparency.
      if (distance > kernelSize)
      {
        v = 0;
      }
      else
      {
        v = (float) LoG(distance, sigma);
      }
      data[i++] = v;
      sum += v;
    }
  }

  // normalize the kernel
  for (i = 0; i < data.length; i++)
  {
    data[i] = data[i] / sum;
  }

  return data;
}

/**
 * Sets the minimum distance out from the center the kernel will consider when
 * creating the kernel in meters. Defaults to 4 * sigma. Anything larger is
 * probably overkill and anything less will probably show artifacts. Compute
 * time will increase roughly at O(size ^ 2).
 *
 */
void setKernelSize(double size)
{
  kernelSize = size;
}

@Override
public int getWidth() { return kernelWidth; }

@Override
public int getHeight() { return kernelHeight; }

}
