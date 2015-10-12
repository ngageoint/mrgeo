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

package org.mrgeo.kernel;

import org.mrgeo.utils.Gaussian;
import org.mrgeo.utils.LatLng;
import org.mrgeo.utils.TMSUtils;

/**
 * @author jason.surratt
 *
 */
public class GaussianGeographicKernel implements GeographicKernel
{
private static final double MAX_LATITUDE = 60.0;


private double kernelSize;
private double sigma;

private int kernelWidth = -1;
private int kernelHeight = -1;

/**
 * Edge effects near the poles are undefined. Don't do that.
 *
 * @param sigma
 *          Standard deviation of the Gaussian kernel in meters
 */
public GaussianGeographicKernel(double sigma)
{
  this.sigma = sigma;
  kernelSize = sigma * 3;
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
      // doing this avoids a squared off kernel effect. (makes it prettier when using
      // transparency.
      if (distance > kernelSize)
      {
        v = 0;
      }
      else
      {
        v = (float) Gaussian.phi(distance, sigma);
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
 * creating the kernel in meters. Defaults to 3 * sigma. Anything larger than
 * 3 is probably overkill and less than 2 will show artifacts. Compute time
 * will increase roughly at O(size ^ 2).
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
