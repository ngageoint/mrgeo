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
public float[] createKernel(double pixelWidthMeters, double pixelHeightMeters)
{
  kernelHeight = (int) (Math.ceil(kernelSize / pixelHeightMeters)) * 2 + 1;
  kernelWidth = (int) (Math.ceil(kernelSize / pixelWidthMeters)) * 2 + 1;
  // Make sure the kernel doesn't extend beyond the requested kernelSize by a pixel
  // around the outside ring of the kernel.
  if (kernelHeight * pixelHeightMeters > kernelSize)
  {
    kernelHeight -= 2;
    kernelWidth -= 2;
  }

  kernelHeight = Math.max(1, kernelHeight);
  kernelWidth = Math.max(1, kernelWidth);

  float[] data = new float[kernelWidth * kernelHeight];
  int i = 0;
  float sum = 0.0f;
  for (int py = -kernelHeight / 2; py <= kernelHeight / 2; py++)
  {
    for (int px = -kernelWidth / 2; px <= kernelWidth / 2; px++)
    {
      double vertDistance = Math.abs(py) * pixelHeightMeters;
      double horizDistance = Math.abs(px) * pixelWidthMeters;
      double distance = Math.sqrt(vertDistance * vertDistance + horizDistance * horizDistance);
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
