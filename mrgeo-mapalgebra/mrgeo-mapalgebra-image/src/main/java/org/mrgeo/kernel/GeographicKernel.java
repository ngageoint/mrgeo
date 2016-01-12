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

/**
 * Interface for a kernel in geographic space. The assumption is that pixels
 * are square, so the size of every pixel in geographic space is the same.
 */
public interface GeographicKernel
{
  /**
   * Create the kernel itself. The return value is an array of values
   * representing a matrix of pixel weights. The array values appear
   * in left to right, top to bottom order.
   *
   * @param pixelWidthMeters
   * @param pixelHeightMeters
   * @return
   */
  float[] createKernel(double pixelWidthMeters, double pixelHeightMeters);

  /**
   * Return the width of the kernel in pixels. The createKernel method
   * must be called first.
   */
  int getWidth();

  /**
   * Return the height of the kernel in pixels. The createKernel method
   * must be called first.
   */
  int getHeight();
}
