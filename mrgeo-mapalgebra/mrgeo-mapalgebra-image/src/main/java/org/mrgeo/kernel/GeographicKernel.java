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
 * Interface for a kernel in geographic space. Obviously the concept of a kernel
 * is a little different. The kernel for each row of data will be different due
 * to each row containing pixels of slightly different size (pixels get smaller
 * in width as you get close to the poles). Because of this, a kernel must be
 * generated for each latitude.
 * 
 * @author jason.surratt
 * 
 */
public interface GeographicKernel
{
  float[] createKernel(double latitude, double pixelWidth, double pixelHeight);
  float[] createMaxSizeKernel(int zoom, int tileSize);

int getWidth();
int getHeight();

}
