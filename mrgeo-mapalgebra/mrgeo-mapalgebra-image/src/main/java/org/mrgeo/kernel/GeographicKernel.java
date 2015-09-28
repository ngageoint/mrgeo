/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
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
