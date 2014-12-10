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

import javax.media.jai.FloatDoubleColorModel;
import javax.media.jai.PlanarImage;
import javax.vecmath.Vector3d;
import java.awt.*;
import java.awt.color.ColorSpace;
import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.WritableRaster;
import java.io.IOException;
import java.util.Vector;

@SuppressWarnings("unchecked")
public final class DiffuseReflectionShadingOpImage extends MrGeoOpImage
{
  Vector3d lightSource;

  /**
   * 
   * @param coloredImage
   *          Background image to be shaded.
   * @param elevation
   *          Elevation data to use for shading, units must be meters.
   * @param lightSource
   *          vector of the light source (x, y, z) where z is vertical, doesn't
   *          need to be normalized (e.g. { 1, 1, -1 })
   * @return
   * @throws IOException
   */
  public static DiffuseReflectionShadingOpImage create(RenderedImage elevation,
    Vector3d lightSource, RenderingHints hints) throws IOException
    {
    @SuppressWarnings("rawtypes")
    Vector sources = new Vector();
    sources.add(elevation);

    return new DiffuseReflectionShadingOpImage(sources, lightSource, hints);
    }

  @SuppressWarnings("rawtypes")
  private DiffuseReflectionShadingOpImage(Vector sources, Vector3d lightSource, RenderingHints hints)
  {
    super(sources, hints);

    this.lightSource = (Vector3d) lightSource.clone();
    this.lightSource.normalize();

    RenderedImage src = (RenderedImage)sources.get(0);

    colorModel = new FloatDoubleColorModel(ColorSpace.getInstance(ColorSpace.CS_GRAY), false,
      false, Transparency.OPAQUE, DataBuffer.TYPE_FLOAT);
    sampleModel = colorModel.createCompatibleSampleModel(src.getSampleModel().getWidth(), 
      src.getSampleModel().getHeight());
  }

  // TODO Break this into multiple functions
  @Override
  final protected void computeRect(PlanarImage[] sources, WritableRaster dest, Rectangle destRect)
  {
    PlanarImage normals = sources[0];

    // if it is not a valid region, set all the pixels to null
    double nv = getNull();

    Raster r = normals.getData(destRect);

    Vector3d normal = new Vector3d();
    double[] v = new double[3];
    double intensity;

    for (int y = destRect.y; y < destRect.y + destRect.height; y++)
    {
      for (int x = destRect.x; x < destRect.x + destRect.width; x++)
      {
        r.getPixel(x, y, v);
        // if the pixel or its surrounding pixels are invalid, set the result to
        // transparent.
        if (isNull(v[0]) || isNull(v[1]) || isNull(v[2]))
        {
          intensity = nv;
        }
        else
        {
          normal.set(v);
          // this is equivalent to Diffuse Reflection shading model (cos(alpha))
          intensity = Math.abs(normal.dot(lightSource));
        }
        dest.setSample(x, y, 0, intensity);
      }
    }

  }
  @Override
  public String toString()
  {
    return getClass().getSimpleName();
  }

}
