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
import java.util.Hashtable;
import java.util.Vector;

/**
 * This method expects a 3 band rendered image as input, where the three bands
 * represent XYZ of the normal vector. I.e. as calculated by HornNormalOpImagev1.
 * It is assumed that the input values are normalized.
 */
@SuppressWarnings("unchecked")
public class SlopeFromNormalOpImage extends OpImage
{
  public final static int SLOPE = 2;
  public final static int PERCENT = 1;
  public final static int DEGREES = 0;

  RenderedImage src;
  // one of percent or degrees
  int units;
  private double noDataValue;
  private boolean isNoDataNan;

  public static SlopeFromNormalOpImage create(RenderedImage src, int units, RenderingHints hints)
  {
    @SuppressWarnings("rawtypes")
    Vector sources = new Vector();
    sources.add(src);

    return new SlopeFromNormalOpImage(sources, units, hints);
  }

  @SuppressWarnings("rawtypes")
  private SlopeFromNormalOpImage(Vector sources, int units, RenderingHints hints)
  {
    super(sources, null, null, false);
    if (hints == null)
    {
      hints = (RenderingHints) JAI.getDefaultInstance().getRenderingHints().clone();
    }

    src = (RenderedImage) sources.get(0);

    this.units = units;
    noDataValue = OpImageUtils.getNoData(src, Double.NaN);
    isNoDataNan = Double.isNaN(noDataValue);

    colorModel = new FloatDoubleColorModel(ColorSpace.getInstance(ColorSpace.CS_GRAY), false,
        false, Transparency.OPAQUE, DataBuffer.TYPE_DOUBLE);
    sampleModel = colorModel.createCompatibleSampleModel(src.getSampleModel().getWidth(), src
        .getSampleModel().getHeight());
  }

  // TODO Break this into multiple functions
  @Override
  final protected void computeRect(PlanarImage[] sources, WritableRaster dest, Rectangle destRect)
  {
    PlanarImage source = sources[0];

    Raster normals = source.getData(destRect);
    
    Vector3d v = new Vector3d();
    Vector3d up = new Vector3d(0, 0, 1.0);
    for (int y = destRect.y; y < destRect.y + destRect.height; y++)
    {
      for (int x = destRect.x; x < destRect.x + destRect.width; x++)
      {
        v.x = normals.getSampleDouble(x, y, 0);
        double r;
        if (OpImageUtils.isNoData(v.x, noDataValue, isNoDataNan))
        {
          r = noDataValue;
        }
        else
        {
          v.y = normals.getSampleDouble(x, y, 1);
          v.z = normals.getSampleDouble(x, y, 2);
          // angle in radians
          double theta = Math.acos(up.dot(v));
          if (units == DEGREES)
          {
            r = theta * 180.0 / Math.PI;
          }
          else if (units == PERCENT)
          {
            r = Math.tan(theta) * 100.0;
          }
          else
          {
            r = Math.tan(theta);
          }
        }
        dest.setSample(x, y, 0, r);
      }
    }
  }

  @Override
  @SuppressWarnings("rawtypes")
  protected Hashtable getProperties()
  {
    Hashtable result = super.getProperties();
    if (result == null)
    {
      result = new Hashtable();
    }
    result.put(OpImageUtils.NODATA_PROPERTY, new Double(noDataValue));
    return result;
  }

  @Override
  public Object getProperty(String name)
  {
    return getProperties().get(name);
  }

  @Override
  public String[] getPropertyNames()
  {
    Vector<String> result = new Vector<String>();
    for (Object k : getProperties().keySet())
    {
      String key = (String) k;
      result.add(key);
    }
    return result.toArray(new String[0]);
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
}
