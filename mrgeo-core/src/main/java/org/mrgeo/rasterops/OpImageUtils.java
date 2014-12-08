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
package org.mrgeo.rasterops;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.media.jai.OpImage;
import javax.media.jai.PlanarImage;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.WritableRaster;

public class OpImageUtils
{
  public static final String NODATA_PROPERTY = "mrgeo.NoData";
  private static final double EPSILON = 1e-8;

  @SuppressWarnings("unused")
  private static final Logger log = LoggerFactory.getLogger(OpImageUtils.class);
  
//  public static ImageStats calculateStats(Raster r)
//  {
//    Raster tmp = r.createTranslatedChild(0, 0);
//    ImageStats result = new MrsPyramidv1.Stats();
//    double min = Double.MAX_VALUE;
//    double max = -Double.MAX_VALUE;
//    
//    int validePixelCount = 0;
//    int sum = 0; 
//    for (int y = 0; y < tmp.getHeight(); y++)
//    {
//      for (int x = 0; x < tmp.getWidth(); x++)
//      {
//        double v = tmp.getSampleDouble(x, y, 0);
//        if (Double.isNaN(v) == false)
//        {
//          min = Math.min(min, v);
//          max = Math.max(max, v);
//          sum += v;
//          validePixelCount++;
//        }
//      }
//    }
//
//    result.min = min;
//    result.max = max;
//    if (validePixelCount > 0)
//    {
//      result.mean = sum / validePixelCount;
//    }
//    
//    return result;
//  }

  public static void setNoData(PlanarImage image, double noDataValue)
  {
    image.setProperty(OpImageUtils.NODATA_PROPERTY, noDataValue);
  }

  public static double getNoData(RenderedImage image, double defaultNoData)
  {
    double noDataValue = defaultNoData;
    Object noDataProperty = OpImageUtils.getRecursiveProperty(image, OpImageUtils.NODATA_PROPERTY);
    if (noDataProperty != null && (noDataProperty instanceof Number))
    {
      noDataValue = ((Number)noDataProperty).doubleValue();
    }
    return noDataValue;
  }

  /**
   * Performs a breadth first search through all sources for a given property.
   * The first property found is returned.
   * 
   * @param image
   * @param property
   * @return
   */
  public static Object getRecursiveProperty(RenderedImage image, String property)
  {
    Object obj = image.getProperty(property);
    if (obj != null && obj.getClass().equals(Object.class) == true)
    {
      obj = null;
    }

    if (obj == null && image.getSources() != null)
    {
      for (Object src : image.getSources())
      {
        try
        {
          if (src instanceof RenderedImage)
          {
            RenderedImage pi = (RenderedImage) src;
            obj = getRecursiveProperty(pi, property);
            if (obj != null && obj.getClass().equals(Object.class) == false)
            {
              return obj;
            }
          }
        }
        catch (ClassCastException e)
        {
          // no worries, move on.
        }
      }
    }

    return obj;
  }


  // The isNoDataNan is an optimization that
  // allows the caller to invoke Double.isNan(noDataValue) once, and pass it
  // into this method so that Double.isNan() does not have to be executed
  // multiple times when this method is called for every pixel of the raster.
  public static boolean isNoData(double value, double noDataValue, boolean isNoDataNan)
  {
    if (isNoDataNan)
    {
      return Double.isNaN(value);
    }
    return ((value >= noDataValue - EPSILON) && (value <= noDataValue + EPSILON));
  }

  public static void copyWithoutNulls(Raster r, WritableRaster wr)
  {
    assert (r.getWidth() == wr.getWidth());
    assert (r.getHeight() == wr.getHeight());
    assert (r.getMinX() == wr.getMinX());
    assert (r.getMinY() == wr.getMinY());
    assert (r.getNumBands() == wr.getNumBands() || r.getNumBands() == 3 && wr.getNumBands() == 4);
    
    int wrBands = wr.getNumBands();
    double[] v = new double[r.getNumBands()];
    for (int rx = r.getMinX(); rx < r.getMinX() + r.getWidth(); rx++)
    {
      for (int ry = r.getMinY(); ry < r.getMinY() + r.getHeight(); ry++)
      {
        boolean nonNull = false;
        boolean allZero = true;
        r.getPixel(rx, ry, v);
        for (int band = 0; band < v.length; band++)
        {
          if (Double.isNaN(v[band]) == false)
          {
            wr.setSample(rx, ry, band, v[band]);
            nonNull = true;
            if (v[band] != 0.0)
            {
              allZero = false;
            }
          }
        }
        if (nonNull == true && allZero == false && v.length == 3 && wrBands == 4)
        {
          wr.setSample(rx, ry, 3, 255);
        }
      }
    }
  }
}
