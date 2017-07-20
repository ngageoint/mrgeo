/*
 * Copyright 2009-2017. DigitalGlobe, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.mrgeo.mapalgebra.vector.paint;

import org.mrgeo.geometry.Point;
import org.mrgeo.utils.FloatUtils;
import org.mrgeo.utils.Gaussian;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.*;
import java.awt.geom.AffineTransform;
import java.awt.geom.NoninvertibleTransformException;
import java.awt.geom.Point2D;
import java.awt.image.ColorModel;
import java.awt.image.Raster;
import java.awt.image.WritableRaster;

public class GaussianComposite extends WeightedComposite
{
private static Logger log = LoggerFactory.getLogger(GaussianComposite.class);

private Point2D.Double center = new Point2D.Double();
private double major;
private double minor;

private AffineTransform rotate;
private AffineTransform translate;

public GaussianComposite()
{
}

public GaussianComposite(double weight)
{
  super(weight);
}

public GaussianComposite(double weight, double nodata)
{
  super(weight, nodata);
}

public void setEllipse(Point center, double majorWidth, double minorWidth,
    double orientation, AffineTransform transform)
{
  major = majorWidth;
  minor = minorWidth;

  this.center = new Point2D.Double(center.getX(), center.getY());

  // translation from lat/lon to pixels
  translate = new AffineTransform(transform);

  // orientation of the ellipse
  rotate = AffineTransform.getRotateInstance(-orientation);
}

/*
 * (non-Javadoc)
 *
 * @see java.awt.Composite#createContext(java.awt.image.ColorModel,
 * java.awt.image.ColorModel, java.awt.RenderingHints)
 */
@Override
public CompositeContext createContext(ColorModel srcColorModel, ColorModel dstColorModel,
    RenderingHints hints)
{
  return new GaussianCompositeContext();
}

private class GaussianCompositeContext implements CompositeContext
{
  public GaussianCompositeContext()
  {
  }

  /*
   * (non-Javadoc)
   *
   * @see java.awt.CompositeContext#compose(java.awt.image.Raster,
   * java.awt.image.Raster, java.awt.image.WritableRaster)
   */
  @Override
  public void compose(Raster src, Raster dstIn, WritableRaster dstOut)
  {
    int minX = dstOut.getMinX();
    int minY = dstOut.getMinY();
    int maxX = minX + dstOut.getWidth();
    int maxY = minY + dstOut.getHeight();

    //System.out.println(minX + ", " + minY + " - " + maxX + ", " + maxY);
    // calculate the area of the ellipse
    double area = Math.PI * (major / 2.0) * (minor / 2.0);

    // the final pixel multiplier, to be combined with the gaussian
    double multiplier = weight / area;

    for (int y = minY; y < maxY; y++)
    {
      for (int x = minX; x < maxX; x++)
      {
        double d = dstIn.getSampleDouble(x, y, 0);
        double s = src.getSampleDouble(x, y, 0);

        double sample;

        if (isNodataNaN)
        {
          if (Double.isNaN(s))
          {
            sample = d;
          }
          else
          {
            // do the gaussian...
            sample = calculateGaussian(s, x - dstIn.getSampleModelTranslateX(),
                y - dstIn.getSampleModelTranslateY(), multiplier);
          }
        }
        else
        {
          if (FloatUtils.isEqual(s, nodata))
          {
            sample = d;
          }
          else
          {
            // do the gaussian...
            sample = calculateGaussian(s, x - dstIn.getSampleModelTranslateX(),
                y - dstIn.getSampleModelTranslateY(), multiplier);
          }
        }

        dstOut.setSample(x, y, 0, sample);
      }
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see java.awt.CompositeContext#dispose()
   */
  @Override
  public void dispose()
  {

  }

  private double calculateGaussian(double v, int x, int y, double multiplier)
  {
    Point2D.Double src = new Point2D.Double(x, y);

    // transform in the src point from pixels to lat/lon
    try
    {
      translate.inverseTransform(src, src);
    }
    catch (NoninvertibleTransformException e)
    {
      log.error("Exception thrown", e);
    }

    // calculate the lat/lon delta of the src point
    Point2D.Double delta = new Point2D.Double((src.getX() - center.getX()),
        (src.getY() - center.getY()));

    // transform the delta to the orientation of the ellipse
    rotate.transform(delta, delta);


    double gaussian = Gaussian.phi(delta.getX(), major) *
        Gaussian.phi(delta.getY(), minor);

    return v * (gaussian * multiplier);
  }
}
}
