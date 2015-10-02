package org.mrgeo.paint;

import org.mrgeo.geometry.Point;

import java.awt.*;
import java.awt.image.ColorModel;
import java.awt.image.Raster;
import java.awt.image.WritableRaster;

public class GaussianComposite extends WeightedComposite
{

private Point center;
private double major;
private double minor;
private double orientation;
private double pixelWidth;
private double pixelHeight;

public GaussianComposite()
{
  super();
}

public GaussianComposite(double weight)
{
  super(weight);
}

public GaussianComposite(double weight, double nodata)
{
  super(weight, nodata);
}

public void setEllipse(Point center, double majorWidth, double minorWidth, double orientation, double pixelWidth, double pixelHeight)
{
  this.center = center;
  this.major = majorWidth;
  this.minor = minorWidth;
  this.orientation = orientation;
  this.pixelWidth = pixelWidth;
  this.pixelHeight = pixelHeight;
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

    // calculate the area of the ellipse
    double area = Math.PI * (major / 2.0) * (minor / 2.0) / 1000000;

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
          if (Double.isNaN(d))
          {
            sample = calculateGaussian(s, multiplier);
          }
          else if (Double.isNaN(s))
          {
            sample = d;
          }
          else
          {
            // do the gaussian...
            sample = calculateGaussian(s, multiplier);
          }
        }
        else
        {
          if (d == nodata)
          {
            sample = calculateGaussian(s, multiplier);
          }
          else if (s == nodata)
          {
            sample = d;
          }
          else
          {
            // do the gaussian...
            sample = calculateGaussian(s, multiplier);
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

  private double calculateGaussian(double v, double multiplier)
  {
    return v * multiplier;
  }
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
}
