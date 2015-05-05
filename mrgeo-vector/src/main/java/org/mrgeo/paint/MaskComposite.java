package org.mrgeo.paint;

import java.awt.*;
import java.awt.image.ColorModel;
import java.awt.image.Raster;
import java.awt.image.WritableRaster;

public class MaskComposite implements Composite
{
  private static final double EPSILON = 1e-8;

  private double srcMaskedValue;
  private double maskedValue;
  private double unmaskedValue;

  public class MaskCompositeContext implements CompositeContext
  {
    public MaskCompositeContext()
    {
    }

    @Override
    public void dispose()
    {
    }

    @Override
    public void compose(Raster src, Raster dstIn, WritableRaster dstOut)
    {
      int minX = dstOut.getMinX();
      int minY = dstOut.getMinY();
      int maxX = minX + dstOut.getWidth();
      int maxY = minY + dstOut.getHeight();

      for (int y = minY; y < maxY; y++)
      {
        for (int x = minX; x < maxX; x++)
        {
          double srcValue = src.getSampleDouble(x, y, 0);
          // If the source value is set to the srcMaskedValue, then write out
          // the maskedValue. Otherwise, write out the unmaskedValue.
          if (((srcMaskedValue - EPSILON) <= srcValue) && (srcValue <= (srcMaskedValue + EPSILON)))
          {
//            dstOut.setSample(x, y, 0, maskedValue);
            dstOut.setSample(x, y, 0, maskedValue);
          }
          else
          {
            dstOut.setSample(x, y, 0, unmaskedValue);
          }
        }
      }
    }
  }

  public MaskComposite(double srcMaskedValue, double maskedValue, double unmaskedValue)
  {
    this.srcMaskedValue = srcMaskedValue;
    this.maskedValue = maskedValue;
    this.unmaskedValue = unmaskedValue;
  }

  @Override
  public CompositeContext createContext(ColorModel srcColorModel, ColorModel dstColorModel, RenderingHints hints)
  {
    return new MaskCompositeContext();
  }
}
