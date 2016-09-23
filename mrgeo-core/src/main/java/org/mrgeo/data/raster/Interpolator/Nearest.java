package org.mrgeo.data.raster.Interpolator;

import org.mrgeo.data.raster.MrGeoRaster;

public class Nearest
{
public static double EPSILON = 1e-6f;

public static void scaleInt(final MrGeoRaster src, final MrGeoRaster dst)
{
  final double x_ratio = (double) src.width() / dst.width();
  final double y_ratio = (double) src.height() / dst.height();

  double startX = 0.0;
  double startY = 0.0;
  if (x_ratio >= 2.0)
  {
    startX = x_ratio / 2;
  }
  if (y_ratio >= 2.0)
  {
    startY = y_ratio / 2;
  }

  double srcX;
  double srcY = startY;

  for (int b = 0; b < src.bands(); b++)
  {
    for (int y = 0; y < dst.height(); y++)
    {
      srcX = startX;
      for (int x = 0; x < dst.width(); x++)
      {
        dst.setPixel(x, y, b, src.getPixelInt((int)(srcX + EPSILON), (int)(srcY + EPSILON), b));
        srcX += x_ratio;
      }
      srcY += y_ratio;
    }
  }
}

public static void scaleFloat(final MrGeoRaster src, final MrGeoRaster dst)
{
  final double x_ratio = (double) src.width() / dst.width();
  final double y_ratio = (double) src.height() / dst.height();

  double srcX;
  double srcY = 0;

  for (int b = 0; b < src.bands(); b++)
  {
    for (int y = 0; y < dst.height(); y++)
    {
      srcX = 0;
      for (int x = 0; x < dst.width(); x++)
      {

        dst.setPixel(x, y, b, src.getPixelFloat((int)(srcX + EPSILON), (int)(srcY + EPSILON), b));
        srcX += x_ratio;
      }
      srcY += y_ratio;
    }
  }
}

public static void scaleDouble(final MrGeoRaster src, final MrGeoRaster dst)
{
  final double x_ratio = (double) src.width() / dst.width();
  final double y_ratio = (double) src.height() / dst.height();

  double srcX;
  double srcY = 0;

  for (int b = 0; b < src.bands(); b++)
  {
    for (int y = 0; y < dst.height(); y++)
    {
      srcX = 0;
      for (int x = 0; x < dst.width(); x++)
      {
        dst.setPixel(x, y, b, src.getPixelDouble((int)(srcX + EPSILON), (int)(srcY + EPSILON), b));
        srcX += x_ratio;
      }
      srcY += y_ratio;
    }
  }

}



}
