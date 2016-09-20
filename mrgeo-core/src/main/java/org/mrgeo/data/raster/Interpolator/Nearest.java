package org.mrgeo.data.raster.Interpolator;

import org.mrgeo.data.raster.MrGeoRaster;

public class Nearest
{
public static void scaleInt(final MrGeoRaster src, final MrGeoRaster dst)
{
  final float x_ratio = (float) src.width() / dst.width();
  final float y_ratio = (float) src.height() / dst.height();

  float srcX = 0;
  float srcY = 0;

  for (int b = 0; b < src.bands(); b++)
  {
    for (int y = 0; y < dst.height(); y++)
    {
      for (int x = 0; x < dst.width(); x++)
      {
        dst.setPixel(x, y, b, src.getPixelInt((int)srcX, (int)srcY, b));
        srcX += x_ratio;
      }
      srcY += y_ratio;
    }
  }
}

public static void scaleFloat(final MrGeoRaster src, final MrGeoRaster dst)
{
  final float x_ratio = (float) src.width() / dst.width();
  final float y_ratio = (float) src.height() / dst.height();

  float srcX = 0;
  float srcY = 0;

  for (int b = 0; b < src.bands(); b++)
  {
    for (int y = 0; y < dst.height(); y++)
    {
      for (int x = 0; x < dst.width(); x++)
      {
        dst.setPixel(x, y, b, src.getPixelFloat((int)srcX, (int)srcY, b));
        srcX += x_ratio;
      }
      srcY += y_ratio;
    }
  }
}

public static void scaleDouble(final MrGeoRaster src, final MrGeoRaster dst)
{
  final float x_ratio = (float) src.width() / dst.width();
  final float y_ratio = (float) src.height() / dst.height();

  float srcX = 0;
  float srcY = 0;

  for (int b = 0; b < src.bands(); b++)
  {
    for (int y = 0; y < dst.height(); y++)
    {
      for (int x = 0; x < dst.width(); x++)
      {
        dst.setPixel(x, y, b, src.getPixelDouble((int)srcX, (int)srcY, b));
        srcX += x_ratio;
      }
      srcY += y_ratio;
    }
  }

}



}
