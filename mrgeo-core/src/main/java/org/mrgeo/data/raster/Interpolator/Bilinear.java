package org.mrgeo.data.raster.Interpolator;

import org.mrgeo.data.raster.MrGeoRaster;
import org.mrgeo.utils.FloatUtils;

public class Bilinear
{
public static void scaleInt(final MrGeoRaster src, final MrGeoRaster dst, final double[] nodatas)
{
  final int srcW = src.width();
  final int srcH = src.height();

  int A, B, C, D;
  int r1, r2;

  int x, y, x2, y2;
  final float x_ratio = (float) srcW / dst.width();
  final float y_ratio = (float) srcH / dst.height();
  float x_diff, y_diff;


  for (int b = 0; b < src.bands(); b++)
  {
    final int nodata = (int) nodatas[b];

    for (int i = 0; i < dst.width(); i++)
    {
      for (int j = 0; j < dst.height(); j++)
      {
        x = (int) (x_ratio * j);
        y = (int) (y_ratio * i);

        x_diff = (x_ratio * j) - x;
        y_diff = (y_ratio * i) - y;

        // keep the indexes from going out of bounds.
        if (y < srcH - 1)
        {
          y2 = y + 1;
        }
        else
        {
          y2 = y;
        }

        if (x < srcW - 1)
        {
          x2 = x + 1;
        }
        else
        {
          x2 = x;
        }

        A = src.getPixelInt(x, y, b);
        B = src.getPixelInt(x2, y, b);
        C = src.getPixelInt(x, y2, b);
        D = src.getPixelInt(x2, y2, b);


        if (FloatUtils.isNodata(A, nodata))
        {
          r1 = B;
        }
        else if (FloatUtils.isNodata(B, nodata))
        {
          r1 = A;
        }
        else
        {
          r1 = (int) (A * (1 - x_diff) + B * (x_diff));
        }

        if (FloatUtils.isNodata(C, nodata))
        {
          r2 = D;
        }
        else if (FloatUtils.isNodata(D, nodata))
        {
          r2 = C;
        }
        else
        {
          r2 = (int) (C * (1 - x_diff) + D * (x_diff));
        }

        if (r1 == nodata)
        {
          dst.setPixel(i, j, b, r2);
        }
        else if (r2 == nodata)
        {
          dst.setPixel(i, j, b, r1);
        }
        else
        {
          dst.setPixel(i, j, b, r1 * (1 - y_diff) + r2 * y_diff);
        }
      }
    }
  }
}

public static void scaleFloat(final MrGeoRaster src, final MrGeoRaster dst, final double[] nodatas)
{
  final int srcW = src.width();
  final int srcH = src.height();

  float A, B, C, D;
  float r1, r2;

  int x, y, x2, y2;
  final float x_ratio = (float) srcW / dst.width();
  final float y_ratio = (float) srcH / dst.height();
  float x_diff, y_diff;


  for (int b = 0; b < src.bands(); b++)
  {
    float nodata = (float) nodatas[b];

    for (int i = 0; i < dst.width(); i++)
    {
      for (int j = 0; j < dst.height(); j++)
      {
        x = (int) (x_ratio * j);
        y = (int) (y_ratio * i);

        x_diff = (x_ratio * j) - x;
        y_diff = (y_ratio * i) - y;

        // keep the indexes from going out of bounds.
        if (y < srcH - 1)
        {
          y2 = y + 1;
        }
        else
        {
          y2 = y;
        }

        if (x < srcW - 1)
        {
          x2 = x + 1;
        }
        else
        {
          x2 = x;
        }

        A = src.getPixelFloat(x, y, b);
        B = src.getPixelFloat(x2, y, b);
        C = src.getPixelFloat(x, y2, b);
        D = src.getPixelFloat(x2, y2, b);

        if (Double.compare(A, nodata) == 0)
        {
          r1 = B;
        }
        else if (Double.compare(B, nodata) == 0)
        {
          r1 = A;
        }
        else
        {
          r1 = A * (1 - x_diff) + B * (x_diff);
        }

        if (Double.compare(C, nodata) == 0)
        {
          r2 = D;
        }
        else if (Double.compare(D, nodata) == 0)
        {
          r2 = C;
        }
        else
        {
          r2 = C * (1 - x_diff) + D * (x_diff);
        }

        if (FloatUtils.isNodata(r1, nodata))
        {
          dst.setPixel(i, j, b, r2);
        }
        else if (FloatUtils.isNodata(r2, nodata))
        {
          dst.setPixel(i, j, b, r1);
        }
        else
        {
          dst.setPixel(i, j, b, r1 * (1 - y_diff) + r2 * y_diff);
        }
      }
    }
  }
}

public static void scaleDouble(final MrGeoRaster src, final MrGeoRaster dst, final double[] nodatas)
{
  final int srcW = src.width();
  final int srcH = src.height();

  double A, B, C, D;
  double r1, r2;

  int x, y, x2, y2;
  final float x_ratio = (float) srcW / dst.width();
  final float y_ratio = (float) srcH / dst.height();
  float x_diff, y_diff;


  for (int b = 0; b < src.bands(); b++)
  {
    double nodata = nodatas[b];

    for (int i = 0; i < dst.width(); i++)
    {
      for (int j = 0; j < dst.height(); j++)
      {
        x = (int) (x_ratio * j);
        y = (int) (y_ratio * i);

        x_diff = (x_ratio * j) - x;
        y_diff = (y_ratio * i) - y;

        // keep the indexes from going out of bounds.
        if (y < srcH - 1)
        {
          y2 = y + 1;
        }
        else
        {
          y2 = y;
        }

        if (x < srcW - 1)
        {
          x2 = x + 1;
        }
        else
        {
          x2 = x;
        }

        A = src.getPixelDouble(x, y, b);
        B = src.getPixelDouble(x2, y, b);
        C = src.getPixelDouble(x, y2, b);
        D = src.getPixelDouble(x2, y2, b);

        if (Double.compare(A, nodata) == 0)
        {
          r1 = B;
        }
        else if (Double.compare(B, nodata) == 0)
        {
          r1 = A;
        }
        else
        {
          r1 = A * (1 - x_diff) + B * (x_diff);
        }

        if (FloatUtils.isNodata(C, nodata))
        {
          r2 = D;
        }
        else if (FloatUtils.isNodata(D, nodata))
        {
          r2 = C;
        }
        else
        {
          r2 = C * (1 - x_diff) + D * (x_diff);
        }

        if (FloatUtils.isNodata(r1, nodata))
        {
          dst.setPixel(i, j, b, r2);
        }
        else if (FloatUtils.isNodata(r2, nodata))
        {
          dst.setPixel(i, j, b, r1);
        }
        else
        {
          dst.setPixel(i, j, b, r1 * (1 - y_diff) + r2 * y_diff);
        }
      }
    }
  }
}
}
