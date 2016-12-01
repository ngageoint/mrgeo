package org.mrgeo.utils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class FloatUtils
{

public static final double DOUBLE_EPSILON = 1.0;
public static final float FLOAT_EPSILON = 1.0f;

@SuppressFBWarnings(value = "URF_UNREAD_FIELD", justification = "external api")
static class Floater
{
  private final int bits;

  private final boolean inf;
  private final boolean neginf;
  private final boolean nan;

  private final boolean neg;
  private final int exp;
  private final int mant;

  public Floater(float f) {
    bits = Float.floatToRawIntBits(f);

    inf = bits == 0x7f800000;
    neginf = bits == 0xff800000;

    //nan = (bits >= 0x7f800001 && bits <= 0x7fffffff) || (bits >= 0xff800001 && bits <= 0xffffffff);
    nan = bits >= 0x7f800001 || (bits >= 0xff800001 && bits <= 0xffffffff);

    neg = (bits >> 31) != 0;
    exp = (bits >> 23) & 0xff;
    mant = (exp == 0) ? (bits & 0x7fffff) << 1 : (bits & 0x7fffff) | 0x800000;
  }
}

@SuppressFBWarnings(value = "URF_UNREAD_FIELD", justification = "external api")
static class Doubler
{
  private final long bits;

  private final boolean inf;
  private final boolean neginf;
  private final boolean nan;

  private final boolean neg;
  private final long exp;
  private final long mant;

  public Doubler(double d) {
    bits = Double.doubleToRawLongBits(d);

    inf = bits == 0x7ff0000000000000L;
    neginf = bits == 0xfff0000000000000L;

//    nan = (bits >= 0x7ff0000000000001L && bits <= 0x7fffffffffffffffL) ||
//        (bits >= 0xfff0000000000001L && bits <= 0xffffffffffffffffL);
    nan = bits >= 0x7ff0000000000001L ||
        (bits >= 0xfff0000000000001L && bits <= 0xffffffffffffffffL);


    neg = (bits >> 63) != 0;
    exp = (bits >> 52) & 0x7ffL;
    mant = (exp == 0) ? (bits & 0x000fffffffffffffL) << 1 :
        (bits & 0x000fffffffffffffL) | 0x10000000000000L;
  }

  long bits() { return bits; }
  boolean negative() { return neg; }
  long exp() { return exp; }
  long mantissa() { return mant; }
}

public static boolean isEqual(float a, float b, int bitsprecision)
{
  if (bitsprecision >= 32 || bitsprecision < 0) {
    if (bitsprecision == 32) {
      return true;
    }
    return false;
  }

  Floater fa = new Floater(a);
  Floater fb = new Floater(b);

  // different signs mean they don't match
  if (fa.neg != fb.neg)
  {
    // unless they are zero! (0.0 and -0.0)
    return (fa.exp | fb.exp | fa.mant | fb.mant) == 0;
  }
  if (bitsprecision == 0)
  {
    return fa.bits == fb.bits;
  }

  int d = fa.bits - fb.bits;
  int bp = 1 << (bitsprecision - 1);

  return d <= bp && d >= -bp;
}

public static boolean isEqual(double a, double b, int bitsprecision)
{
  if (bitsprecision >= 64 || bitsprecision < 0) {
    if (bitsprecision == 64) {
      return true;
    }
    return false;
  }

  Doubler da = new Doubler(a);
  Doubler db = new Doubler(b);

  // different signs mean they don't match
  if (da.neg != db.neg)
  {
    // unless they are zero! (0.0 and -0.0)
    return (da.exp | db.exp | da.mant | db.mant) == 0;
  }
  if (bitsprecision == 0)
  {
    return da.bits == db.bits;
  }

  long d = da.bits - db.bits;
  long bp = 1 << (bitsprecision - 1);

  return d <= bp && d >= -bp;
}

public static boolean isEqual(double a, double b)
{
  return isEqual(a, b, 1);
}

public static boolean isEqual(float a, float b)
{
  return isEqual(a, b, 1);
}

public static boolean isEqual(float a, float b, float epsilon)
{
  float d = a - b;
  return d <= epsilon && d >= -epsilon;
}


public static boolean isEqual(double a, double b, double epsilon)
{
  double d = a - b;
  return d <= epsilon && d >= -epsilon;
}


public static boolean isNodata(double v, double nodata)
{
  if (Double.isNaN(nodata))
  {
    return Double.isNaN(v);
  }

  return isEqual(v, nodata, 1);
}


public static boolean isNotNodata(double v, double nodata)
{
  if (Double.isNaN(nodata))
  {
    return !Double.isNaN(v);
  }

  return !isEqual(v, nodata, 1);
}


public static boolean isNodata(float v, float nodata)
{
  if (Float.isNaN(nodata))
  {
    return Float.isNaN(v);
  }

  return isEqual(v, nodata, 1);
}


public static boolean isNotNodata(float v, float nodata)
{
  if (Float.isNaN(nodata))
  {
    return !Float.isNaN(v);
  }

  return !isEqual(v, nodata, 1);
}

}
