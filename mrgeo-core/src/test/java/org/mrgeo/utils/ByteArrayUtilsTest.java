package org.mrgeo.utils;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.junit.UnitTest;

import java.util.Random;

public class ByteArrayUtilsTest
{
@Test
@Category(UnitTest.class)
public void testByte()
{
  byte[] data = new byte[1];
  byte dst;

  for (byte src = Byte.MIN_VALUE; src < Byte.MAX_VALUE; src++)
  {
    ByteArrayUtils.setByte(src, data, 0);
    dst = ByteArrayUtils.getByte(data, 0);

    Assert.assertEquals("Byte values are different", src, dst);

    ByteArrayUtils.setByte(src, data);
    dst = ByteArrayUtils.getByte(data);

    Assert.assertEquals("Byte values are different", src, dst);
  }
}

@Test
@Category(UnitTest.class)
public void testShort()
{
  byte[] data = new byte[2];
  short dst;

  for (short src = Short.MIN_VALUE; src < Short.MAX_VALUE; src++)
  {
    ByteArrayUtils.setShort(src, data, 0);
    dst = ByteArrayUtils.getShort(data, 0);

    Assert.assertEquals("Short values are different", src, dst);

    ByteArrayUtils.setShort(src, data);
    dst = ByteArrayUtils.getShort(data);

    Assert.assertEquals("Short values are different", src, dst);
  }
}

@Test
@Category(UnitTest.class)
public void testInt()
{
  byte[] data = new byte[4];
  int src;
  int dst;

  Random rand = new Random();

  for (int i = 0; i < 1000000; i++)
  {
    src = rand.nextInt();

    ByteArrayUtils.setInt(src, data, 0);
    dst = ByteArrayUtils.getInt(data, 0);

    Assert.assertEquals("Int values are different", src, dst);

    ByteArrayUtils.setInt(src, data);
    dst = ByteArrayUtils.getInt(data);

    Assert.assertEquals("Int values are different", src, dst);
  }
}


@Test
@Category(UnitTest.class)
public void testLong()
{
  byte[] data = new byte[8];
  long src;
  long dst;

  Random rand = new Random();

  for (int i = 0; i < 1000000; i++)
  {
    src = rand.nextLong();

    ByteArrayUtils.setLong(src, data, 0);
    dst = ByteArrayUtils.getLong(data, 0);

    Assert.assertEquals("Long values are different", src, dst);

    ByteArrayUtils.setLong(src, data);
    dst = ByteArrayUtils.getLong(data);

    Assert.assertEquals("Long values are different", src, dst);
  }


}

@Test
@Category(UnitTest.class)
public void testFloat()
{
  byte[] data = new byte[4];
  float src;
  float dst;

  Random rand = new Random();

  for (int i = 0; i < 1000000; i++)
  {
    src = rand.nextFloat();

    ByteArrayUtils.setFloat(src, data, 0);
    dst = ByteArrayUtils.getFloat(data, 0);

    Assert.assertEquals("Float values are different", src, dst, 1e-8);

    ByteArrayUtils.setFloat(src, data);
    dst = ByteArrayUtils.getFloat(data);

    Assert.assertEquals("Float values are different", src, dst, 1e-8);
  }

  src = Float.NaN;

  ByteArrayUtils.setFloat(src, data, 0);
  dst = ByteArrayUtils.getFloat(data, 0);

  Assert.assertEquals("Float values are different", src, dst, 1e-8);

  ByteArrayUtils.setFloat(src, data);
  dst = ByteArrayUtils.getFloat(data);

  Assert.assertEquals("Float values are different", src, dst, 1e-8);

  src = Float.POSITIVE_INFINITY;

  ByteArrayUtils.setFloat(src, data, 0);
  dst = ByteArrayUtils.getFloat(data, 0);

  Assert.assertEquals("Float values are different", src, dst, 1e-8);

  ByteArrayUtils.setFloat(src, data);
  dst = ByteArrayUtils.getFloat(data);

  Assert.assertEquals("Float values are different", src, dst, 1e-8);

  src = Float.NEGATIVE_INFINITY;

  ByteArrayUtils.setFloat(src, data, 0);
  dst = ByteArrayUtils.getFloat(data, 0);

  Assert.assertEquals("Float values are different", src, dst, 1e-8);

  ByteArrayUtils.setFloat(src, data);
  dst = ByteArrayUtils.getFloat(data);

  Assert.assertEquals("Float values are different", src, dst, 1e-8);

}

@Test
@Category(UnitTest.class)
public void testDouble()
{
  byte[] data = new byte[8];
  double src;
  double dst;

  Random rand = new Random();

  for (int i = 0; i < 1000000; i++)
  {
    src = rand.nextDouble();

    ByteArrayUtils.setDouble(src, data, 0);
    dst = ByteArrayUtils.getDouble(data, 0);

    Assert.assertEquals("Double values are different", src, dst, 1e-8);

    ByteArrayUtils.setDouble(src, data);
    dst = ByteArrayUtils.getDouble(data);

    Assert.assertEquals("Double values are different", src, dst, 1e-8);
  }

  src = Double.NaN;

  ByteArrayUtils.setDouble(src, data, 0);
  dst = ByteArrayUtils.getDouble(data, 0);

  Assert.assertEquals("Double values are different", src, dst, 1e-8);

  ByteArrayUtils.setDouble(src, data);
  dst = ByteArrayUtils.getDouble(data);

  Assert.assertEquals("Double values are different", src, dst, 1e-8);

  src = Double.POSITIVE_INFINITY;

  ByteArrayUtils.setDouble(src, data, 0);
  dst = ByteArrayUtils.getDouble(data, 0);

  Assert.assertEquals("Double values are different", src, dst, 1e-8);

  ByteArrayUtils.setDouble(src, data);
  dst = ByteArrayUtils.getDouble(data);

  Assert.assertEquals("Double values are different", src, dst, 1e-8);

  src = Double.NEGATIVE_INFINITY;

  ByteArrayUtils.setDouble(src, data, 0);
  dst = ByteArrayUtils.getDouble(data, 0);

  Assert.assertEquals("Double values are different", src, dst, 1e-8);

  ByteArrayUtils.setDouble(src, data);
  dst = ByteArrayUtils.getDouble(data);

  Assert.assertEquals("Double values are different", src, dst, 1e-8);

}

}
