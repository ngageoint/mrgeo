package org.mrgeo.utils;

import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.junit.UnitTest;

import java.util.Random;

@SuppressWarnings("all") // test code, not included in production
public class FloatUtilsTest
{
@Test
@Category(UnitTest.class)
public void testdouble()
{

  // 1 and 1.0000000000000002 (the smallest number > 1)  1 bit difference
  Assert.assertTrue("Values should be equal", FloatUtils.isEqual(1.0, 1.0000000000000002, 1));
  Assert.assertTrue("Values should be equal", FloatUtils.isEqual(1.0000000000000002, 1.0, 1));

  Assert.assertFalse("Values should not be equal", FloatUtils.isEqual(1.0, 1.0000000000000002, 0));
  Assert.assertFalse("Values should not be equal", FloatUtils.isEqual(1.0000000000000002, 1.0, 0));

  // 1 and 1.0000000000000004 (the second smallest number > 1)  2 bits difference
  Assert.assertFalse("Values should not be equal", FloatUtils.isEqual(1.0, 1.0000000000000004, 1));
  Assert.assertFalse("Values should not be equal", FloatUtils.isEqual(1.0000000000000004, 1.0, 1));

  // Double min and max values
  Assert.assertFalse("Values should not be equal", FloatUtils.isEqual(Double.MIN_VALUE, Double.MAX_VALUE, 1));
  Assert.assertTrue("Values should be equal", FloatUtils.isEqual(Double.MIN_VALUE, Double.MAX_VALUE, 64));

  // 0.0 and -0.0
  Assert.assertTrue("Values should be equal", FloatUtils.isEqual(0.0, -0.0, 1));
  Assert.assertTrue("Values should be equal", FloatUtils.isEqual(-0.0, 0.0, 1));

  // 0.0 and -1.0
  Assert.assertFalse("Values should not be equal", FloatUtils.isEqual(0.0, -1.0, 1));
  Assert.assertFalse("Values should not be equal", FloatUtils.isEqual(-1.0, 0.0, 1));

  // 1.0 and -1.0
  Assert.assertFalse("Values should not be equal", FloatUtils.isEqual(1.0, -1.0, 1));
  Assert.assertFalse("Values should not be equal", FloatUtils.isEqual(-1.0, 1.0, 1));


}

@Test
@Category(UnitTest.class)
public void testfloat()
{
  // 1 and 1.0000001 (the smallest number > 1)  1 bit difference
  Assert.assertTrue("Values should be equal", FloatUtils.isEqual(1.0f, 1.0000001f, 1));
  Assert.assertTrue("Values should be equal", FloatUtils.isEqual(1.0000001f, 1.0f, 1));

  // 1 and 1.0000002 (the second smallest number > 1)  2 bits difference
  Assert.assertFalse("Values should not be equal", FloatUtils.isEqual(1.0f, 1.0000002f, 1));
  Assert.assertFalse("Values should not be equal", FloatUtils.isEqual(1.0000002f, 1.0f, 1));

  // 1 and 1.0000002 (the second smallest number > 1)  2 bits difference
  Assert.assertTrue("Values should be equal", FloatUtils.isEqual(1.0f, 1.0000002f, 2));
  Assert.assertTrue("Values should be equal", FloatUtils.isEqual(1.0000002f, 1.0f, 2));

  // Double min and max values
  Assert.assertFalse("Values should not be equal", FloatUtils.isEqual(Float.MIN_VALUE, Float.MAX_VALUE, 1));
  Assert.assertTrue("Values should be equal", FloatUtils.isEqual(Float.MIN_VALUE, Float.MAX_VALUE, 32));

  // 0.0 and -0.0
  Assert.assertTrue("Values should be equal", FloatUtils.isEqual(0.0f, -0.0f, 1));
  Assert.assertTrue("Values should be equal", FloatUtils.isEqual(-0.0f, 0.0f, 1));

  // 0.0 and -1.0
  Assert.assertFalse("Values should not be equal", FloatUtils.isEqual(0.0f, -1.0f, 1));
  Assert.assertFalse("Values should not be equal", FloatUtils.isEqual(-1.0f, 0.0f, 1));

  // 1.0 and -1.0
  Assert.assertFalse("Values should not be equal", FloatUtils.isEqual(1.0f, -1.0f, 1));
  Assert.assertFalse("Values should not be equal", FloatUtils.isEqual(-1.0f, 1.0f, 1));


}

}
