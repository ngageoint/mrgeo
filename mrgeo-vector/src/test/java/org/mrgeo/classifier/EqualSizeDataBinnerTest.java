package org.mrgeo.classifier;

import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.databinner.DataBinner;
import org.mrgeo.databinner.EqualSizeDataBinner;
import org.mrgeo.junit.UnitTest;

@SuppressWarnings("static-method")
public class EqualSizeDataBinnerTest
{
  @Test 
  @Category(UnitTest.class)
  public void test()
  {
    {
      DataBinner binner = new EqualSizeDataBinner(false, 10, 10.0, 210.0);
      Assert.assertEquals(0, binner.calculateBin(Double.NaN));
      Assert.assertEquals(0, binner.calculateBin(9.0));
      Assert.assertEquals(9, binner.calculateBin(215.0));
      Assert.assertEquals(0, binner.calculateBin(10.0));
      Assert.assertEquals(0, binner.calculateBin(29.9999));
      Assert.assertEquals(1, binner.calculateBin(30.0));
      Assert.assertEquals(1, binner.calculateBin(35.0));
      Assert.assertEquals(8, binner.calculateBin(189.9999));
      Assert.assertEquals(9, binner.calculateBin(190.0));
      Assert.assertEquals(9, binner.calculateBin(209.9999));
    }
    {
      DataBinner binner = new EqualSizeDataBinner(true, 10, 10.0, 210.0);
      Assert.assertEquals(0, binner.calculateBin(Double.NaN));
      Assert.assertEquals(1, binner.calculateBin(9.0));
      Assert.assertEquals(10, binner.calculateBin(215.0));
      Assert.assertEquals(1, binner.calculateBin(10.0));
      Assert.assertEquals(1, binner.calculateBin(29.9999));
      Assert.assertEquals(2, binner.calculateBin(30.0));
      Assert.assertEquals(2, binner.calculateBin(35.0));
      Assert.assertEquals(9, binner.calculateBin(189.9999));
      Assert.assertEquals(10, binner.calculateBin(190.0));
      Assert.assertEquals(10, binner.calculateBin(209.9999));
    }
  }
}
