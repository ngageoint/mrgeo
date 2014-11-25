package org.mrgeo.pdf;

import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.databinner.DataBinner;
import org.mrgeo.junit.UnitTest;

/**
 * The data used in this unit test came from event RFD/PDF data that I ran in Signature Analyst.
 * To determine the correct bin calculation, I exported the SA PDF to a text file. The data
 * values used to initialize the PdfDataBinner came from event RFD stats computed by SA. This
 * makes for a good test since we are only testing the PDF binning in this unit test, not RFD
 * calculation. 
 */
@SuppressWarnings("static-method")
public class PdfDataBinnerTest
{
  private static final double MIN = 0.140736829115484;
  private static final double MAX = 0.924996589945073;
  private static final double MIN_BW = 0.1826795299999981;
  private static final long DATA_SIZE = 28;
  private static final double Q1 = 0.20412128602761376;
  private static final double Q3 = 0.58424987648521388;
  
  @Test 
  @Category(UnitTest.class)
  public void testOutOfBounds()
  {
    DataBinner b = new PdfDataBinner(MIN, MAX, MIN_BW, DATA_SIZE, Q1, Q3);
    // Test too small
    Assert.assertEquals(-1, b.calculateBin(-1.0));
    // test too large
    Assert.assertEquals(-1, b.calculateBin(2.0));
  }

  @Test 
  @Category(UnitTest.class)
  public void test1()
  {
    DataBinner b = new PdfDataBinner(MIN, MAX, MIN_BW, DATA_SIZE, Q1, Q3);
    Assert.assertEquals(109, b.getBinCount());
    Assert.assertEquals(0, b.calculateBin(0.001));
    Assert.assertEquals(1, b.calculateBin(0.01523));
    Assert.assertEquals(107, b.calculateBin(1.63));
    Assert.assertEquals(108, b.calculateBin(1.645));
  }
}
