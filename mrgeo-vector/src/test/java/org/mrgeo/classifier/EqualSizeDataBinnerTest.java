/*
 * Copyright 2009-2015 DigitalGlobe, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

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
