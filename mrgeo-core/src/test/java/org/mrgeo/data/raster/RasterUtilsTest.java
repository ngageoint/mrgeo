/*
 * Copyright 2009-2016 DigitalGlobe, Inc.
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
 *
 */

package org.mrgeo.data.raster;

import junit.framework.Assert;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.test.TestUtils;

import java.awt.image.*;

@SuppressWarnings("static-method")
public class RasterUtilsTest
{ 

  @BeforeClass
  public static void init()
  {
  }


  @AfterClass
  public static void tearDown() throws Exception
  {    
  }

  @Before
  public void setUp() throws Exception
  {
  }

  @Test
  @Category(UnitTest.class)
  public void testIsFloatingPoint()
  {
    Assert.assertTrue(RasterUtils.isFloatingPoint(DataBuffer.TYPE_DOUBLE));
    Assert.assertTrue(RasterUtils.isFloatingPoint(DataBuffer.TYPE_FLOAT));
    Assert.assertFalse(RasterUtils.isFloatingPoint(DataBuffer.TYPE_BYTE));
    Assert.assertFalse(RasterUtils.isFloatingPoint(DataBuffer.TYPE_INT));
    Assert.assertFalse(RasterUtils.isFloatingPoint(DataBuffer.TYPE_SHORT));
    Assert.assertFalse(RasterUtils.isFloatingPoint(DataBuffer.TYPE_USHORT));
  }

  @Test(expected=IllegalArgumentException.class)
  @Category(UnitTest.class)
  public void testIsFloatingPointWithBadInput1()
  {
    RasterUtils.isFloatingPoint(DataBuffer.TYPE_UNDEFINED);
  }

  @Test(expected=IllegalArgumentException.class)
  @Category(UnitTest.class)
  public void testIsFloatingPointWithBadInput2()
  {
    RasterUtils.isFloatingPoint(1000);
  }




}
