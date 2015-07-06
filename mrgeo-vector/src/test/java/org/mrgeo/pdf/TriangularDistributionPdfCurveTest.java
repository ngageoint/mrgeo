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

package org.mrgeo.pdf;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.junit.UnitTest;

public class TriangularDistributionPdfCurveTest
{
  private final double EPSILON = 1e-8;

  @Test
  @Category(UnitTest.class)
  public void testBadCurveMaxTooSmall() throws IllegalArgumentException
  {
    double min = 20.0;
    double max = 10.0;
    double mode = 15.0;
    TriangularDistributionPdfCurve triCurve = new TriangularDistributionPdfCurve(min, mode, max, 0.0);
    try
    {
      Assert.assertEquals(0.0, triCurve.getLikelihood(12.0), EPSILON);
      Assert.fail("Expected an error indicating that the max must be greater than the min");
    }
    catch(IllegalArgumentException e)
    {
      // We expect an exception in this case because the curve is poorly defined.
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testBadCurveModeTooLarge() throws IllegalArgumentException
  {
    double min = 20.0;
    double max = 30.0;
    double mode = 45.0;
    TriangularDistributionPdfCurve triCurve = new TriangularDistributionPdfCurve(min, mode, max, 0.0);
    try
    {
      Assert.assertEquals(0.0, triCurve.getLikelihood(12.0), EPSILON);
      Assert.fail("Expected an error indicating that the mode is out of range");
    }
    catch(IllegalArgumentException e)
    {
      // We expect an exception in this case because the curve is poorly defined.
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testGetLikelihoodBad() throws IllegalArgumentException
  {
    double min = 0.0;
    double max = 10.0;
    double mode = 5.0;
    TriangularDistributionPdfCurve triCurve = new TriangularDistributionPdfCurve(min, mode, max, 0.0);
    Assert.assertEquals(0.0, triCurve.getLikelihood(12.0), EPSILON);
    Assert.assertEquals(0.0, triCurve.getLikelihood(-1.0), EPSILON);
    Assert.assertEquals(0.0, triCurve.getLikelihood(Double.NaN), EPSILON);
  }

  @Test
  @Category(UnitTest.class)
  public void testGetLikelihoodSymmetric() throws IllegalArgumentException
  {
    double min = 0.0;
    double max = 10.0;
    double mode = 5.0;
    TriangularDistributionPdfCurve triCurve = new TriangularDistributionPdfCurve(min, mode, max, 0.0);
    Assert.assertEquals(0.0, triCurve.getLikelihood(min), EPSILON);
    Assert.assertEquals(0.0, triCurve.getLikelihood(max), EPSILON);
    Assert.assertEquals(0.2, triCurve.getLikelihood(mode), EPSILON);
    Assert.assertEquals(0.1, triCurve.getLikelihood((mode - min) / 2.0), EPSILON);
    Assert.assertEquals(0.1, triCurve.getLikelihood((max - mode) / 2.0), EPSILON);
  }

  @Test
  @Category(UnitTest.class)
  public void testGetLikelihoodModeEqualsMin() throws IllegalArgumentException
  {
    double min = 1.0;
    double max = 11.0;
    double mode = min;
    TriangularDistributionPdfCurve triCurve = new TriangularDistributionPdfCurve(min, mode, max, 0.0);
    Assert.assertEquals(0.0, triCurve.getLikelihood(min), EPSILON);
    Assert.assertEquals(2.0, triCurve.getLikelihood(max), EPSILON);
    Assert.assertEquals(1.0, triCurve.getLikelihood((min + max) / 2.0), EPSILON);
  }

  @Test
  @Category(UnitTest.class)
  public void testGetLikelihoodModeEqualsMax() throws IllegalArgumentException
  {
    double min = 2.0;
    double max = 12.0;
    double mode = max;
    TriangularDistributionPdfCurve triCurve = new TriangularDistributionPdfCurve(min, mode, max, 0.0);
    Assert.assertEquals(0.0, triCurve.getLikelihood(min), EPSILON);
    Assert.assertEquals(2.0, triCurve.getLikelihood(max), EPSILON);
    Assert.assertEquals(1.0, triCurve.getLikelihood((min + max) / 2.0), EPSILON);
  }

  @Test
  @Category(UnitTest.class)
  public void testGetLikelihoodMinEqualsModeEqualsMax() throws IllegalArgumentException
  {
    double min = 2.0;
    double max = min;
    double mode = min;
    TriangularDistributionPdfCurve triCurve = new TriangularDistributionPdfCurve(min, mode, max, 0.0);
    try
    {
      Assert.assertEquals(0.0, triCurve.getLikelihood(min), EPSILON);
      Assert.fail("Expected an error indicating that max must be greater than min");
    }
    catch(IllegalArgumentException e)
    {
      // We expect an exception in this case because the curve is poorly defined.
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testGetLikelihood() throws IllegalArgumentException
  {
    double min = 0.0;
    double max = 10.0;
    double mode = 7.5;
    TriangularDistributionPdfCurve triCurve = new TriangularDistributionPdfCurve(min, mode, max, 0.0);
    Assert.assertEquals(0.0, triCurve.getLikelihood(min), EPSILON);
    Assert.assertEquals(0.0, triCurve.getLikelihood(max), EPSILON);
    Assert.assertEquals(0.2, triCurve.getLikelihood(mode), EPSILON);
    Assert.assertEquals(0.1, triCurve.getLikelihood((mode - min) / 2.0), EPSILON);
    double value = mode + (max - mode) / 2.0;
    double expected = 2.0 * (max - value) / ((max - min) * (max - mode));
    Assert.assertEquals(expected, triCurve.getLikelihood(value), EPSILON);
    value = mode + (max - mode) * 9.0 / 10.0;
    expected = 2.0 * (max - value) / ((max - min) * (max - mode));
    Assert.assertEquals(expected, triCurve.getLikelihood(value), EPSILON);
    value = min + (mode - min) * 9.0 / 10.0;
    expected = 2.0 * (value - min) / ((max - min) * (mode - min));
    Assert.assertEquals(expected, triCurve.getLikelihood(value), EPSILON);
  }
}
