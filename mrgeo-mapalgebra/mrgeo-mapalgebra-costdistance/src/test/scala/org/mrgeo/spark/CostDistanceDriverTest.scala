package org.mrgeo.spark

import junit.framework.Assert
import org.junit.Test
import org.junit.experimental.categories.Category
import org.mrgeo.junit.UnitTest
import org.mrgeo.utils.{TMSUtils, LatLng, Bounds}
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitSuite

import scala.collection.mutable

class CostDistanceDriverTest extends FlatSpec {
  behavior of "bounds calculation (with maxCost of METERS_PER_DEGREE seconds/meter and min friction value of 0.5"

  val maxCost: Double = LatLng.METERS_PER_DEGREE
  val minPixelValue: Double = 0.5
  val EPSILON = 1e-5

  // With the maxCost value set to METERS_PER_DEGREE and the minPixelValue at 0.5, the bounds
  // should extend 2 degrees in all four directions beyond the MBR of the source points.

  "Using a source point at lon=0, lat=0" should "return bounds from (-2, -2) to (2, 2)" in {
    val sourcePoints: mutable.ListBuffer[(Float,Float)] = new mutable.ListBuffer[(Float,Float)]
    sourcePoints.append((0.0f, 0.0f))
    // The distance is 20000 meters for the expanded bounds.
    val b: Bounds = CostDistanceDriver.calculateBoundsFromCost(maxCost, sourcePoints, minPixelValue)
    Assert.assertNotNull(b)
    Assert.assertEquals(-2.0, b.getMinX, EPSILON)
    Assert.assertEquals(-2.0, b.getMinY, EPSILON)
    Assert.assertEquals(2.0, b.getMaxX, EPSILON)
    Assert.assertEquals(2.0, b.getMaxY, EPSILON)
  }

  "Using a source point at lon=180, lat=90" should "return bounds from (178, 88) to (180, 90)" in {
    val sourcePoints: mutable.ListBuffer[(Float,Float)] = new mutable.ListBuffer[(Float,Float)]
    sourcePoints.append((180.0f, 90.0f))
    // The distance is 20000 meters for the expanded bounds.
    val b: Bounds = CostDistanceDriver.calculateBoundsFromCost(maxCost, sourcePoints, minPixelValue)
    Assert.assertNotNull(b)
    Assert.assertEquals(178.0, b.getMinX, EPSILON)
    Assert.assertEquals(88.0, b.getMinY, EPSILON)
    Assert.assertEquals(180.0, b.getMaxX, EPSILON)
    Assert.assertEquals(90.0, b.getMaxY, EPSILON)
  }

  "Using a source point at lon=180, lat=-90" should "return bounds from (178, -90) to (180, -88)" in {
    val sourcePoints: mutable.ListBuffer[(Float,Float)] = new mutable.ListBuffer[(Float,Float)]
    sourcePoints.append((180.0f, -90.0f))
    // The distance is 20000 meters for the expanded bounds.
    val b: Bounds = CostDistanceDriver.calculateBoundsFromCost(maxCost, sourcePoints, minPixelValue)
    Assert.assertNotNull(b)
    Assert.assertEquals(178.0, b.getMinX, EPSILON)
    Assert.assertEquals(-90.0, b.getMinY, EPSILON)
    Assert.assertEquals(180.0, b.getMaxX, EPSILON)
    Assert.assertEquals(-88.0, b.getMaxY, EPSILON)
  }

  "Using a source point at lon=-180, lat=-90" should "return bounds from (-180, -90) to (-178, -88)" in {
    val sourcePoints: mutable.ListBuffer[(Float,Float)] = new mutable.ListBuffer[(Float,Float)]
    sourcePoints.append((-180.0f, -90.0f))
    // The distance is 20000 meters for the expanded bounds.
    val b: Bounds = CostDistanceDriver.calculateBoundsFromCost(maxCost, sourcePoints, minPixelValue)
    Assert.assertNotNull(b)
    Assert.assertEquals(-180.0, b.getMinX, EPSILON)
    Assert.assertEquals(-90.0, b.getMinY, EPSILON)
    Assert.assertEquals(-178.0, b.getMaxX, EPSILON)
    Assert.assertEquals(-88.0, b.getMaxY, EPSILON)
  }

  "Using a source point at lon=-180, lat=90" should "return bounds from (-180, 88) to (-178, 90)" in {
    val sourcePoints: mutable.ListBuffer[(Float,Float)] = new mutable.ListBuffer[(Float,Float)]
    sourcePoints.append((-180.0f, 90.0f))
    // The distance is 20000 meters for the expanded bounds.
    val b: Bounds = CostDistanceDriver.calculateBoundsFromCost(maxCost, sourcePoints, minPixelValue)
    Assert.assertNotNull(b)
    Assert.assertEquals(-180.0, b.getMinX, EPSILON)
    Assert.assertEquals(88.0, b.getMinY, EPSILON)
    Assert.assertEquals(-178.0, b.getMaxX, EPSILON)
    Assert.assertEquals(90.0, b.getMaxY, EPSILON)
  }
}
