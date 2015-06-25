package org.mrgeo.rasterops;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.progress.Progress;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.utils.Bounds;
import org.mrgeo.utils.TMSUtils;
import org.mrgeo.utils.TMSUtils.LatLon;
import org.mrgeo.utils.TMSUtils.Pixel;

import java.io.IOException;
import java.util.HashSet;

@SuppressWarnings("static-method")
public class RandomSamplerTest
{
  public class RandomSamplerTestListener implements RandomSampler.RandomSamplerListener
  {
    private long count = 0;
    private Bounds bounds;
    
    public RandomSamplerTestListener(Bounds bounds)
    {
      this.bounds = bounds;
    }

    @Override
    public void beginningSamples() throws IOException
    {
    }

    @Override
    public void completedSamples() throws IOException
    {
    }

    @Override
    public void reportPoint(Pixel pixel, LatLon location) throws IOException
    {
      count++;
      Assert.assertTrue("Generated point (" + location.lat + ", " + location.lon +
          ") outside of bounds (" + bounds.toString() + ")",
          bounds.containsPoint(location.lon, location.lat));
    }

    public long getCount()
    {
      return count;
    }
  }

  public class TestProgress implements Progress
  {
    @Override
    public void complete()
    {
    }

    @Override
    public void complete(String result, String kml)
    {
    }

    @Override
    public void failed(String result)
    {
    }

    @Override
    public float get()
    {
      return 0;
    }

    @Override
    public String getResult()
    {
      return "";
    }

    @Override
    public boolean isFailed()
    {
      return false;
    }

    @Override
    public void set(float progress)
    {
    }

    @Override
    public void starting()
    {
    }

    @Override
    public void cancelled()
    {
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testAllPixelIteratorNoExclusions()
  {
    TMSUtils.Pixel topLeft = new TMSUtils.Pixel(0, 0);
    long height = 4;
    long width = 3;
    TMSUtils.Pixel bottomRight = new TMSUtils.Pixel(width - 1, height - 1);
    RandomSampler.AllPixelIterator iter = new RandomSampler.AllPixelIterator(topLeft, bottomRight);
    validateIteratorResults(iter, topLeft, bottomRight, null);
  }

  @Test
  @Category(UnitTest.class)
  public void testAllPixelIteratorNoExclusions2()
  {
    long top = 4;
    long left = 6;
    TMSUtils.Pixel topLeft = new TMSUtils.Pixel(left, top);
    long height = 4;
    long width = 3;
    TMSUtils.Pixel bottomRight = new TMSUtils.Pixel(left + width - 1, top + height - 1);
    RandomSampler.AllPixelIterator iter = new RandomSampler.AllPixelIterator(topLeft, bottomRight);
    validateIteratorResults(iter, topLeft, bottomRight, null);
  }

  @Test
  @Category(UnitTest.class)
  public void testAllPixelIteratorWithExclusions()
  {
    TMSUtils.Pixel topLeft = new TMSUtils.Pixel(0, 0);
    long height = 4;
    long width = 3;
    TMSUtils.Pixel bottomRight = new TMSUtils.Pixel(width - 1, height - 1);
    HashSet<Pixel> exclusions = new HashSet<Pixel>();
    exclusions.add(new TMSUtils.Pixel(0, 0));
    exclusions.add(bottomRight);
    RandomSampler.AllPixelIterator iter = new RandomSampler.AllPixelIterator(topLeft, bottomRight, exclusions);
    validateIteratorResults(iter, topLeft, bottomRight, exclusions);
  }

  @Test
  @Category(UnitTest.class)
  public void testAllPixelIteratorWithExclusions2()
  {
    long top = 4;
    long left = 6;
    TMSUtils.Pixel topLeft = new TMSUtils.Pixel(left, top);
    long height = 4;
    long width = 3;
    TMSUtils.Pixel bottomRight = new TMSUtils.Pixel(left + width - 1, top + height - 1);
    HashSet<Pixel> exclusions = new HashSet<Pixel>();
    exclusions.add(new TMSUtils.Pixel(left, top));
    exclusions.add(new TMSUtils.Pixel(left + 1, top + 1));
    exclusions.add(bottomRight);
    RandomSampler.AllPixelIterator iter = new RandomSampler.AllPixelIterator(topLeft, bottomRight, exclusions);
    validateIteratorResults(iter, topLeft, bottomRight, exclusions);
  }

  private static void validateIteratorResults(RandomSampler.AllPixelIterator iter,
      TMSUtils.Pixel topLeft, TMSUtils.Pixel bottomRight,
      HashSet<TMSUtils.Pixel> exclusions)
  {
    long count = 0;
    for (long py = topLeft.py; py <= bottomRight.py; py++)
    {
      for (long px = topLeft.px; px <= bottomRight.px; px++)
      {
        TMSUtils.Pixel possiblePixel = new TMSUtils.Pixel(px, py);
        if (exclusions == null || !exclusions.contains(possiblePixel))
        {
          Assert.assertTrue(iter.hasNext());
          TMSUtils.Pixel pixel = iter.next();
          Assert.assertNotNull(pixel);
          Assert.assertEquals(px, pixel.px);
          Assert.assertEquals(py, pixel.py);
          count++;
        }
      }
    }
    // All pixels should have been iterated
    Assert.assertFalse(iter.hasNext());
    long exclusionCount = (exclusions != null) ? exclusions.size() : 0;
    long expectedCount = ((bottomRight.py - topLeft.py + 1) * (bottomRight.px - topLeft.px + 1)) - exclusionCount;
    Assert.assertEquals(expectedCount, count);
  }

  @Test
  @Category(UnitTest.class)
  public void testOneSample() throws IOException
  {
    RandomSampler rs = new RandomSampler();
    int zoomLevel = 5;
    int tileSize = MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT_INT;
    double resolution = TMSUtils.resolution(zoomLevel, tileSize);
    TMSUtils.Pixel topLeftPixel = new TMSUtils.Pixel(2, 4);
    TMSUtils.Pixel bottomRightPixel = new TMSUtils.Pixel(5, 8);
    TMSUtils.LatLon topLeft = TMSUtils.pixelToLatLonUL(topLeftPixel.px, topLeftPixel.py, zoomLevel, tileSize);
    // The world coord of the top left corner of the bottom right pixel
    TMSUtils.LatLon bottomRight = TMSUtils.pixelToLatLonUL(bottomRightPixel.px, bottomRightPixel.py, zoomLevel, tileSize);
    Bounds bounds = new Bounds(topLeft.lon, bottomRight.lat, bottomRight.lon, topLeft.lat);
    Bounds testBounds = new Bounds(topLeft.lon, bottomRight.lat - resolution, bottomRight.lon + resolution, topLeft.lat);
    RandomSamplerTestListener listener = new RandomSamplerTestListener(testBounds);
    long sampleCount = 1;
    Progress p = new TestProgress();
    rs.generateRandomPoints(p, bounds, zoomLevel, tileSize, sampleCount, listener);
    Assert.assertEquals(sampleCount, listener.getCount());
  }

  private static long calculateTotalPixels(TMSUtils.Pixel topLeftPixel, TMSUtils.Pixel bottomRightPixel)
  {
    long pixelsWide = bottomRightPixel.px - topLeftPixel.px + 1;
    long pixelsHigh = bottomRightPixel.py - topLeftPixel.py + 1;
    long totalPixels = pixelsHigh * pixelsWide;
    return totalPixels;
  }

  @Test
  @Category(UnitTest.class)
  public void testFullAutoSample() throws IOException
  {
    // When the total number of AOI points is < 100,000
    // test that we use the total number of AOI points.
    RandomSampler rs = new RandomSampler();
    int zoomLevel = 5;
    int tileSize = MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT_INT;
    double resolution = TMSUtils.resolution(zoomLevel, tileSize);
    TMSUtils.Pixel topLeftPixel = new TMSUtils.Pixel(2, 4);
    TMSUtils.Pixel bottomRightPixel = new TMSUtils.Pixel(101, 83);
    TMSUtils.LatLon topLeft = TMSUtils.pixelToLatLonUL(topLeftPixel.px, topLeftPixel.py, zoomLevel, tileSize);
    // The world coord of the top left corner of the bottom right pixel
    TMSUtils.LatLon bottomRight = TMSUtils.pixelToLatLonUL(bottomRightPixel.px, bottomRightPixel.py, zoomLevel, tileSize);
    Bounds bounds = new Bounds(topLeft.lon, bottomRight.lat, bottomRight.lon, topLeft.lat);
    Bounds testBounds = new Bounds(topLeft.lon, bottomRight.lat - resolution, bottomRight.lon + resolution, topLeft.lat);
    RandomSamplerTestListener listener = new RandomSamplerTestListener(testBounds);
    long totalPixels = calculateTotalPixels(topLeftPixel, bottomRightPixel);
    Progress p = new TestProgress();
    rs.generateRandomPoints(p, bounds, zoomLevel, tileSize, 0, listener);
    Assert.assertEquals(totalPixels, listener.getCount());
  }

  @Test
  @Category(UnitTest.class)
  public void testFullBoundaryAutoSample() throws IOException
  {
    // When the total number of AOI points = 100,000
    // test that we use the total number of AOI points.
    RandomSampler rs = new RandomSampler();
    int zoomLevel = 5;
    int tileSize = MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT_INT;
    double resolution = TMSUtils.resolution(zoomLevel, tileSize);
    TMSUtils.Pixel topLeftPixel = new TMSUtils.Pixel(2, 4);
    TMSUtils.Pixel bottomRightPixel = new TMSUtils.Pixel(101, 1003);
    TMSUtils.LatLon topLeft = TMSUtils.pixelToLatLonUL(topLeftPixel.px, topLeftPixel.py, zoomLevel, tileSize);
    // The world coord of the top left corner of the bottom right pixel
    TMSUtils.LatLon bottomRight = TMSUtils.pixelToLatLonUL(bottomRightPixel.px, bottomRightPixel.py, zoomLevel, tileSize);
    Bounds bounds = new Bounds(topLeft.lon, bottomRight.lat, bottomRight.lon, topLeft.lat);
    Bounds testBounds = new Bounds(topLeft.lon, bottomRight.lat - resolution, bottomRight.lon + resolution, topLeft.lat);
    RandomSamplerTestListener listener = new RandomSamplerTestListener(testBounds);
    Progress p = new TestProgress();
    rs.generateRandomPoints(p, bounds, zoomLevel, tileSize, 0, listener);
    Assert.assertEquals(100000, listener.getCount());
  }

  @Test
  @Category(UnitTest.class)
  public void testMinimumAutoSample() throws IOException
  {
    // When 20% of the AOI points is < 100,000, the number of
    // generated points should be 100,000.
    RandomSampler rs = new RandomSampler();
    int zoomLevel = 5;
    int tileSize = MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT_INT;
    double resolution = TMSUtils.resolution(zoomLevel, tileSize);
    TMSUtils.Pixel topLeftPixel = new TMSUtils.Pixel(2, 4);
    TMSUtils.Pixel bottomRightPixel = new TMSUtils.Pixel(601, 603);
    TMSUtils.LatLon topLeft = TMSUtils.pixelToLatLonUL(topLeftPixel.px, topLeftPixel.py, zoomLevel, tileSize);
    // The world coord of the top left corner of the bottom right pixel
    TMSUtils.LatLon bottomRight = TMSUtils.pixelToLatLonUL(bottomRightPixel.px, bottomRightPixel.py, zoomLevel, tileSize);
    Bounds bounds = new Bounds(topLeft.lon, bottomRight.lat, bottomRight.lon, topLeft.lat);
    Bounds testBounds = new Bounds(topLeft.lon, bottomRight.lat - resolution, bottomRight.lon + resolution, topLeft.lat);
    RandomSamplerTestListener listener = new RandomSamplerTestListener(testBounds);
    Progress p = new TestProgress();
    rs.generateRandomPoints(p, bounds, zoomLevel, tileSize, 0, listener);
    Assert.assertEquals(100000, listener.getCount());
  }

  @Test
  @Category(UnitTest.class)
  public void testMaximumAutoSample() throws IOException
  {
    // When 20% of the AOI points is > 100,000, the number of
    // generated points should be 20% of the AOI points.
    RandomSampler rs = new RandomSampler();
    int zoomLevel = 5;
    int tileSize = MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT_INT;
    double resolution = TMSUtils.resolution(zoomLevel, tileSize);
    TMSUtils.Pixel topLeftPixel = new TMSUtils.Pixel(2, 4);
    TMSUtils.Pixel bottomRightPixel = new TMSUtils.Pixel(551, 1003);
    TMSUtils.LatLon topLeft = TMSUtils.pixelToLatLonUL(topLeftPixel.px, topLeftPixel.py, zoomLevel, tileSize);
    // The world coord of the top left corner of the bottom right pixel
    TMSUtils.LatLon bottomRight = TMSUtils.pixelToLatLonUL(bottomRightPixel.px, bottomRightPixel.py, zoomLevel, tileSize);
    Bounds bounds = new Bounds(topLeft.lon, bottomRight.lat, bottomRight.lon, topLeft.lat);
    Bounds testBounds = new Bounds(topLeft.lon, bottomRight.lat - resolution, bottomRight.lon + resolution, topLeft.lat);
    RandomSamplerTestListener listener = new RandomSamplerTestListener(testBounds);
    long totalPixels = calculateTotalPixels(topLeftPixel, bottomRightPixel);
    Progress p = new TestProgress();
    rs.generateRandomPoints(p, bounds, zoomLevel, tileSize, 0, listener);
    Assert.assertEquals((long)(totalPixels * 0.2), listener.getCount());
  }

  @Test
  @Category(UnitTest.class)
  public void testSubSample() throws IOException
  {
    // Test a specified sample count of at least the expected minimum
    RandomSampler rs = new RandomSampler();
    int zoomLevel = 8;
    int tileSize = MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT_INT;
    double minX = -180.0;
    double minY = -90.0;
    double maxX = -178.0;
    double maxY = -87.0;
    double resolution = TMSUtils.resolution(zoomLevel, tileSize);
    Bounds bounds = new Bounds(minX, minY, maxX, maxY);
    Bounds testBounds = new Bounds(minX, minY, maxX, maxY);
    testBounds.expand(resolution, resolution);
    Progress p = new TestProgress();
    long sampleCount = 100001;
    RandomSamplerTestListener listener = new RandomSamplerTestListener(testBounds);
    rs.generateRandomPoints(p, bounds, zoomLevel, tileSize, sampleCount, listener);
    Assert.assertEquals(sampleCount, listener.getCount());
  }
}
