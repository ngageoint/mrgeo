package org.mrgeo.rasterops;

import com.google.common.primitives.Longs;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.rasterops.CostDistanceDriver.RasterizeVectorReader;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.test.MapOpTestUtils;
import org.mrgeo.utils.TMSUtils.Bounds;

import java.io.IOException;

@SuppressWarnings("static-method")
public class RasterizeVectorReaderTest
{
  private static MapOpTestUtils testUtils;
  private static final String RASTERIZED_VECTOR_INPUT = "rasterized_vector";
  private static final int INPUT_ZOOM = 10;

  @BeforeClass
  public static void init()
  {
    try {
      testUtils = new MapOpTestUtils(RasterizeVectorReaderTest.class);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  @Test
  @Category(UnitTest.class)
  public void testRead() throws IOException {
    Path input = new Path("file://" + testUtils.getInputLocal(), RASTERIZED_VECTOR_INPUT);
    RasterizeVectorReader rvr = new RasterizeVectorReader();    
    rvr.read(input.toUri().toString(), INPUT_ZOOM, null);
    Assert.assertArrayEquals(new long[]{349880,352955,358081}, Longs.toArray(rvr.getTileIds()));
    
    Bounds expectedBounds = new Bounds(64.6875, 29.8828125, 68.203125, 33.046875);
    Assert.assertEquals(expectedBounds, rvr.getBounds());
  }
}
