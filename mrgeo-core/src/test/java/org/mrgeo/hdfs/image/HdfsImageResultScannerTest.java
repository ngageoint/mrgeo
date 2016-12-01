package org.mrgeo.hdfs.image;

import junit.framework.Assert;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.MapFile;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.data.raster.MrGeoRaster;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.hdfs.utils.HdfsMrsImageReaderBuilder;
import org.mrgeo.hdfs.utils.MapFileReaderBuilder;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.test.TestUtils;
import org.mrgeo.utils.LongRectangle;
import org.mrgeo.utils.tms.TMSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;

@SuppressWarnings("all") // test code, not included in production
public class HdfsImageResultScannerTest {
private static final Logger logger = LoggerFactory.getLogger(HdfsImageResultScannerTest.class);

private HdfsMrsImageReader mockImageReader;
private MapFile.Reader firstPartitionMockMapFileReader;
private MapFile.Reader secondPartitionMapFileReader;
private LongRectangle bounds = new LongRectangle(0L,0L, 7L, 3L);
private TileIdWritable[] firstPartitionTileIds = {new TileIdWritable(2L), new TileIdWritable(4L), new TileIdWritable(6L)};
private TileIdWritable[] secondPartitionTileIds = {new TileIdWritable(8L), new TileIdWritable(10L), new TileIdWritable(12L)};
private RasterWritable[] firstPartitionRasters = createRasters(0, firstPartitionTileIds.length);
private RasterWritable[] secondPartitionRasters = createRasters(firstPartitionRasters.length, secondPartitionTileIds.length);
private int zoom = 0;

private HdfsImageResultScanner subject;

public HdfsImageResultScannerTest() throws IOException
{
}

@Before
public void setUp() throws Exception {


}

@After
public void tearDown() throws Exception {

}

@Test
@Category(UnitTest.class)
public void testConstructionWithNoZoom() throws Exception {
  subject = createDefaultSubject(0,bounds);
  Assert.assertEquals(firstPartitionTileIds[0], subject.currentKey());
}

@Test
@Category(UnitTest.class)
public void testConstructionWithZoom() throws Exception {
  zoom = 3;
  subject = createDefaultSubject(zoom,bounds);
  Assert.assertTrue(subject.hasNext());
  assertIterateOverTiles(0,firstPartitionTileIds.length + secondPartitionTileIds.length - 1);
}

@Test
@Category(UnitTest.class)
public void testConstructionWithZoomOutOfLeftBound() throws Exception {
  zoom = 3;
  bounds = new LongRectangle(1, 0, 7, 2);
  subject = createDefaultSubject(zoom, bounds);
  Assert.assertTrue(subject.hasNext());
  assertIterateOverTiles(0,firstPartitionTileIds.length + secondPartitionTileIds.length - 1);
}

@Test
@Category(UnitTest.class)
public void testConstructionWithZoomOutOfRightBound() throws Exception {
  zoom = 3;
  bounds = new LongRectangle(0, 0, 3, 2);
  subject = createDefaultSubject(zoom, bounds);
  Assert.assertTrue(subject.hasNext());
  assertIterateOverTiles(0,firstPartitionTileIds.length + secondPartitionTileIds.length - 1);
}

@Test
@Category(UnitTest.class)
public void testStartEqualFirstTileEndEqualLast() throws Exception {
  subject = createDefaultSubject(firstPartitionTileIds[0], secondPartitionTileIds[secondPartitionTileIds.length - 1]);
  // Need an initial hasNext so the readFirst flag will clear.  This behavior should probably be changed
  Assert.assertTrue(subject.hasNext());
  assertIterateOverTiles(0,firstPartitionTileIds.length + secondPartitionTileIds.length - 1);
  Assert.assertFalse(subject.hasNext());
}

@Test
@Category(UnitTest.class)
public void testStartGreaterThanFirstTileEndEqualLast() throws Exception {
  subject = createDefaultSubject(firstPartitionTileIds[1], secondPartitionTileIds[secondPartitionTileIds.length - 1]);
//        Assert.assertEquals(startIndex < firstPartitionTileIds.length ? 0 : 1, subject.curPartitionIndex);
  // Need an initial hasNext so the readFirst flag will clear.  This behavior should probably be changed
  Assert.assertTrue(subject.hasNext());
  assertIterateOverTiles(1,firstPartitionTileIds.length + secondPartitionTileIds.length - 1);
  Assert.assertFalse(subject.hasNext());
}

@Test
@Category(UnitTest.class)
public void testStartInSecondPartitionEndEqualLast() throws Exception {
  subject = createDefaultSubject(secondPartitionTileIds[0], secondPartitionTileIds[secondPartitionTileIds.length - 1]);
//        Assert.assertEquals(startIndex < firstPartitionTileIds.length ? 0 : 1, subject.curPartitionIndex);
  // Need an initial hasNext so the readFirst flag will clear.  This behavior should probably be changed
  Assert.assertTrue(subject.hasNext());
  assertIterateOverTiles(firstPartitionTileIds.length, firstPartitionTileIds.length + secondPartitionTileIds.length - 1);
  Assert.assertFalse(subject.hasNext());
}

@Test
@Category(UnitTest.class)
public void testStartEqualFirstTileEndBeforeLast() throws Exception {
  subject = createDefaultSubject(firstPartitionTileIds[0], secondPartitionTileIds[secondPartitionTileIds.length - 2]);
  // Need an initial hasNext so the readFirst flag will clear.  This behavior should probably be changed
  Assert.assertTrue(subject.hasNext());
  assertIterateOverTiles(0,firstPartitionTileIds.length + secondPartitionTileIds.length - 2);
  Assert.assertFalse(subject.hasNext());
}

@Test
@Category(UnitTest.class)
public void testStartEqualFirstTileEndInFirstPartition() throws Exception {
  subject = createDefaultSubject(firstPartitionTileIds[0], firstPartitionTileIds[firstPartitionTileIds.length - 1]);
  // Need an initial hasNext so the readFirst flag will clear.  This behavior should probably be changed
  Assert.assertTrue(subject.hasNext());
  assertIterateOverTiles(0, firstPartitionTileIds.length - 1);
  Assert.assertFalse(subject.hasNext());
}

@Test
@Category(UnitTest.class)
public void testNullStart() throws Exception {
  subject = createDefaultSubject(null, secondPartitionTileIds[secondPartitionTileIds.length - 2]);
  // Need an initial hasNext so the readFirst flag will clear.  This behavior should probably be changed
  Assert.assertTrue(subject.hasNext());
  assertIterateOverTiles(0, firstPartitionTileIds.length + secondPartitionTileIds.length - 2);
  Assert.assertFalse(subject.hasNext());
}

@Test
@Category(UnitTest.class)
public void testNullEnd() throws Exception {
  subject = createDefaultSubject(firstPartitionTileIds[1], null);
  // Need an initial hasNext so the readFirst flag will clear.  This behavior should probably be changed
  Assert.assertTrue(subject.hasNext());
  assertIterateOverTiles(1, firstPartitionTileIds.length + secondPartitionTileIds.length - 1);
  Assert.assertFalse(subject.hasNext());
}

@Test
@Category(UnitTest.class)
public void testNullStartAndEnd() throws Exception {
  subject = createDefaultSubject(null, null);
  // Need an initial hasNext so the readFirst flag will clear.  This behavior should probably be changed
  Assert.assertTrue(subject.hasNext());
  assertIterateOverTiles(0, firstPartitionTileIds.length + secondPartitionTileIds.length - 1);
  Assert.assertFalse(subject.hasNext());
}

@Test
@Category(UnitTest.class)
public void testStartAfterLastTile() throws Exception {
  subject = createDefaultSubject(new TileIdWritable(secondPartitionTileIds[secondPartitionTileIds.length - 1].get() + 2), null);
  Assert.assertFalse(subject.hasNext());
}

@Test
@Category(UnitTest.class)
public void testEndBeforeFirstTile() throws Exception {
  subject = createDefaultSubject(null, new TileIdWritable(firstPartitionTileIds[0].get() - 1));
  Assert.assertFalse(subject.hasNext());
}

@Test
@Category(UnitTest.class)
public void testEndAfterLastTile() throws Exception {
  subject = createDefaultSubject(null, new TileIdWritable(secondPartitionTileIds[secondPartitionTileIds.length - 1].get() + 1));
  Assert.assertTrue(subject.hasNext());
  assertIterateOverTiles(0, firstPartitionTileIds.length + secondPartitionTileIds.length - 1);
  Assert.assertFalse(subject.hasNext());
}

@Test
@Category(UnitTest.class)
public void testEndNotEqualToKey() throws Exception {
  subject = createDefaultSubject(null, new TileIdWritable(secondPartitionTileIds[secondPartitionTileIds.length - 1].get() - 1));
  Assert.assertTrue(subject.hasNext());
  assertIterateOverTiles(0, firstPartitionTileIds.length + secondPartitionTileIds.length - 2);
  Assert.assertFalse(subject.hasNext());
}

@Test
@Category(UnitTest.class)
public void testReaderCannotBeCached() throws Exception {
  mockImageReader = createDefaultImageReader(0, false);
  subject = new HdfsImageResultScanner(firstPartitionTileIds[0],
      secondPartitionTileIds[secondPartitionTileIds.length - 1], mockImageReader);
  // Need an initial hasNext so the readFirst flag will clear.  This behavior should probably be changed
  Assert.assertTrue(subject.hasNext());
  assertIterateOverTiles(0,firstPartitionTileIds.length + secondPartitionTileIds.length - 1);
  Assert.assertFalse(subject.hasNext());
  verify(firstPartitionMockMapFileReader, atLeastOnce()).close();
}

@Test
@Category(UnitTest.class)
public void testClose() throws Exception {
  subject = createDefaultSubject(firstPartitionTileIds[0], firstPartitionTileIds[firstPartitionTileIds.length - 1]);
  subject.close();
  verify(firstPartitionMockMapFileReader, atLeastOnce()).close();
}

private HdfsMrsImageReader createDefaultImageReader(int zoomLevel, boolean canBeCached) throws IOException {
  firstPartitionMockMapFileReader = new MapFileReaderBuilder()
      .keyClass(TileIdWritable.class)
      .valueClass(RasterWritable.class)
      .keys(firstPartitionTileIds)
      .values(firstPartitionRasters)
      .build();
  secondPartitionMapFileReader = new MapFileReaderBuilder()
      .keyClass(TileIdWritable.class)
      .valueClass(RasterWritable.class)
      .keys(secondPartitionTileIds)
      .values(secondPartitionRasters)
      .build();
  HdfsMrsImageReaderBuilder builder = new HdfsMrsImageReaderBuilder()
      .canBeCached(canBeCached)
      .mapFileReader(firstPartitionMockMapFileReader)
      .mapFileReader(secondPartitionMapFileReader);
  if (zoomLevel > 0) {
    return builder.zoom(zoomLevel).build();
  }
  else {
    return builder.build();
  }
}

private HdfsImageResultScanner createDefaultSubject(int zoomLevel, LongRectangle bounds) throws IOException {
  mockImageReader = createDefaultImageReader(zoomLevel, true);
  return new HdfsImageResultScanner(bounds,mockImageReader);
}

private HdfsImageResultScanner createDefaultSubject(TileIdWritable start, TileIdWritable end) throws IOException {
  mockImageReader = createDefaultImageReader(0, true);
  return new HdfsImageResultScanner(start, end, mockImageReader);
}

private RasterWritable[] createRasters(int seed, int count) throws IOException
{
  RasterWritable[] rasters = new RasterWritable[count];
  for (int i = 0; i < count; i++) {
    byte[] bytes = new byte[12 + 4];
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    buffer.order(ByteOrder.LITTLE_ENDIAN);

    buffer.put((byte)0x03); // version
    buffer.putInt(1); // width
    buffer.putInt(1); // height
    buffer.putShort((short)1); // number of bands
    buffer.put((byte)DataBuffer.TYPE_BYTE); // datatype
    buffer.putInt(i + seed); // data

    rasters[i] = RasterWritable.fromBytes(bytes);
  }
  return rasters;
}

private void assertIterateOverTiles(int startIndex, int endIndex) throws IOException {
  logger.debug("Testing iteration over tiles, starting with " + startIndex + ", ending with " + endIndex + "...");
  for (int i = startIndex; i <= endIndex; i++) {
    if (i < firstPartitionTileIds.length) {
      if (zoom <= 0 || withinBounds(firstPartitionTileIds[i])) {
        Assert.assertTrue(subject.hasNext());
        assertTilesAreEqual(firstPartitionTileIds[i], subject.currentKey());
        TestUtils.compareRasters(RasterWritable.toMrGeoRaster(firstPartitionRasters[i]), subject.currentValue());

        // currentValue() and next() should return the same value
        TestUtils.compareRasters(RasterWritable.toMrGeoRaster(firstPartitionRasters[i]), subject.next());
      }

    }
    else {
      int i2 = i - firstPartitionTileIds.length;
      if (zoom <= 0 || withinBounds(secondPartitionTileIds[i2])) {
        Assert.assertTrue(subject.hasNext());
        assertTilesAreEqual(secondPartitionTileIds[i2], subject.currentKey());
        TestUtils.compareRasters(RasterWritable.toMrGeoRaster(secondPartitionRasters[i2]), subject.currentValue());

        // currentValue() and next() should return the same value
        TestUtils.compareRasters(RasterWritable.toMrGeoRaster(secondPartitionRasters[i2]), subject.next());
      }
    }
    logger.debug("Iteration " + i + " passed.");
  }
}

private boolean withinBounds(TileIdWritable tileId) {
  long tx = TMSUtils.tileid(tileId.get(), zoom).tx;
  return (tx >= bounds.getMinX() && tx <= bounds.getMaxX());
}

private void assertTilesAreEqual(TileIdWritable tile1, TileIdWritable tile2) {
  Assert.assertEquals(tile1.get(), tile2.get());
}
}