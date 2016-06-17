package org.mrgeo.hdfs.image;

import junit.framework.Assert;
import org.apache.hadoop.io.MapFile;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.hdfs.utils.HdfsMrsImageReaderBuilder;
import org.mrgeo.hdfs.utils.MapFileReaderBuilder;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.utils.LongRectangle;
import org.mrgeo.utils.tms.TMSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;

/**
 * Created by ericwood on 6/14/16.
 */
public class HdfsImageResultScannerTest {
    private static final Logger logger = LoggerFactory.getLogger(HdfsImageResultScannerTest.class);

    private HdfsMrsImageReader mockImageReader;
    private MapFile.Reader firstPartitionMockMapFileReader;
    private MapFile.Reader secondPartitionMapFileReader;
    private LongRectangle bounds = new LongRectangle(0L,0L, 14L, 14L);
    private TileIdWritable[] firstPartitionTileIds = {new TileIdWritable(2L), new TileIdWritable(4L), new TileIdWritable(6L)};
    private TileIdWritable[] secondPartitionTileIds = {new TileIdWritable(8L), new TileIdWritable(10L), new TileIdWritable(12L)};
    private RasterWritable[] firstPartitionRasters = createRasters(0, firstPartitionTileIds.length);
    private RasterWritable[] secondPartitionRasters = createRasters(firstPartitionRasters.length, secondPartitionTileIds.length);
    private int zoom = 0;

    private HdfsImageResultScanner subject;

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
        zoom = 2;
        subject = createDefaultSubject(zoom,bounds);
        Assert.assertEquals(firstPartitionTileIds[0], subject.currentKey());
    }

    @Test
    @Category(UnitTest.class)
    public void testConstructionWithZoomOutOfLeftBound() throws Exception {
        zoom = 2;
        bounds = new LongRectangle(2, 0, 14, 14);
        subject = createDefaultSubject(2, bounds);
        Assert.assertTrue(subject.hasNext());
        assertIterateOverTiles(0,firstPartitionTileIds.length + secondPartitionTileIds.length - 1);
    }

    @Test
    @Category(UnitTest.class)
    public void testConstructionWithZoomOutOfRightBound() throws Exception {
        zoom = 4;
        bounds = new LongRectangle(0, 0, 8, 8);
        subject = createDefaultSubject(2, bounds);
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
                .values(firstPartitionRasters)
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

    private RasterWritable[] createRasters(int seed, int count) {
        RasterWritable[] rasters = new RasterWritable[count];
        for (int i = 0; i < count; i++) {
            ByteBuffer buffer = ByteBuffer.allocate(7 * 4);
            buffer.putInt(0); // header size
            buffer.putInt(1); // height
            buffer.putInt(1); // width
            buffer.putInt(1); // number of bands
            buffer.putInt(DataBuffer.TYPE_BYTE); // datatype
            buffer.putInt(1); // Model type = Banded
            buffer.putInt(i + seed); // data
            rasters[i] = new RasterWritable(buffer.array());
        }
        return rasters;
    }

    private void assertIterateOverTiles(int startIndex, int endIndex) throws IOException {
        logger.debug("Testing iteration over tiles, staritng with " + startIndex + " ending with " + endIndex + "...");
        for (int i = startIndex; i <= endIndex; i++) {
            if (i < firstPartitionTileIds.length) {
                if (zoom <= 0 || withinBounds(firstPartitionTileIds[i])) {
                    Assert.assertTrue(subject.hasNext());
                    assertTilesAreEqual(firstPartitionTileIds[i], subject.currentKey());
                    assertRastersAreEqual(subject.toNonWritable(firstPartitionRasters[i]), subject.currentValue());

                    // currentValue() and next() should return the same value
                    assertRastersAreEqual(subject.toNonWritable(firstPartitionRasters[i]), subject.next());
                }

            }
            else {
                int i2 = i - firstPartitionTileIds.length;
                if (zoom <= 0 || withinBounds(secondPartitionTileIds[i2])) {
                    Assert.assertTrue(subject.hasNext());
                    assertTilesAreEqual(secondPartitionTileIds[i2], subject.currentKey());
                    assertRastersAreEqual(subject.toNonWritable(secondPartitionRasters[i2]), subject.currentValue());

                    // currentValue() and next() should return the same value
                    assertRastersAreEqual(subject.toNonWritable(secondPartitionRasters[i2]), subject.next());
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

    private void assertRastersAreEqual(Raster raster1, Raster raster2) {
        Assert.assertEquals(raster1.getDataBuffer().getElem(0), raster2.getDataBuffer().getElem(0));
    }

}