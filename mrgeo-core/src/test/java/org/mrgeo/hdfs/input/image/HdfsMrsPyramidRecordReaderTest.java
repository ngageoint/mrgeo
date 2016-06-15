package org.mrgeo.hdfs.input.image;

import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.hdfs.utils.*;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.mapreduce.splitters.TiledInputSplit;

import java.io.IOException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Created by ericwood on 6/10/16.
 */
public class HdfsMrsPyramidRecordReaderTest {



    private TaskAttemptContext mockContext;
    private SequenceFile.Reader mockReader;
    private Configuration mockConfig;
    private TiledInputSplit mockTiledInputSplit;
    private HdfsMrsPyramidRecordReader subject;
    private TileIdWritable mockTileId;
    private RasterWritable mockRaster;
    private FileSplit mockFileSplit;
    private Path mockPath;
    private TileIdWritable[] tileIds = {new TileIdWritable(1L), new TileIdWritable(2L), new TileIdWritable(3L)};
    private RasterWritable[] rasters = {new RasterWritable("AB".getBytes()),
                                        new RasterWritable("CD".getBytes()),
                                        new RasterWritable("EF".getBytes())};


    @Before
    public void setUp() throws Exception {
        // Create Mocks
        mockContext = new TaskAttemptContextBuilder().configuration(
                new ConfigurationBuilder().build()
        ).build();

        mockTiledInputSplit = new TiledInputSplitBuilder().wrappedSplit(
                new FileSplitBuilder().path(
                        new PathBuilder().fileSystem(
                                new FileSystemBuilder().build())
                        .build())
                .build())
        .build();

        mockReader = new SequenceFileReaderBuilder()
                .keyClass(TileIdWritable.class)
                .valueClass(RasterWritable.class)
                .keys(tileIds)
                .values(rasters)
        .build();

//        mockTiledInputSplit = mock(TiledInputSplit.class);
//        mockContext = mock(TaskAttemptContext.class);
//        mockConfig = mock(Configuration.class);
//        mockTileId = mock(TileIdWritable.class);
//        mockRaster = mock(RasterWritable.class);
//        mockFileSplit = mock(FileSplit.class);
//        mockPath = mock(Path.class);
//        mockFileSystem = mock(FileSystem.class);

        // Instance under test
        subject = new HdfsMrsPyramidRecordReader(new HdfsMrsPyramidRecordReader.ReaderFactory() {
            public SequenceFile.Reader createReader(FileSystem fs, Path path, Configuration config) {
                return mockReader;
            }
        });

        subject.initialize(mockTiledInputSplit, mockContext);

    }

    private void setupTiledInputSplit(long startTileId, long endTileId) {
        when(mockTiledInputSplit.getStartTileId()).thenReturn(startTileId);
        when(mockTiledInputSplit.getEndTileId()).thenReturn(endTileId);
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    @Category(UnitTest.class)
    public void getProgress() throws Exception {

    }

    @Test
    @Category(UnitTest.class)
    public void close() throws Exception {

    }

    @Test
    @Category(UnitTest.class)
    public void testInitializeWithNonTiledInputSplit() throws Exception {
        InputSplit mockSplit = mock(InputSplit.class);
        try {
            subject.initialize(mockSplit, mockContext);
            Assert.fail("initialize did not throw an exception if the input split was not a TiledInputSplit");
        }
        catch (IOException e) {
            // Passed so do nothing
        }
    }



//    @Test
//    @Category(UnitTest.class)
//    public void testInitialize() throws Exception {
//        subject.initialize(mockTiledInputSplit, mockContext);
//
//        // verify Key and Value
//
//
//    }

    @Test
    @Category(UnitTest.class)
    public void testNextKeyValue() throws Exception {

        for (int i = 0; i < tileIds.length; i++) {
            Assert.assertTrue("more key values exist", subject.nextKeyValue());
            Assert.assertEquals(subject.getCurrentKey(), tileIds[i]);
            Assert.assertEquals(subject.getCurrentValue(), rasters[i]);
        }
        Assert.assertFalse("no more key values exist", subject.nextKeyValue());
    }

}