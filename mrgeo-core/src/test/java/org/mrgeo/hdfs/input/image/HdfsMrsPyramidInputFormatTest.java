package org.mrgeo.hdfs.input.image;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.hdfs.image.HdfsMrsImageDataProvider;
import org.mrgeo.hdfs.tile.FileSplit;
import org.mrgeo.hdfs.utils.*;
import org.mrgeo.image.MrsPyramidMetadata;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.mapreduce.splitters.TiledInputSplit;
import org.mrgeo.utils.tms.Bounds;

import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

@SuppressWarnings("all") // test code, not included in production
public class HdfsMrsPyramidInputFormatTest {
    private HdfsMrsPyramidInputFormat subject;
    private HdfsMrsImageDataProvider mockImageDataProvider;
    private FileSplit mockFileSplit;
    private String resourcePathString = HdfsMrsPyramidInputFormatTest.class.getName() + "-testResourcePath";
    private String imageString = HdfsMrsPyramidInputFormatTest.class.getName() + "-testImage";
    private int zoomLevel = 2;

//private FileSplit.FileSplitInfo[] splits = {new FileSplit.FileSplitInfo(1L, 3L, "split1", 0),
//    new FileSplit.FileSplitInfo(5L, 7L, "split2", 1)};
private FileSplit.FileSplitInfo[] splits = {new FileSplit.FileSplitInfo(1L, 3L, "split1", 0)};
    @Before
    public void setUp() throws Exception {
        HdfsMrsPyramidInputFormat spySubject = new HdfsMrsPyramidInputFormat();
        subject = spy(spySubject);

        // Can use real object because it has no external dependencies
        MrsPyramidMetadata pyramidMetadata = new MrsPyramidMetadata();
        MrsPyramidMetadata.ImageMetadata[] imageData = new MrsPyramidMetadata.ImageMetadata[zoomLevel + 1];
        imageData[zoomLevel] = new MrsPyramidMetadata.ImageMetadata();
        imageData[zoomLevel].setImage(imageString + zoomLevel);
        pyramidMetadata.setImageMetadata(imageData);
        Path resourcePath = new Path(resourcePathString);
        mockImageDataProvider = new HdfsMrsImageDataProviderBuilder()
                .pyramidMetadata(pyramidMetadata)
                .resourcePath(resourcePath)
                .build();

//        mockFileSplit = new MrGeoFileSplitBuilder()
//            .split(splits[0])
//            .split(splits[1])
//            .build();
        mockFileSplit = new MrGeoFileSplitBuilder()
            .split(splits[0])
            .build();

        // When spying, need to make sure all mocks are setup before calling doReturn
        prepareSubject();
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void createRecordReader() throws Exception {

    }

    @Test
    @Category(UnitTest.class)
    public void testGetZoomName() throws Exception {
        String returnPath = HdfsMrsPyramidInputFormat.getZoomName(mockImageDataProvider, zoomLevel);
        Assert.assertTrue(returnPath.contains(resourcePathString));
        Assert.assertTrue(returnPath.contains(imageString));
        Assert.assertTrue(returnPath.endsWith(String.valueOf(zoomLevel)));
    }

    @Test
    @Category(UnitTest.class)
    public void getSplitsNoBounds() throws Exception {
        TaskAttemptContext mockContext = createDefaultTaskAttemptContext();
        List<InputSplit> result = subject.getSplits(mockContext);
        Assert.assertEquals(1, result.size());
        verifySplitsAreEqual(result);
    }

    @Test
    @Category(UnitTest.class)
    public void getSplitsWithBoundsNoIntersection() throws Exception {
        Bounds bounds = new Bounds(-180, 10, -100, 90);
        TaskAttemptContext mockContext = createDefaultTaskAttemptContext(bounds);
        List<InputSplit> result = subject.getSplits(mockContext);
        Assert.assertEquals(0, result.size());
    }

    @Test
    @Category(UnitTest.class)
    public void getSplitsWithBoundsIntersectFirstSplit() throws Exception {
        Bounds bounds = new Bounds(-180, -90, 180, 0);
        TaskAttemptContext mockContext = createDefaultTaskAttemptContext(bounds);
        List<InputSplit> result = subject.getSplits(mockContext);
        Assert.assertEquals(1, result.size());
        verifySplitsAreEqual(0, 0, result);
    }

    @Test
    public void setInputInfo() throws Exception {

    }

    private void prepareSubject() {
        doReturn(mockImageDataProvider).when(subject).createHdfsMrsImageDataProvider(any(Configuration.class));
        doReturn(mockFileSplit).when(subject).createFileSplit();
    }

    private TaskAttemptContext createDefaultTaskAttemptContext() throws Exception {
        return createDefaultTaskAttemptContext(null);
    }

    private TaskAttemptContext createDefaultTaskAttemptContext(Bounds bounds) throws Exception {
        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder()
                .zoomLevel(2)
                .tileSize(512);
        if (bounds != null) {
            configurationBuilder.boundsString(bounds.toCommaString());
        }
        return new TaskAttemptContextBuilder()
                .configuration(configurationBuilder.build())
        .build();
    }

    private void verifySplitsAreEqual(List<InputSplit> result) {
        verifySplitsAreEqual(0, 0, result);
    }

    private void verifySplitsAreEqual(int partitionIndex, int resultIndex, List<InputSplit> result) {
        TiledInputSplit split = (TiledInputSplit) result.get(resultIndex);
        org.apache.hadoop.mapreduce.lib.input.FileSplit wrappedSplit =
                (org.apache.hadoop.mapreduce.lib.input.FileSplit)split.getWrappedSplit();
        Assert.assertEquals(split.getZoomLevel(),zoomLevel);
        Assert.assertEquals(split.getStartTileId(), splits[partitionIndex].getStartId());
        Assert.assertEquals(split.getEndTileId(), splits[partitionIndex].getEndId());
        Assert.assertTrue(wrappedSplit.getPath().toString().contains(splits[partitionIndex].getName()));
    }

}