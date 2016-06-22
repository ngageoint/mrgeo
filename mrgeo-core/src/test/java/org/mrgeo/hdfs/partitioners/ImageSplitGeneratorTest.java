package org.mrgeo.hdfs.partitioners;

import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.hdfs.tile.SplitInfo;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.utils.LongRectangle;

import static org.junit.Assert.*;

/**
 * Created by ericwood on 6/21/16.
 */
public class ImageSplitGeneratorTest {
    private ImageSplitGenerator subject;
    private LongRectangle bounds;
    private int zoomLevel;
    private int increment;

    @Before
    public void setUp() throws Exception {

    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    @Category(UnitTest.class)
    public void getSplitsPositiveIncrement() throws Exception {
        subject = createSubject(1);
        SplitInfo[] splits = subject.getSplits();
        Assert.assertEquals(bounds.getMaxY() - bounds.getMinY() + 1, splits.length);
    }

    @Test
    @Category(UnitTest.class)
    public void getSplitsZeroIncrement() throws Exception {
        subject = createSubject(0);
        SplitInfo[] splits = subject.getSplits();
        Assert.assertEquals(0, splits.length);
    }

    @Test
    @Category(UnitTest.class)
    public void getPartitionsPositiveIncrement() throws Exception {
        subject = createSubject(1);
        SplitInfo[] partitions = subject.getPartitions();
        Assert.assertEquals(bounds.getMaxY() - bounds.getMinY() + 1, partitions.length);
    }

    @Test
    @Category(UnitTest.class)
    public void getPartitionsZeroIncrement() throws Exception {
        subject = createSubject(0);
        SplitInfo[] partitions = subject.getPartitions();
        Assert.assertEquals(0, partitions.length);
    }

    private ImageSplitGenerator createSubject(int increment) {
        bounds = new LongRectangle(1, 0, 4, 2);
        zoomLevel = 2;
        subject = new ImageSplitGenerator(bounds.getMinX(), bounds.getMinY(), bounds.getMaxX(), bounds.getMaxY(),
                zoomLevel, increment);
        return subject;
    }

}