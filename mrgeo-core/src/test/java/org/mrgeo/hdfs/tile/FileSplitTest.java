package org.mrgeo.hdfs.tile;

import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.hdfs.utils.*;
import org.mrgeo.junit.UnitTest;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Scanner;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@SuppressWarnings("all") // test code, not included in production
public class FileSplitTest {
    private FileSplit subject;

    @Before
    public void setUp() throws Exception {

    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    @Category(UnitTest.class)
    public void testenerateSplitsFromSplits() throws Exception {
        subject = new FileSplit();
        FileSplit.FileSplitInfo[] fileSplits = new FileSplit.FileSplitInfo[2];
        subject.generateSplits(fileSplits);
        Assert.assertEquals(fileSplits.length, subject.getSplits().length);

    }

    @Test
    @Category(UnitTest.class)
    public void testGenerateSplitsFromStartAndEndIds() throws Exception {
        subject = new FileSplit();
        long[] startIds = {1L, 10L, 20L};
        long[] endIds = {5L, 14L, 23L};
        String[] names = {"split1", "split2", "split3"};
        subject.generateSplits(startIds, endIds, names);
        SplitInfo[] fileSplits = subject.getSplits();
        for (int i = 0; i < startIds.length; i++) {
            Assert.assertEquals(endIds[i], fileSplits[i].getTileId());
            Assert.assertEquals(i, fileSplits[i].getPartition());
        }
    }

    @Test
    @Category(UnitTest.class)
    public void testGenerateSplitsFromPath() throws Exception {
        // Setup a mock directory structure
        Path rootPath = new Path(FileSplitTest.class.getName() + "-testRootPath");
        Path path1 = new Path(rootPath, FileSplitTest.class.getName() + "-testPath1");
        Path path2 = new Path(rootPath, FileSplitTest.class.getName() + "-testPath2");
        Path path3 = new Path(rootPath, FileSplitTest.class.getName() + "-testPath3");
        Path path1_1 = new Path(path1, "notDataDir");
        Path path1_2 = new Path(path1, "data");
        Path path2_1 = new Path(path2, "data");
        Path path3_1 = new Path(path3, "notDataDir");

        // Setup the FileSystem
        FileSystem mockFS = new FileSystemBuilder()
                .fileStatus(rootPath, new FileStatusBuilder().path(path1).build())
                .fileStatus(rootPath, new FileStatusBuilder().path(path2).build())
                .fileStatus(rootPath, new FileStatusBuilder().path(path3).build())
                .fileStatus(path1, new FileStatusBuilder().path(path1_1).build())
                .fileStatus(path1, new FileStatusBuilder().path(path1_2).build())
                .fileStatus(path2, new FileStatusBuilder().path(path2_1).build())
                .fileStatus(path3, new FileStatusBuilder().path(path3_1).build())
        .build();

        // setup map file readers for each of the data directories
        RasterWritable mockValue = new RasterWritable();
        TileIdWritable[] path1Keys = {new TileIdWritable(2L), new TileIdWritable(4L), new TileIdWritable(6L)};
        RasterWritable[] path1Values = {mockValue, mockValue, mockValue};
        TileIdWritable[] path2Keys = {new TileIdWritable(5L), new TileIdWritable(6L), new TileIdWritable(7L)};
        RasterWritable[] path2Values = {mockValue, mockValue, mockValue};
        MapFile.Reader mockMapFileReaderPath1 = new MapFileReaderBuilder()
                .keyClass(TileIdWritable.class)
                .valueClass(RasterWritable.class)
                .keys(path1Keys)
                .values(path1Values)
                .build();

        MapFile.Reader mockMapFileReaderPath2 = new MapFileReaderBuilder()
                .keyClass(TileIdWritable.class)
                .valueClass(RasterWritable.class)
                .keys(path2Keys)
                .values(path2Values)
                .build();

        // Setup a Configuration
        Configuration mockConfiguration = new ConfigurationBuilder().build();


        FileSplit spySubject = new FileSplit();
        subject = spy(spySubject);
        doReturn(mockFS).when(subject).getFileSystem(rootPath);
        doReturn(mockMapFileReaderPath1).when(subject).createMapFileReader(mockConfiguration, path1);
        doReturn(mockMapFileReaderPath2).when(subject).createMapFileReader(mockConfiguration, path2);
        subject.generateSplits(rootPath, mockConfiguration);

        // Verify we got splits for path 1 and 2
        SplitInfo[] splits = subject.getSplits();
        Assert.assertEquals(2, splits.length);
        verifySplit(path1, path1Keys, splits, 0);
        verifySplit(path2, path2Keys, splits, 1);
    }

    @Test
    @Category(UnitTest.class)
    public void testFindSplitFileNewVersion() throws Exception {
        Path parent = new Path(FileSplitTest.class.getName() + "-testRootPath");
        Path splitsFile = new Path(parent, "splits");
        FileSplit spySubject = new FileSplit();
        subject = spy(spySubject);
        doReturn(true).when(subject).fileExists(splitsFile);

        Assert.assertEquals(splitsFile.toString(), subject.findSplitFile(parent).toString());
    }

    @Test
    @Category(UnitTest.class)
    public void testFindSplitFileOldVersion() throws Exception {
        Path parent = new Path(FileSplitTest.class.getName() + "-testRootPath");
        Path newSplitsFile = new Path(parent, "splits");
        Path oldSplitsFile = new Path(parent, "splits.txt");
        FileSplit spySubject = new FileSplit();
        subject = spy(spySubject);
        doReturn(false).when(subject).fileExists(newSplitsFile);
        doReturn(true).when(subject).fileExists(oldSplitsFile);
        Assert.assertEquals(oldSplitsFile.toString(), subject.findSplitFile(parent).toString());
    }

    @Test
    @Category(UnitTest.class)
    public void testFindSplitFileNotFound() throws Exception {
        Path parent = null;
        Path oldSplitsFile = null;
        try {
            parent = new Path(FileSplitTest.class.getName() + "-testRootPath");
            Path newSplitsFile = new Path(parent, "splits");
            oldSplitsFile = new Path(parent, "splits.txt");
            FileSplit spySubject = new FileSplit();
            subject = spy(spySubject);
            doReturn(false).when(subject).fileExists(newSplitsFile);
            doReturn(false).when(subject).fileExists(oldSplitsFile);
            String result = subject.findSplitFile(parent);
            fail("findSplitFile did not throw IOException when split file not found.  Returned " + result);
        }
        catch (IllegalArgumentException e) {
            e.printStackTrace();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    @Category(UnitTest.class)
    public void testReadSplitsVersion3() throws Exception {
        String version = "v3";
        int count = 3;
        FileSplit.FileSplitInfo split1 = new FileSplit.FileSplitInfo(1, 2, "split1", 0);
        FileSplit.FileSplitInfo split2 = new FileSplit.FileSplitInfo(6, 4, "split2", 1);
        FileSplit.FileSplitInfo split3 = new FileSplit.FileSplitInfo(7, 9, "split3", 2);

        StringBuffer sb = new StringBuffer();
        sb.append(version);
        sb.append("\n");
        sb.append(count);
        sb.append("\n");
        appendSplit(split1, sb);
        sb.append("\n");
        appendSplit(split2, sb);
        sb.append("\n");
        appendSplit(split3, sb);
        ByteArrayInputStream in = new ByteArrayInputStream(sb.toString().getBytes());
        subject = new FileSplit();
        subject.readSplits(in);
        SplitInfo[] splits = subject.getSplits();
        Assert.assertEquals(2, splits.length);
        verifySplit(split1, splits, 0);
        verifySplit(split3, splits, 1);
    }

    @Test
    @Category(UnitTest.class)
    public void testReadSplitsByStreamVersion2() {
        ByteArrayInputStream in = createVersion2InputStream();
        subject = new FileSplit();
        try {
            subject.readSplits(in);
            Assert.fail("Did not throw exception when reading version 2 split file");
        }
        catch (Splits.SplitException e) {
            e.printStackTrace();
        }
    }

    @Test
    @Category(UnitTest.class)
    public void testReadSplitsByStreamNotVersion2() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(8);
        byteBuffer.putLong(-2468);
        byte[] base64Bytes = org.apache.commons.codec.binary.Base64.encodeBase64(byteBuffer.array());
        ByteArrayInputStream in = new ByteArrayInputStream(base64Bytes);
        subject = new FileSplit();
        try {
            subject.readSplits(in);
            Assert.fail("Did not throw exception when reading non version 2 split file");
        }
        catch (Splits.SplitException e) {
            e.printStackTrace();
        }
    }

    @Test
    @Category(UnitTest.class)
    public void testIsVersion2FileNotExist() throws IOException {
        Path testPath = new Path(FileSplitTest.class.getName() + "-testPath");
        FileSplit spySubject = new FileSplit();
        subject = spy(spySubject);
        doReturn(false).when(subject).fileExists(any(Path.class));

        Assert.assertTrue(subject.isVersion2(testPath));
    }


    @Test
    @Category(UnitTest.class)
    // Cannot test this case as the code currently exists due to the superclass call
    public void readSplitsByPathVersion2() throws Exception {
        // Setup a mock directory structure
        Path parent = new Path(FileSplitTest.class.getName() + "-testParent");
        Path splitsFile = new Path(parent, "splits");

        // Setup an InputStream
        ByteArrayInputStream in = createVersion2InputStream();

        // Setup the FileSystem
        FileSystem mockFS = new FileSystemBuilder()
                .inputStream(splitsFile, in)
                .build();

        FileSplit spySubject = new FileSplit();
        subject = spy(spySubject);
        doReturn(mockFS).when(subject).getFileSystem(any(Path.class));
        doReturn(true).when(subject).fileExists(any(Path.class));

        // Prevent calls from the method
        doNothing().when(subject).generateSplits(any(Path.class),any(Configuration.class));
        doNothing().when(subject).writeSplits(any(Path.class));
        doNothing().when(subject).readSplits(any(InputStream.class));

        subject.readSplits(parent);
        verify(subject, atLeastOnce()).generateSplits(any(Path.class),any(Configuration.class));
        verify(subject, atLeastOnce()).writeSplits(any(Path.class));
    }

    @Test
    @Category(UnitTest.class)
    public void testWriteSplitsNoSplitsGenerated() throws Exception {
        try {
            subject = new FileSplit();
            subject.writeSplits(new ByteArrayOutputStream());
            fail("Did not throw expection when trying to write splits with generating them first");
        }
        catch (Splits.SplitException e) {
            e.printStackTrace();
        }
    }

    @Test
    @Category(UnitTest.class)
    public void testWriteSplits() throws Exception {
        subject = new FileSplit();
        long[] startIds = {1L, 10L, 20L};
        long[] endIds = {5L, 14L, 23L};
        String[] names = {"split1", "split2", "split3"};
        subject.generateSplits(startIds, endIds, names);
        Path parent = new Path(FileSplitTest.class.getName() + "-testParent");
        Path splitsFile = new Path(parent, "splits");
        PipedOutputStream out = new PipedOutputStream();
        PipedInputStream in = new PipedInputStream(out);

        // Setup the FileSystem
        FileSystem mockFS = new FileSystemBuilder()
                .outputStream(splitsFile, out)
                .build();

        FileSplit spySubject = new FileSplit();
        subject = spy(spySubject);
        doReturn(mockFS).when(subject).getFileSystem(any(Path.class));
        subject.generateSplits(startIds, endIds, names);
        subject.writeSplits(parent);
        Scanner reader = new Scanner(in);
        Assert.assertEquals("v3", reader.nextLine());
        Assert.assertEquals(startIds.length, Integer.parseInt(reader.nextLine()));
        for (int i = 0; i < startIds.length; ++i) {
            Assert.assertEquals(startIds[i], reader.nextLong());
            Assert.assertEquals(endIds[i], reader.nextLong());
            Assert.assertEquals(names[i], reader.next());
            Assert.assertEquals(i, reader.nextInt());
        }
    }


    protected void verifySplit(Path path, TileIdWritable[] keys, SplitInfo[] splits, int index) {
        FileSplit.FileSplitInfo split = (FileSplit.FileSplitInfo) splits[index];
        Assert.assertEquals(keys[0].get(), split.getStartId());
        Assert.assertEquals(keys[keys.length - 1].get(), split.getEndId());
        Assert.assertEquals(path.getName(), split.getName());
        Assert.assertEquals(index, split.getPartition());
    }

    protected void verifySplit(FileSplit.FileSplitInfo expectedSplit, SplitInfo[]splits, int index) {
        FileSplit.FileSplitInfo split = (FileSplit.FileSplitInfo) splits[index];
        Assert.assertEquals(expectedSplit.getStartId(), split.getStartId());
        Assert.assertEquals(expectedSplit.getEndId(), split.getEndId());
        Assert.assertEquals(expectedSplit.getName(), split.getName());
        Assert.assertEquals(expectedSplit.getPartition(), split.getPartition());
    }

    protected void appendSplit(FileSplit.FileSplitInfo split, StringBuffer sb) {
        sb.append(split.getStartId());
        sb.append(" ");
        sb.append(split.getEndId());
        sb.append(" ");
        sb.append(split.getName());
        sb.append(" ");
        sb.append(split.getPartition());
    }

    protected ByteArrayInputStream createVersion2InputStream() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(8);
        byteBuffer.putLong(-12345);
        byte[] base64Bytes = org.apache.commons.codec.binary.Base64.encodeBase64(byteBuffer.array());
        return new ByteArrayInputStream(base64Bytes);
    }


}