package org.mrgeo.hdfs.output.image;

import junit.framework.Assert;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.hdfs.utils.ConfigurationBuilder;
import org.mrgeo.hdfs.utils.TaskAttemptContextBuilder;
import org.mrgeo.junit.UnitTest;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * Created by ericwood on 6/20/16.
 */
public class HdfsMrsPyramidOutputFormatTest {
    private HdfsMrsPyramidOutputFormat subject;
    private TaskAttemptContext mockContext;
    private String outputPathString = HdfsMrsPyramidOutputFormatTest.class.getName() + "-testOutputPath";
    private CompressionCodec defaultCodec;
    private SequenceFile.CompressionType defaultCompressionType;
    private MapFile.Writer mockWriter;
    private Path outputPath;

    @Before
    public void setup() throws Exception {
        defaultCodec = DefaultCodec.class.newInstance();
        defaultCompressionType = SequenceFile.CompressionType.BLOCK;
        mockWriter = mock(MapFile.Writer.class);
        outputPath = new Path(outputPathString);
    }

    @Test
    @Category(UnitTest.class)
    public void testGetRecordWriterWithCompression() throws Exception {
        subject = createSubject(true);
        RecordWriter writer = subject.getRecordWriter(mockContext);
        verify(subject, atLeastOnce()).createMapFileWriter(mockContext, defaultCodec, defaultCompressionType, outputPath);
    }

    @Test
    @Category(UnitTest.class)
    public void testGetRecordWriterWithoutCompression() throws Exception {
        subject = createSubject(false);
        RecordWriter writer = subject.getRecordWriter(mockContext);
        verify(subject, never()).getCompressionCodec(mockContext);
        verify(subject, atLeastOnce()).createMapFileWriter(mockContext, null, SequenceFile.CompressionType.NONE, outputPath);
    }

    @Test
    @Category(UnitTest.class)
    public void testWriteTileIdWritableKey() throws Exception {
        subject = createSubject(false);
        RecordWriter writer = subject.getRecordWriter(mockContext);
        TileIdWritable tileId = new TileIdWritable(1L);
        RasterWritable raster = new RasterWritable();
        ArgumentCaptor keyParamCaptor = ArgumentCaptor.forClass(TileIdWritable.class);
        ArgumentCaptor valueParamCaptor = ArgumentCaptor.forClass(RasterWritable.class);
        writer.write(tileId, raster);
        verify(mockWriter, atLeastOnce()).append((TileIdWritable) keyParamCaptor.capture(),
                                                 (RasterWritable)valueParamCaptor.capture());
        Assert.assertEquals(tileId.get(), ((TileIdWritable)keyParamCaptor.getValue()).get());
    }

    @Test
    @Category(UnitTest.class)
    public void testWriteNonTileIdWritableKey() throws Exception {
        subject = createSubject(false);
        RecordWriter writer = subject.getRecordWriter(mockContext);
        WritableComparable testKey =  mock(WritableComparable.class);
        Writable testValue = mock(Writable.class);
        writer.write(testKey, testValue);
        verify(mockWriter, atLeastOnce()).append(testKey, testValue);
    }

    private HdfsMrsPyramidOutputFormat createSubject(boolean compressOutput) throws Exception {
        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        if (compressOutput) {
            configurationBuilder.compressOutput(true)
                                .outputCompressionType(defaultCompressionType.toString());
        }
        mockContext = new TaskAttemptContextBuilder()
                .configuration(configurationBuilder.build())
                .outputKeyClass(TileIdWritable.class)
                .outputValueClass(RasterWritable.class)
                .build();


        HdfsMrsPyramidOutputFormat spySubject = new HdfsMrsPyramidOutputFormat();
        HdfsMrsPyramidOutputFormat subject = spy(spySubject);
        doReturn(outputPath).when(subject).getDefaultWorkFile(mockContext, "");
        doReturn(defaultCodec).when(subject).getCompressionCodec(mockContext);
        doReturn(mockWriter).when(subject).createMapFileWriter(any(TaskAttemptContext.class),
                any(CompressionCodec.class),
                any(SequenceFile.CompressionType.class),
                any(Path.class));
        return subject;
    }

    private static class ExtendedTileIdWritable extends TileIdWritable {

    }

}