package org.mrgeo.hdfs.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.mrgeo.data.tile.TileIdWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Arrays;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by ericwood on 6/13/16.
 */
public class SequenceFileReaderBuilder {
    private static final Logger logger = LoggerFactory.getLogger(SequenceFileReaderBuilder.class);

    private SequenceFile.Reader sequenceFileReader;
    private Class keyClass;
    private Class valueClass;
    private Writable[] keys;
    private Writable[] values;

    public SequenceFileReaderBuilder() {
        this.sequenceFileReader = mock(SequenceFile.Reader.class);
    }

    public SequenceFileReaderBuilder keyClass(Class keyClass) {
        this.keyClass = keyClass;

        return this;
    }

    public SequenceFileReaderBuilder valueClass(Class valueClass) {
        this.valueClass = valueClass;

        return this;
    }

    public SequenceFileReaderBuilder keys(Writable[] keys) {
        this.keys = Arrays.copyOf(keys, keys.length);

        return this;
    }

    public SequenceFileReaderBuilder values(Writable[] values) {
        this.values = Arrays.copyOf(values, values.length);

        return this;
    }

    public SequenceFile.Reader build() throws IOException {
        when(sequenceFileReader.getKeyClass()).thenReturn(keyClass);
        when(sequenceFileReader.getValueClass()).thenReturn(valueClass);

        when(sequenceFileReader.next(any(Writable.class), any(Writable.class))).thenAnswer(new Answer<Boolean>() {
            private int index = 0;
            @Override
            public Boolean answer(InvocationOnMock invocationOnMock) throws Throwable {
                if (index >= keys.length) return false;

                // Get the key and value
                Object[] args = invocationOnMock.getArguments();
                Writable key = (Writable)args[0];
                Writable value = (Writable)args[1];
                copyData(keys[index], key);
                copyData(values[index], value);
                logger.info("Read: key: " + keys[index] + " value: " + values[index] + " Wrote: key: " + key + " value: " + value);
                ++index;
                return true;
            }

            private void copyData(Writable src, Writable tgt) throws IOException {
                PipedInputStream in = new PipedInputStream();
                PipedOutputStream out = new PipedOutputStream(in);
                DataOutputStream dos = new DataOutputStream(out);
                DataInputStream din = new DataInputStream(in);
                src.write(dos);
                tgt.readFields(din);
            }
        });

        return sequenceFileReader;
    }

}
