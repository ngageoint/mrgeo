package org.mrgeo.hdfs.utils;

import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by ericwood on 6/13/16.
 */
public class SequenceFileReaderBuilder {
    private static final Logger logger = LoggerFactory.getLogger(SequenceFileReaderBuilder.class);

    private SequenceFile.Reader sequenceFileReader;
    private KeyValueHelper keyValueHelper;

    public SequenceFileReaderBuilder() {
        this.sequenceFileReader = mock(SequenceFile.Reader.class);
        keyValueHelper = new KeyValueHelper();
    }

    public SequenceFileReaderBuilder keyClass(Class keyClass) {
        keyValueHelper.keyClass(keyClass);

        return this;
    }

    public SequenceFileReaderBuilder valueClass(Class valueClass) {
        keyValueHelper.valueClass(valueClass);

        return this;
    }

    public SequenceFileReaderBuilder keys(Writable[] keys) {
        keyValueHelper.keys(keys);

        return this;
    }

    public SequenceFileReaderBuilder values(Writable[] values) {
        keyValueHelper.values(values);

        return this;
    }

    public SequenceFile.Reader build() throws IOException {
        when(sequenceFileReader.getKeyClass()).thenReturn(keyValueHelper.getKeyClass());
        when(sequenceFileReader.getValueClass()).thenReturn(keyValueHelper.getValueClass());

        when(sequenceFileReader.next(any(Writable.class), any(Writable.class))).thenAnswer(new Answer<Boolean>() {
            @Override
            public Boolean answer(InvocationOnMock invocationOnMock) throws Throwable {
                // Get the key and value
                Object[] args = invocationOnMock.getArguments();
                Writable key = (Writable)args[0];
                Writable value = (Writable)args[1];
                return keyValueHelper.next(key, value);
            }
        });

        return sequenceFileReader;
    }

}
