package org.mrgeo.hdfs.utils;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
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
public class KeyValueHelper {
    private static final Logger logger = LoggerFactory.getLogger(KeyValueHelper.class);

    private int index = 0;
    private Class keyClass;
    private Class valueClass;
    private Writable[] keys;
    private Writable[] values;

    public KeyValueHelper() {

    }

    public KeyValueHelper keyClass(Class keyClass) {
        this.keyClass = keyClass;

        return this;
    }

    public KeyValueHelper valueClass(Class valueClass) {
        this.valueClass = valueClass;

        return this;
    }

    public Class getKeyClass() {
        return keyClass;
    }

    public Class getValueClass() {
        return valueClass;
    }

    public KeyValueHelper keys(Writable[] keys) {
        this.keys = Arrays.copyOf(keys, keys.length);

        return this;
    }

    public KeyValueHelper values(Writable[] values) {
        this.values = Arrays.copyOf(values, values.length);

        return this;
    }

    public boolean next(Writable key, Writable value) throws IOException {
        if (index >= keys.length) return false;

        // Get the key and value
        copyData(keys[index], key);
        copyData(values[index], value);
        ++index;
        return true;
    }

    public Writable getClosest(WritableComparable key, Writable value) throws IOException {

        Writable foundKey = null;
        Writable foundValue = null;
        for (int i = 0; i < keys.length; i++) {
            int result = ((WritableComparable)keys[i]).compareTo(key);
            if (result == 0) {
                foundKey = keys[i];
                foundValue = values[i];
                break;
            }
            else if (result > 0 && i > 0) {
                foundKey = keys[i - 1];
                foundValue = values[i - 1];
                break;
            }
            else if (i == 0) {
                return null;
            }
        }
        if (foundKey != null) {
            copyData(foundKey, (Writable)key);
            copyData(foundValue, value);
            return foundKey;
        }
        else {
            return null;
        }
    }

    private void copyData(Writable src, Writable tgt) throws IOException {
        PipedInputStream in = new PipedInputStream();
        PipedOutputStream out = new PipedOutputStream(in);
        DataOutputStream dos = new DataOutputStream(out);
        DataInputStream din = new DataInputStream(in);
        src.write(dos);
        tgt.readFields(din);
    }



}
