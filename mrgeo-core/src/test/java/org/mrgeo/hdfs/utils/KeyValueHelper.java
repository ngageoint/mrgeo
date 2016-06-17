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

    // Index of current key
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

        logger.debug("Reading next key...");
        // Get the key and value
        copyData(keys[index], key);
        copyData(values[index], value);
        ++index;
        return true;
    }

    /**
     *
     * Find the first key larger than the specified key.
     *
     * The index will be positioned at this key, such that the next call to next() will return the key after the found key.
     * @param key The key to find
     * @param value The value of the closest key
     * @return the key that was the closest match
     * @throws IOException
     */
    public Writable getClosest(WritableComparable key, Writable value) throws IOException {
        logger.debug("getClosest called with key " + key);
        Writable foundKey = null;
        Writable foundValue = null;
        int keyIndex = -1;
        for (int i = 0; i < keys.length; i++) {
            int result = ((WritableComparable)keys[i]).compareTo(key);
            logger.debug("Result of comparing key " + keys[i] + " with key " + key + " = " + result);
            if (result >= 0) {
                foundKey = keys[i];
                foundValue = values[i];
                keyIndex = i;
                break;
            }
        }
        if (foundKey != null) {
            copyData(foundValue, value);
            // Update the index.
            index = keyIndex;
            logger.debug("Found key " + foundKey);
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
