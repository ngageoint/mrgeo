package org.mrgeo.data;

/**
 * This interface extends the capability of the KVIterator with the addition
 * of the Java CLoseable interface. This is useful for iterators that make
 * use of sources of data that need to be closed after they are used.
 * 
 * @param <K>
 * @param <V>
 */
public interface CloseableKVIterator<K, V> extends KVIterator<K, V>, java.io.Closeable
{
}
