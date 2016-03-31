/*
 * Copyright 2009-2016 DigitalGlobe, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 *
 */

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
