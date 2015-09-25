/*
 * Copyright 2009-2015 DigitalGlobe, Inc.
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
 */

package org.mrgeo.hdfs.vector.shp.util;

public interface HashMapNListener
{
  void added(HashMapV map, Object value);

  void removed(HashMapV map);

  void reverseTraversed(HashMapV map, Object[] key);

  void reverseTraversedRoot(HashMapV map);

  void reverseTraversedStart(HashMapN map);

  void traversed(HashMapV map, Object key, int level);
}
