/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.data.shp.util;

public interface HashMapNListener
{
  void added(HashMapV map, Object value);

  void removed(HashMapV map);

  void reverseTraversed(HashMapV map, Object[] key);

  void reverseTraversedRoot(HashMapV map);

  void reverseTraversedStart(HashMapN map);

  void traversed(HashMapV map, Object key, int level);
}
