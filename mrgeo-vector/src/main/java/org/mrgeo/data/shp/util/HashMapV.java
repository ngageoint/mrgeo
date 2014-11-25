/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.data.shp.util;

import java.util.HashMap;

@SuppressWarnings("rawtypes")
public class HashMapV extends HashMap
{
  private static final long serialVersionUID = 1L;
  public Object value = null;

  public HashMapV()
  {
    super();
  }

  public HashMapV(int size)
  {
    super(size);
  }

  public Object getValue()
  {
    return value;
  }

  public void setValue(Object value)
  {
    this.value = value;
  }
}
