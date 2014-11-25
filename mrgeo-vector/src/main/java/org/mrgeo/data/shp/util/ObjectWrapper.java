/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.data.shp.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class ObjectWrapper extends java.lang.Object implements java.io.Serializable
{
  private static final long serialVersionUID = 1L;

  public static ObjectWrapper unwrap(byte[] b) throws Exception
  {
    // capture object through deserialization
    ByteArrayInputStream bais = new ByteArrayInputStream(b);
    ObjectInputStream ois = new ObjectInputStream(bais);
    ObjectWrapper wrapper = (ObjectWrapper) ois.readObject();
    ois.close();
    bais.close();

    // store bytes
    wrapper.bytes = b;

    // return
    return wrapper;
  }

  public static ObjectWrapper wrap(Object obj) throws Exception
  {
    // create new wrapper
    ObjectWrapper wrapper = new ObjectWrapper();
    wrapper.obj = obj;

    // capture bytes through serialization
    ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(wrapper);
    oos.flush();
    oos.close();
    baos.close();

    // store bytes
    wrapper.bytes = baos.toByteArray();

    // return
    return wrapper;
  }

  private transient byte[] bytes;

  private Object obj;

  private ObjectWrapper()
  {
  }
  
  public byte[] getBytes()
  {
    return bytes;
  }

  public Object getObject()
  {
    return obj;
  }

  @Override
  public String toString()
  {
    if (obj != null)
    {
      return obj.getClass().getName();
    }
    return "null";
  }
}
