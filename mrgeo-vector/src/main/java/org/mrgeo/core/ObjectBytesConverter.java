/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */

package org.mrgeo.core;

import org.apache.hadoop.io.BytesWritable;

import java.io.*;

/**
 * This is a horribly slow way of serializing arbitrary Serializable objects in hadoop. It is 
 * very inefficient.
 */
public class ObjectBytesConverter
{
  public static BytesWritable toBytes(Object obj) throws IOException
  {
    ByteArrayOutputStream strm = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(strm);
    oos.writeObject(obj);
    oos.close();
    return new BytesWritable(strm.toByteArray());
  }

  public static Object toObject(BytesWritable bytes) throws IOException, ClassNotFoundException
  {
    ByteArrayInputStream bais = new ByteArrayInputStream(bytes.getBytes());
    ObjectInputStream ois = new ObjectInputStream(bais);
    return ois.readObject();
  }
}
