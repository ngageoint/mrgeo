/*
 * Copyright 2009-2014 DigitalGlobe, Inc.
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
