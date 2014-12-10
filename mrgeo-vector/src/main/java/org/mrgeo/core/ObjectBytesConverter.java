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
