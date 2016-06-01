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

package org.mrgeo.hdfs.vector.shp.util;

import java.io.*;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

public class ObjectUtils extends java.lang.Object
{

public static String debug(Object[] key)
{
  if (key == null)
    return null;
  if (key.length == 0)
    return "";
  StringBuffer buffer = new StringBuffer();
  for (int i = 0; i < key.length - 1; i++)
  {
    buffer.append(key[i]);
    buffer.append(",");
  }
  buffer.append(key[key.length - 1]);
  return buffer.toString();
}

public static void main(String[] args)
{
  try
  {
    if (args.length < 1)
    {
      System.out.println("USAGE: ObjectUtils <objectfile> {compressed}");
      System.exit(1);
    }
    String file = args[0];
    boolean compressed = false;
    if (args.length == 2)
      compressed = Boolean.parseBoolean(args[1]);
    readObjectFile(file, compressed);
    System.out.println("OK.");
  }
  catch (Exception e)
  {
    System.out.println("ERROR!");
    e.printStackTrace();
  }
}

public static Object[] pack(Object obj1)
{
  Object[] p = new Object[1];
  p[0] = obj1;
  return p;
}

public static Object[] pack(Object obj1, Object obj2)
{
  Object[] p = new Object[2];
  p[0] = obj1;
  p[1] = obj2;
  return p;
}

public static Object[] pack(Object obj1, Object obj2, Object obj3)
{
  Object[] p = new Object[3];
  p[0] = obj1;
  p[1] = obj2;
  p[2] = obj3;
  return p;
}

public static Object[] pack(Object obj1, Object obj2, Object obj3, Object obj4)
{
  Object[] p = new Object[4];
  p[0] = obj1;
  p[1] = obj2;
  p[2] = obj3;
  p[3] = obj4;
  return p;
}

public static Object[] pack(Object obj1, Object obj2, Object obj3, Object obj4, Object obj5)
{
  Object[] p = new Object[5];
  p[0] = obj1;
  p[1] = obj2;
  p[2] = obj3;
  p[3] = obj4;
  p[4] = obj5;
  return p;
}

public static Serializable readObjectFile(String fileName, boolean compressed) throws IOException
{
  Serializable obj = null;

  FileInputStream fis = new FileInputStream(fileName);
  BufferedInputStream bis = null;
  InflaterInputStream iis = null;
  if (compressed)
  {
    iis = new InflaterInputStream(fis);
    bis = new BufferedInputStream(iis);
  }
  else
  {
    bis = new BufferedInputStream(fis);
  }
  try (ObjectInputStream ois = new ObjectInputStream(bis))
  {
    obj = (Serializable) ois.readObject();
  }
  catch (ClassNotFoundException e)
  {
    throw new IOException();
  }


  return obj;
}

public static void writeObjectFile(Serializable obj, String fileName, boolean compress)
    throws IOException
{
  FileOutputStream fos = new FileOutputStream(fileName);
  BufferedOutputStream bos = null;
  DeflaterOutputStream dos = null;
  if (compress)
  {
    dos = new DeflaterOutputStream(fos);
    bos = new BufferedOutputStream(dos);
  }
  else
  {
    bos = new BufferedOutputStream(fos);
  }
  try (ObjectOutputStream oos = new ObjectOutputStream(bos))
  {
    oos.writeObject(obj);
  }

  if (dos != null)
    dos.close();
  bos.close();
  fos.close();
}
}
