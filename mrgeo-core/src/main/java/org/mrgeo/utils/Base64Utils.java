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

package org.mrgeo.utils;

import org.apache.commons.codec.binary.Base64;

import java.io.*;

public class Base64Utils
{

  public static String encodeObject(Object obj) throws IOException
  {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = null;
    byte[] rawBytes;
    try
    {
      oos = new ObjectOutputStream( baos );
      oos.writeObject( obj );
      rawBytes = baos.toByteArray();
    }
    catch (IOException e)
    {
      throw e;
    }
    finally
    {
      if (oos != null)
      {
        oos.close();
      }
      baos.close();
    }

    return new String(Base64.encodeBase64(rawBytes));
  }

  public static Object decodeToObject(String encoded) throws IOException, ClassNotFoundException
  {  
    byte[] objBytes = Base64.decodeBase64(encoded.getBytes());

    ByteArrayInputStream bais = null;
    ObjectInputStream ois = null;

    try
    {
      bais = new ByteArrayInputStream(objBytes);
      ois = new ObjectInputStream(bais);
      Object obj = ois.readObject();

      return obj;
    }
    finally
    {
      if (ois != null)
      {
        ois.close();
      }
      if (bais != null)
      {
        bais.close();
      }
    }
  }

  public static String encodeDoubleArray(double[] noData) throws IOException
  {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = null;
    byte[] rawBytes;
    try
    {
      oos = new ObjectOutputStream( baos );
      oos.writeInt(noData.length);
      for (int i = 0; i < noData.length; i++)
      {
        oos.writeDouble(noData[i]);
      }
      oos.flush();
      rawBytes = baos.toByteArray();
    }
    finally
    {
      if (oos != null)
      {
        oos.close();
      }
      baos.close();
    }
  
    return new String(Base64.encodeBase64(rawBytes));
  }

  public static double[] decodeToDoubleArray(String encoded) throws IOException
  {  
    byte[] objBytes = Base64.decodeBase64(encoded.getBytes());

    ByteArrayInputStream bais = null;
    ObjectInputStream ois = null;

    try
    {
      bais = new ByteArrayInputStream(objBytes);
      ois = new ObjectInputStream(bais);
      int arrayLength = ois.readInt();
      double result[] = new double[arrayLength];
      for (int i = 0; i < arrayLength; i++)
      {
        result[i] = ois.readDouble();
      }
      return result;
    }
    finally
    {
      if (ois != null)
      {
        ois.close();
      }
      if (bais != null)
      {
        bais.close();
      }
    }
  }
}
