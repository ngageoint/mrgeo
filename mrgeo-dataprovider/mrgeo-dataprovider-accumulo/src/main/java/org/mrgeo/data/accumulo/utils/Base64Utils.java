/*
 * Copyright 2009-2017. DigitalGlobe, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.mrgeo.data.accumulo.utils;

import javax.xml.bind.DatatypeConverter;
import java.io.*;

public class Base64Utils
{
public static boolean isBase64(String encoded)
{
  // To be a candidate for base64 decoding, the string length _must_ be a multiple of 4,
  // and only contain [A-Z,a-z,0-9,and + /], padded with "="
  return (encoded.length() > 0) && (encoded.length() % 4 == 0) &&
      encoded.matches("^([A-Za-z0-9+/]{4})*([A-Za-z0-9+/]{4}|[A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{2}==)$");

}

public static String encodeObject(Object obj) throws IOException
{
  byte[] bytes = ObjectUtils.encodeObject(obj);
  return DatatypeConverter.printBase64Binary(bytes);
}

public static String decodeToString(String encoded) throws IOException, ClassNotFoundException
{
  return (String) decodeToObject(encoded);
}

public static Object decodeToObject(String encoded) throws IOException, ClassNotFoundException
{
  byte[] bytes = DatatypeConverter.parseBase64Binary(encoded);
  return ObjectUtils.decodeObject(bytes);
}

public static String encodeDoubleArray(double[] noData) throws IOException
{
  ByteArrayOutputStream baos = new ByteArrayOutputStream();
  ObjectOutputStream oos = null;
  byte[] rawBytes;
  try
  {
    oos = new ObjectOutputStream(baos);
    oos.writeInt(noData.length);
    for (double nd : noData)
    {
      oos.writeDouble(nd);
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

  return DatatypeConverter.printBase64Binary(rawBytes);
}

public static double[] decodeToDoubleArray(String encoded) throws IOException
{
  byte[] objBytes = DatatypeConverter.parseBase64Binary(encoded);

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
