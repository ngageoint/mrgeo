package org.mrgeo.utils;

import java.io.*;

public class ObjectUtils
{
public static byte[] encodeObject(Object obj) throws IOException
{
  ByteArrayOutputStream baos = new ByteArrayOutputStream();
  ObjectOutputStream oos = null;
  try
  {
    oos = new ObjectOutputStream(baos);
    oos.writeObject(obj);
    return baos.toByteArray();
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

}

public static Object decodeObject(byte[] bytes) throws IOException, ClassNotFoundException
{
  ByteArrayInputStream bais = null;
  ObjectInputStream ois = null;

  try
  {
    bais = new ByteArrayInputStream(bytes);
    ois = new ObjectInputStream(bais);
    return  ois.readObject();
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
