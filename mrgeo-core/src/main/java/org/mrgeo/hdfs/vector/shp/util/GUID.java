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

import java.net.InetAddress;
import java.rmi.server.UID;
import java.security.MessageDigest;

public class GUID extends java.lang.Object implements java.io.Serializable
{
  private static final long serialVersionUID = 1L;

  private static String getHexString(byte[] hash)
  {
    StringBuffer hashString = new StringBuffer();
    for (int i = 0; i < hash.length; ++i)
    {
      String hex = Integer.toHexString(hash[i]);
      if (hex.length() == 1)
      {
        hashString.append('0');
        hashString.append(hex.charAt(hex.length() - 1));
      }
      else
      {
        hashString.append(hex.substring(hex.length() - 2));
      }
    }
    return hashString.toString();
  }

  private String hash = null;

  public GUID() throws Exception
  {
    hash = generate();
    if (hash != null)
      hash = hash.toUpperCase();
  }

  public GUID(String text) throws Exception
  {
    hash = text;
    if (hash != null)
    {
      if (hash.length() != 40)
        throw new Exception("Invalid GUID");
    }
    else
    {
      throw new Exception("GUID Cannot Be NULL");
    }
  }

  private byte[] encrypt(String text) throws Exception
  {
    MessageDigest digest = null;
    digest = MessageDigest.getInstance("SHA-1"); // MD5 128 bit & SHA-1 168 bit
                                                 // (16/20 bytes respectively;
                                                 // 32/40 chars)
    digest.reset();
    byte[] b = text.getBytes();
    digest.update(b);
    return digest.digest();
  }

  @Override
  public boolean equals(Object obj)
  {
    if (obj instanceof GUID)
    {
      if (this != obj)
        if (!hash.equals(obj.toString()))
          return false;
      return true;
    }
    return false;
  }
  
  @Override
  public int hashCode()
  {
    return super.hashCode();
  }

  private String generate() throws Exception
  {
    // get information
    InetAddress id = InetAddress.getLocalHost();
    byte[] ip = id.getAddress(); // e.g. 192.168.0.1
    UID uid = new UID();
    // build string
    String text = new String(ip) + uid.toString();
    byte[] b = encrypt(text);
    // return
    return getHexString(b);
  }

  @Override
  public String toString()
  {
    return hash; // will be 40 chars for SHA-1 & 32 for MD5
  }
}
