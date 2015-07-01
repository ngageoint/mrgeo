/*
 * Copyright 2009-2015 DigitalGlobe, Inc.
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

package org.mrgeo.services;


public class Version
{
  private int major, minor, micro;

  public Version(String str)
  {
    String[] l = str.split("\\.");
    major = Integer.valueOf(l[0]).intValue();
    minor = Integer.valueOf(l[1]).intValue();
    micro = Integer.valueOf(l[2]).intValue();
  }

  public int compareTo(String str)
  {
    Version other = new Version(str);
    if (other.major < major)
      return 1;
    if (other.major > major)
      return -1;
    if (other.minor < minor)
      return 1;
    if (other.minor > minor)
      return -1;
    if (other.micro < micro)
      return 1;
    if (other.micro > micro)
      return -1;
    return 0;
  }

  public int getMajor()
  {
    return major;
  }

  public int getMicro()
  {
    return micro;
  }

  public int getMinor()
  {
    return minor;
  }

  public boolean isEqual(String str)
  {
    return compareTo(str) == 0;
  }

  public boolean isLess(String str)
  {
    return compareTo(str) == -1;
  }
  
  @Override
  public String toString()
  {
    return major + "." + minor + "." + micro;
  }
}
