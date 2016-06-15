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

package org.mrgeo.services;


import org.mrgeo.utils.HashCodeUtil;

public class Version
{
private int major, minor, micro;

public Version(String str)
{
  String[] l = str.split("\\.");
  major = Integer.parseInt(l[0]);
  minor = Integer.parseInt(l[1]);
  micro = Integer.parseInt(l[2]);
}

public int compareTo(Version other)
{
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

public int compareTo(String str)
{
  return compareTo(new Version(str));
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
public int hashCode()
{
  int result = HashCodeUtil.SEED;
  //collect the contributions of various fields
  result = HashCodeUtil.hash(result, major);
  result = HashCodeUtil.hash(result, minor);
  result = HashCodeUtil.hash(result, micro);
  return result;
}

@Override
public boolean equals(Object obj)
{
  if (!(obj instanceof Version))
  {
    return false;
  }
  return compareTo((Version)obj) == 0;
}

@Override
public String toString()
{
  return major + "." + minor + "." + micro;
}
}
