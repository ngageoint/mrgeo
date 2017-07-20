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

package org.mrgeo.utils.tms;

import java.io.Serializable;

// Pixel 0, 0 is the lower-left corner of the world grid!
public class Pixel implements Serializable
{
private static final long serialVersionUID = 1L;

final public long px;
final public long py;

public Pixel(long px, long py)

{
  this.px = px;
  this.py = py;
}

public long getPy()
{
  return py;
}

public long getPx()
{
  return px;
}

@Override
public boolean equals(Object obj)
{
  if (obj instanceof Pixel)
  {
    Pixel other = (Pixel) obj;
    return (px == other.px) && (py == other.py);
  }
  return super.equals(obj);
}

@Override
public int hashCode()
{
  // Based on Point2D.hashCode() implementation
  long bits = px;
  bits ^= py * 31;
  return (((int) bits) ^ ((int) (bits >> 32)));
}

@Override
public String toString()
{
  return "Pixel [px=" + px + ", py=" + py + "]";
}

}
