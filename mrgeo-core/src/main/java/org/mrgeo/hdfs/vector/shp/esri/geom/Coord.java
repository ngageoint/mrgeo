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

package org.mrgeo.hdfs.vector.shp.esri.geom;

import org.mrgeo.utils.FloatUtils;

public final class Coord implements Cloneable, java.io.Serializable
{
static final long serialVersionUID = 1L;
public double x;
public double y;

/**
 * Creates new Coord
 */
public Coord()
{
  x = 0;
  y = 0;
}

public Coord(double x, double y)
{
  this.x = x;
  this.y = y;
}

@Override
@SuppressWarnings("squid:S1166") // Exception caught and handled
public Object clone()
{
  try
  {
    return super.clone();
  }
  catch (CloneNotSupportedException e)
  {
    // note: we don't propagate this exception because Coord is final
    throw new InternalError(e.toString());
  }
}


@Override
public int hashCode()
{
  return super.hashCode();
}

@Override
public boolean equals(Object obj)
{
  if (obj instanceof Coord)
  {
    Coord p = (Coord) obj;
    if (FloatUtils.isEqual(p.x, x) && FloatUtils.isEqual(p.y, y))
    {
      return true;
    }
  }
  return false;
}

@Override
public String toString()
{
  return x + "," + y;
}
}
