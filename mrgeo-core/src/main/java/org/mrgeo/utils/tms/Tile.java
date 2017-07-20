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

// Tile 0, 0 is the lower-left corner of the world grid!
public class Tile implements Comparable<Tile>
{

final public long tx;
final public long ty;

public Tile(long tx, long ty)
{
  this.tx = tx;
  this.ty = ty;
}

public long getTx()
{
  return tx;
}

public long getTy()
{
  return ty;
}

@Override
public int compareTo(Tile tile)
{
  if (ty == tile.ty && tx == tile.tx)
  {
    return 0;
  }
  else if (ty < tile.ty || (ty == tile.ty && tx < tile.tx))
  {
    return -1;
  }
  return 1;
}

@Override
public boolean equals(Object obj)
{
  return obj instanceof Tile &&
      ty == ((Tile) obj).ty && tx == ((Tile) obj).tx;
}

@Override
public int hashCode()
{
  int prime = 31;
  int result = 1;
  long temp;
  temp = tx;
  result = prime * result + (int) (temp ^ (temp >>> 32));
  temp = ty;
  result = prime * result + (int) (temp ^ (temp >>> 32));
  return result;
}


@Override
public String toString()
{
  return "Tile [tx=" + tx + ", ty=" + ty + "]";
}
}
