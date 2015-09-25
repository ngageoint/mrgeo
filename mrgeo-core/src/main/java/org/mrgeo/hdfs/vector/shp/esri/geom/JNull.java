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

package org.mrgeo.hdfs.vector.shp.esri.geom;

public class JNull extends JShape
{
  @SuppressWarnings("hiding")
  static final long serialVersionUID = 1L;

  /** Creates new JNull */
  public JNull()
  {
    super(NULL);
  }

  @Override
  public byte check(boolean clean)
  {
    return READY;
  }

  @Override
  public void debug()
  {
  }

  @Override
  public JExtent getExtent()
  {
    return null;
  }

  @Override
  public int getRecordLength()
  {
    return 0;
  }

  @Override
  public boolean intersects(JExtent other)
  {
    return false;
  }

  @Override
  public String toString()
  {
    return extent.toString();
  }

  @Override
  public void updateExtent()
  {
    extent = null;
  }
}
