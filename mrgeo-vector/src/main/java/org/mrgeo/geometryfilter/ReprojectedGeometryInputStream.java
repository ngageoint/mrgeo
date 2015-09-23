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

package org.mrgeo.geometryfilter;

import org.mrgeo.geometry.WritableGeometry;
import org.mrgeo.data.GeometryInputStream;
import org.mrgeo.hdfs.vector.Reprojector;

import java.io.IOException;


public class ReprojectedGeometryInputStream implements GeometryInputStream
{

  String newProjection;
  Reprojector reprojector;
  GeometryInputStream srcStream = null;

  public ReprojectedGeometryInputStream(GeometryInputStream src, String newProjection)
  {
    this.newProjection = newProjection;
    srcStream = src;
    reprojector = Reprojector.createFromWkt(src.getProjection(), newProjection);
  }

  @Override
  public String getProjection()
  {
    return newProjection;
  }

  @Override
  public boolean hasNext()
  {
    return srcStream.hasNext();
  }

  @Override
  public WritableGeometry next()
  {
    WritableGeometry g = srcStream.next();
    if (g != null)
    {
      g.filter(reprojector);
    }
    return g;
  }

  @Override
  public void remove()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws IOException
  {
    if (srcStream != null)
    {
      srcStream.close();
      srcStream = null;
    }
  }

}
