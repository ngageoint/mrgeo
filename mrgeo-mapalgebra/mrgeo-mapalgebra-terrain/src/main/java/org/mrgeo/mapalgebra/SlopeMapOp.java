/*
 * Copyright 2009-2014 DigitalGlobe, Inc.
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

package org.mrgeo.mapalgebra;

import java.util.Vector;

import org.mrgeo.mapalgebra.parser.ParserAdapter;
import org.mrgeo.mapalgebra.parser.ParserNode;
import org.mrgeo.mapreduce.formats.TileClusterInfo;
import org.mrgeo.opimage.SlopeDescriptor;

public class SlopeMapOp extends RenderedImageMapOp implements TileClusterInfoCalculator
{
  public static String[] register()
  {
    return new String[] { "slope" };
  }

  public SlopeMapOp()
  {
    _factory = new SlopeDescriptor();
  }

  @Override
  public Vector<ParserNode> processChildren(final Vector<ParserNode> children, final ParserAdapter parser)
  {
    Vector<ParserNode> result = new Vector<ParserNode>();
    if (children.size() != 1)
    {
      throw new IllegalArgumentException("slope takes one argument - single-band raster elevation");
    }
    result.add(children.get(0));
    return result;
  }

  @Override
  public TileClusterInfo calculateTileClusterInfo()
  {
    // Slope uses HornNormalOpImage which needs access to the
    // eight pixels surrounding each pixel. This means we need
    // to get the eight surrounding tiles.
    return new TileClusterInfo(-1, -1, 3, 3, 1);
  }

  @Override
  public String toString()
  {
    return "slope()";
  }
}
