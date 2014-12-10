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


import org.mrgeo.mapalgebra.parser.ParserAdapter;
import org.mrgeo.mapalgebra.parser.ParserNode;
import org.mrgeo.mapreduce.formats.TileClusterInfo;
import org.mrgeo.opimage.AspectDescriptor;

import java.util.Vector;

public class AspectMapOp extends RenderedImageMapOp implements TileClusterInfoCalculator
{
  public static String[] register()
  {
    return new String[] { "aspect" };
  }

  public AspectMapOp()
  {
    _factory = new AspectDescriptor();
  }

  @Override
  public Vector<ParserNode> processChildren(final Vector<ParserNode> children, final ParserAdapter parser)
  {
    Vector<ParserNode> result = new Vector<ParserNode>();

    if (children.size() > 2)
    {
      throw new IllegalArgumentException(
          "Logarithm takes one or two arguments. single-band raster elevation and optional unit format (\"deg\" or \"rad\")");
    }

    result.add(children.get(0));

    if (children.size() == 2)
    {
      String units = MapOp.parseChildString(children.get(1), "units", parser);
      if (!units.equalsIgnoreCase("deg") || units.equalsIgnoreCase("rad"))
      {
        throw new IllegalArgumentException("Units must be \"deg\" or \"rad\".");
      }
      getParameters().add(units);
    }

    return result;
  }

  @Override
  public TileClusterInfo calculateTileClusterInfo()
  {
    // Aspect uses HornNormalOpImage which needs access to the
    // eight pixels surrounding each pixel. This means we need
    // to get the eight surrounding tiles.
    return new TileClusterInfo(-1, -1, 3, 3, 1);
  }

  @Override
  public String toString()
  {
    return "aspect()";
  }
}
