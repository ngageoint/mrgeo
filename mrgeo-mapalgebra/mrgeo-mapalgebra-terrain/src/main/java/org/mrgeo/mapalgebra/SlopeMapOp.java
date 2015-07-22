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

package org.mrgeo.mapalgebra;

import org.mrgeo.mapalgebra.parser.ParserAdapter;
import org.mrgeo.mapalgebra.parser.ParserNode;
import org.mrgeo.mapreduce.job.JobCancelledException;
import org.mrgeo.mapreduce.job.JobFailedException;
import org.mrgeo.progress.Progress;
import org.mrgeo.slope.SlopeDriver;

import java.io.IOException;
import java.util.Vector;

public class SlopeMapOp extends RasterMapOp
{
  String units = "rad";

  public static String[] register()
  {
    return new String[] { "slope" };
  }

@Override
public void addInput(MapOp n) throws IllegalArgumentException
{
  if (!(n instanceof RasterMapOp))
  {
    throw new IllegalArgumentException("Can only run slope() on raster inputs");
  }
  if (_inputs.size() >= 1)
  {
    throw new IllegalArgumentException("Can only run slope() on a single raster input");
  }

  _inputs.add(n);
}

@Override
public void build(Progress p) throws IOException, JobFailedException, JobCancelledException
{
  p.starting();
  String input = ((RasterMapOp) _inputs.get(0)).getOutputName();

  SlopeDriver.slope(input, units, getOutputName(), createConfiguration());

  p.complete();
}

@Override
public Vector<ParserNode> processChildren(Vector<ParserNode> children, ParserAdapter parser)
{
    Vector<ParserNode> result = new Vector<ParserNode>();

    if (children.size() > 2)
    {
      throw new IllegalArgumentException(
          "Slope takes one or two arguments. single-band raster elevation and optional unit format (\"deg\", \"rad\", \"gradient\", or \"percent\")");
    }

    result.add(children.get(0));

    if (children.size() == 2)
    {
      String units = MapOp.parseChildString(children.get(1), "units", parser);
      if (!(units.equalsIgnoreCase("deg") || units.equalsIgnoreCase("rad")
          || units.equalsIgnoreCase("gradient") || units.equalsIgnoreCase("percent")))
      {
        throw new IllegalArgumentException("Units must be \"deg\", \"rad\", \"gradient\", or \"percent\".");
      }
      this.units = units;
    }

    return result;
}

  @Override
  public String toString()
  {
    return "slope()";
  }


}
//public class SlopeMapOp extends RenderedImageMapOp implements TileClusterInfoCalculator
//{
//  public static String[] register()
//  {
//    return new String[] { "slope" };
//  }
//
//  public SlopeMapOp()
//  {
//    _factory = new SlopeDescriptor();
//  }
//
//  @Override
//  public Vector<ParserNode> processChildren(final Vector<ParserNode> children, final ParserAdapter parser)
//  {
//    Vector<ParserNode> result = new Vector<ParserNode>();
//
//    if (children.size() > 2)
//    {
//      throw new IllegalArgumentException(
//          "Slope takes one or two arguments. single-band raster elevation and optional unit format (\"deg\", \"rad\", \"gradient\", or \"percent\")");
//    }
//
//    result.add(children.get(0));
//
//    if (children.size() == 2)
//    {
//      String units = MapOp.parseChildString(children.get(1), "units", parser);
//      if (!(units.equalsIgnoreCase("deg") || units.equalsIgnoreCase("rad")
//          || units.equalsIgnoreCase("gradient") || units.equalsIgnoreCase("percent")))
//      {
//        throw new IllegalArgumentException("Units must be \"deg\", \"rad\", \"gradient\", or \"percent\".");
//      }
//      getParameters().add(units);
//    }
//
//    return result;
//  }
//
//  @Override
//  public TileClusterInfo calculateTileClusterInfo()
//  {
//    // Slope uses HornNormalOpImage which needs access to the
//    // eight pixels surrounding each pixel. This means we need
//    // to get the eight surrounding tiles.
//    return new TileClusterInfo(-1, -1, 3, 3, 1);
//  }
//
//  @Override
//  public String toString()
//  {
//    return "slope()";
//  }
//}
