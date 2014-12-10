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

import org.mrgeo.featurefilter.BufferFeatureFilter;
import org.mrgeo.featurefilter.FeatureFilter;
import org.mrgeo.mapalgebra.parser.ParserAdapter;
import org.mrgeo.mapalgebra.parser.ParserNode;

public class VectorBufferMapOp extends FeatureFilterMapOp
{
  BufferFeatureFilter _filter = new BufferFeatureFilter();

  @Override
  public FeatureFilter getFilter()
  {
    return _filter;
  }

  @Override
  public Vector<ParserNode> processChildren(final Vector<ParserNode> children, final ParserAdapter parser)
  {
    Vector<ParserNode> result = new Vector<ParserNode>();

    if (children.size() > 4)
    {
      throw new IllegalArgumentException(
          "buffer takes two or three arguments. (source vector, and distance or range of distance(min, max) " + 
          "or unit (degree or meters))");
    }

    result.add(children.get(0));

    double distanceMin = -1;
    double distanceMax = -1;

    distanceMin = parseChildDouble(children.get(1), "min distance", parser);

    if (children.size() == 3)
    {
      String nodeValue = parseChildString(children.get(2), "unit or max distance", parser);
      if (nodeValue.equalsIgnoreCase("degree") || nodeValue.equalsIgnoreCase("meter"))
      {
        _filter.setUnit(nodeValue);
      }
      else
      {
        distanceMax = Double.valueOf(nodeValue);
        if (distanceMax < distanceMin)
        {
          double tmp = distanceMin;
          distanceMin = distanceMax;
          distanceMax = tmp;
        }
        if (distanceMin == 0) //in case user put 0 as min
        {
          _filter.setDistance(distanceMax);
        }
        else
        {
          _filter.setDistanceRange(distanceMax);
        }
      }
    }

    if (children.size() == 4)
    {
      distanceMax = parseChildDouble(children.get(2), "max distance", parser);
      if (distanceMax < distanceMin)
      {
        double tmp = distanceMin;
        distanceMin = distanceMax;
        distanceMax = tmp;
      }
      if (distanceMin == 0) //in case user put 0 as min
      {
        _filter.setDistance(distanceMax);
      }
      else
      {
        _filter.setDistanceRange(distanceMax);
      }

      String nodeValue = parseChildString(children.get(3), "unit", parser);
      if (nodeValue.equalsIgnoreCase("degree") || nodeValue.equalsIgnoreCase("meter"))
      {
        _filter.setUnit(nodeValue);
      }
    }

    return result;
  }

  @Override
  public String toString()
  {
    return String.format("BufferMapOp: %s dist: %f",
        _outputName == null ? "null": _outputName.toString(),
            _filter.getDistance());
  }
}
