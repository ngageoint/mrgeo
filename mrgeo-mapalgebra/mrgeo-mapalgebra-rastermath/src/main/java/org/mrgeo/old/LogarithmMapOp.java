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

package org.mrgeo.old;

import org.mrgeo.mapalgebra.old.RenderedImageMapOp;
import org.mrgeo.mapalgebra.old.MapOpHadoop;
import org.mrgeo.mapalgebra.old.ParserAdapterHadoop;
import org.mrgeo.mapalgebra.parser.ParserNode;
import org.mrgeo.opimage.LogarithmRasterDescriptor;

import java.util.Vector;

public class LogarithmMapOp extends RenderedImageMapOp
{
  double base = 0;

  public LogarithmMapOp()
  {
    _factory = new LogarithmRasterDescriptor();
  }

  public static String[] register()
  {
    return new String[] { "log" };
  }

  @Override
  public Vector<ParserNode> processChildren(final Vector<ParserNode> children, final ParserAdapterHadoop parser)
  {
    Vector<ParserNode> result = new Vector<ParserNode>();

    if (children.size() > 2)
    {
      throw new IllegalArgumentException(
          "Logarithm takes one or two arguments. (source raster or base)");
    }

    result.add(children.get(0));

    if (children.size() == 2)
    {
      base = MapOpHadoop.parseChildDouble(children.get(1), "base", parser);
      if ((int) base == 1)
      {
        throw new IllegalArgumentException("The number of base can not be 1.");
      }
    }
    getParameters().add(base);

    return result;
  }

  @Override
  public String toString()
  {
    return String.format("LogarithmMapOp %s",
        _outputName == null ? "null" : _outputName.toString() );
  }

}
