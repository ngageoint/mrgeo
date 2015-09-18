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
import org.mrgeo.mapalgebra.old.ParserAdapterHadoop;
import org.mrgeo.mapalgebra.parser.ParserNode;
import org.mrgeo.opimage.RawUnaryDescriptor;

import java.util.Vector;

public class RawUnaryMapOp extends RenderedImageMapOp
{
  public static String[] register()
  {
    return new String[] { "UMinus",
        "abs",
        "cos",
        "sin",
        "tan",
        "isNull"};
  }

  public RawUnaryMapOp()
  {
    _factory = new RawUnaryDescriptor();
  }

  @Override
  public Vector<ParserNode> processChildren(final Vector<ParserNode> children, final ParserAdapterHadoop parser)
  {
    Vector<ParserNode> result = new Vector<ParserNode>();
    if (children.size() != 1)
    {
      throw new IllegalArgumentException(getFunctionName() + " takes one raster argument");
    }
    result.add(children.get(0));
    parser.addFunction("abs");
    parser.addFunction("cos");
    parser.addFunction("sin");
    parser.addFunction("tan");
    parser.addFunction("isNull");
    return result;
  }

  @Override
  public String toString()
  {
    return getFunctionName() + "()";
  }

  @Override
  public boolean includeFunctionNameInParameters()
  {
    return true;
  }
}
