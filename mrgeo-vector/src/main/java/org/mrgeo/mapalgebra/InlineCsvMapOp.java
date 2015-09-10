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

import java.util.Vector;

import org.mrgeo.mapalgebra.old.MapOpHadoop;
import org.mrgeo.mapalgebra.old.ParserAdapterHadoop;
import org.mrgeo.mapalgebra.parser.ParserNode;
import org.mrgeo.progress.Progress;

public class InlineCsvMapOp extends VectorMapOp
{
  private String _columns, _values;

  @Override
  public void addInput(MapOpHadoop n) throws IllegalArgumentException
  {
    throw new IllegalArgumentException("This ExecuteNode takes no arguments.");
  }

  @Override
  public void build(Progress p)
  {
    if (p != null)
    {
      p.complete();
    }
    _output = new InlineCsvInputFormatDescriptor(_columns, _values);
  }

  @Override
  public Vector<ParserNode> processChildren(final Vector<ParserNode> children, final ParserAdapterHadoop parser)
  {
    Vector<ParserNode> result = new Vector<ParserNode>();

    if (children.size() != 2)
    {
      throw new IllegalArgumentException(
          "Inline CSV takes two arguments. (columns and values)");
    }

    _columns = parseChildString(children.get(0), "columns", parser);
    _values = parseChildString(children.get(1), "values", parser);
    
    return result;
  }
  
  @Override
  public String toString()
  {
    return String.format("InlineCsvMapOp %s",
       _outputName == null ? "null" : _outputName.toString() );
  }

}
