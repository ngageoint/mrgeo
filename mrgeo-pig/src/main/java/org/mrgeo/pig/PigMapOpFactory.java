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

package org.mrgeo.pig;

import java.util.ArrayList;

import org.mrgeo.mapalgebra.MapOp;
import org.mrgeo.mapalgebra.MapOpFactory;
import org.mrgeo.mapalgebra.parser.ParserConstantNode;
import org.mrgeo.mapalgebra.parser.ParserException;
import org.mrgeo.mapalgebra.parser.ParserFunctionNode;
import org.mrgeo.mapalgebra.parser.ParserNode;

/**
 * 
 */
public class PigMapOpFactory implements MapOpFactory
{
  private ArrayList<String> _mapOpNames;
  private MapOpFactory _rootFactory;
  
  public PigMapOpFactory()
  {
    _mapOpNames = new ArrayList<String>();
    _mapOpNames.add("pig");
    _mapOpNames.add("pigScript");
  }

  @Override
  public MapOp convertToMapOp(ParserFunctionNode node) throws ParserException
  {
    PigMapOp result = new PigMapOp();
    
    if (node.getNumChildren() == 0)
    {
      throw new ParserException(String.format("Expected at least 1 arguments for 'pig'."));
    }

    if (node.getName().equals("pig"))
    {
      // Pig script comes first.
      ParserNode child = node.getChild(0);
      ParserConstantNode c = (ParserConstantNode) child;
      PigMapOp.setScript(c.getValue().toString());
    }
    else if (node.getName().equals("pigScript"))
    {
      // Pig script comes first.
      ParserNode child = node.getChild(0);
      ParserConstantNode c = (ParserConstantNode) child;
      try
      {
        result.setScriptFile(c.getValue().toString());
      }
      catch (Exception e)
      {
        throw new ParserException(e.toString());
      }
    }
    else
    {
      throw new ParserException(String.format("Unexpected function name: %s", node.getName()));
    }

    // the inputs always go at the end of the function
    for (int i = 1; i < node.getNumChildren(); i++)
    {
      result.addInput(_rootFactory.convertToMapOp(node.getChild(i)));
    }

    return result;
  }

  @Override
  public MapOp convertToMapOp(ParserNode node) throws ParserException
  {
    throw new IllegalArgumentException("Unsupported by this factory.");
  }

  @Override
  public ArrayList<String> getMapOpNames()
  {
    return _mapOpNames;
  }

  @Override
  public void setRootFactory(MapOpFactory root)
  {
    _rootFactory = root;
  }
}
