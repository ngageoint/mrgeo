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

package org.mrgeo.mapalgebra.optimizer;

import org.mrgeo.mapalgebra.old.MapOpHadoop;
import org.mrgeo.old.RawBinaryMathMapOpHadoop;
import org.mrgeo.mapalgebra.RenderedImageMapOp;

import java.util.ArrayList;
import java.util.List;

/**
 * Algebraic Fraction rules with some simple constant evaluation. E.g.
 * (a / b) * c = (a * c) / b
 * etc.
 */
public class AlgebraicFractionRule extends AlgebraicRule
{
  @Override
  public ArrayList<MapOpHadoop> apply(MapOpHadoop root, MapOpHadoop subject)
  {
    ArrayList<MapOpHadoop> result = new ArrayList<MapOpHadoop>();

    RenderedImageMapOp ri = (RenderedImageMapOp) subject;
    String s = _getOperation(ri);
    if (s == "*")
    {
      result.addAll(_applyMultiplicationRule(root, ri));
    }

    return result;
  }

  private ArrayList<MapOpHadoop> _applyMultiplicationRule(MapOpHadoop root, RenderedImageMapOp subject)
  {
    MapOpHadoop newRoot = root.clone();
    MapOpHadoop newSubject = _findNewSubject(root, newRoot, subject);
    ArrayList<MapOpHadoop> result = new ArrayList<MapOpHadoop>();
    List<MapOpHadoop> newInputs = newSubject.getInputs();
    
    // Convert
    // (a / b) * c
    // to:
    // (a * c) / b
    
    MapOpHadoop ab = newInputs.get(0);
    MapOpHadoop c = newInputs.get(1);
    
    if (!_getOperation(ab).equals("/"))
    {
      MapOpHadoop swap = ab;
      ab = c;
      c = swap;
    }

    if (_getOperation(ab).equals("/"))
    {
      MapOpHadoop a = ab.getInputs().get(0);
      MapOpHadoop b = ab.getInputs().get(1);

      // create a new multiply op
      RawBinaryMathMapOpHadoop multiply = new RawBinaryMathMapOpHadoop();
      multiply.setFunctionName("*");
      multiply.addInput(a);
      multiply.addInput(c);

      // create a new addition op
      RawBinaryMathMapOpHadoop divide = new RawBinaryMathMapOpHadoop();
      divide.setFunctionName("/");
      // put the original a side into the add
      divide.addInput(multiply);
      // put our newly negated value into the b side
      divide.addInput(b);
      
      newRoot = _replaceNode(newRoot, newSubject, divide);
      result.add(newRoot);
    }
    
    return result;
  }

  @Override
  public ArrayList<Class<? extends MapOpHadoop>> getCandidates()
  {
    ArrayList<Class<? extends MapOpHadoop>> result = new ArrayList<Class<? extends MapOpHadoop>>();
//    result.add(RenderedImageMapOp.class);
    result.add(RawBinaryMathMapOpHadoop.class);
    return result;
  }

  @Override
  public boolean isApplicable(MapOpHadoop root, MapOpHadoop subject)
  {
    boolean result = false;

    if (subject instanceof RenderedImageMapOp)
    {
      String s = _getOperation((RenderedImageMapOp) subject);
      if (s != null && (s.equals("+") || s.equals("*") || s.equals("/") || s.equals("-")))
      {
        result = true;
      }
    }

    return result;
  }
}
