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

package org.mrgeo.mapalgebra.optimizer;

import org.mrgeo.mapalgebra.MapOp;
import org.mrgeo.mapalgebra.RawBinaryMathMapOp;
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
  public ArrayList<MapOp> apply(MapOp root, MapOp subject)
  {
    ArrayList<MapOp> result = new ArrayList<MapOp>();

    RenderedImageMapOp ri = (RenderedImageMapOp) subject;
    String s = _getOperation(ri);
    if (s == "*")
    {
      result.addAll(_applyMultiplicationRule(root, ri));
    }

    return result;
  }

  private ArrayList<MapOp> _applyMultiplicationRule(MapOp root, RenderedImageMapOp subject)
  {
    MapOp newRoot = root.clone();
    MapOp newSubject = _findNewSubject(root, newRoot, subject);
    ArrayList<MapOp> result = new ArrayList<MapOp>();
    List<MapOp> newInputs = newSubject.getInputs();
    
    // Convert
    // (a / b) * c
    // to:
    // (a * c) / b
    
    MapOp ab = newInputs.get(0);
    MapOp c = newInputs.get(1);
    
    if (!_getOperation(ab).equals("/"))
    {
      MapOp swap = ab;
      ab = c;
      c = swap;
    }

    if (_getOperation(ab).equals("/"))
    {
      MapOp a = ab.getInputs().get(0);
      MapOp b = ab.getInputs().get(1);

      // create a new multiply op
      RawBinaryMathMapOp multiply = new RawBinaryMathMapOp();
      multiply.setFunctionName("*");
      multiply.addInput(a);
      multiply.addInput(c);

      // create a new addition op
      RawBinaryMathMapOp divide = new RawBinaryMathMapOp();
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
  public ArrayList<Class<? extends MapOp>> getCandidates()
  {
    ArrayList<Class<? extends MapOp>> result = new ArrayList<Class<? extends MapOp>>();
//    result.add(RenderedImageMapOp.class);
    result.add(RawBinaryMathMapOp.class);
    return result;
  }

  @Override
  public boolean isApplicable(MapOp root, MapOp subject)
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
