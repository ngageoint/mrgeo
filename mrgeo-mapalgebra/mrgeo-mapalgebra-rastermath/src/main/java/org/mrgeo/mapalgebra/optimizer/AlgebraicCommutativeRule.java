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
 * Algebraic Commutative rule with some simple constant evaluation. E.g.
 * a + (b + c) = b + (a + c)
 * a * (b * c) = b * (a * c)
 * a - b = a + -b
 * a / b = a * (1 / b)
 * etc.
 */
public class AlgebraicCommutativeRule extends AlgebraicRule
{
  @Override
  public ArrayList<MapOp> apply(MapOp root, MapOp subject)
  {
    ArrayList<MapOp> result = new ArrayList<MapOp>();

    RenderedImageMapOp ri = (RenderedImageMapOp) subject;
    String s = _getOperation(ri);
    if (s.equals("+"))
    {
      result.addAll(_applySimpleRule(root, ri, "+"));
    }
    else if (s.equals("*"))
    {
      result.addAll(_applySimpleRule(root, ri, "*"));
    }
    else if (s.equals("-"))
    {
      result.addAll(_applySubtractionRule(root, ri));
    }
    else if (s.equals("/"))
    {
      result.addAll(_applyDivisionRule(root, ri));
    }
    return result;
  }

  private ArrayList<MapOp> _applySimpleRule(MapOp root, RenderedImageMapOp subject, String op)
  {
    ArrayList<MapOp> result = new ArrayList<MapOp>();
    List<MapOp> inputs = subject.getInputs();

    // Find this structure:
    // a + (b + c)
    // and rearrange as:
    // b + (a + c)
    // c + (b + a)
    
    MapOp left = inputs.get(0);
    MapOp plus = inputs.get(1);
    
    // if there is a + operation child, then manipulate it.
    if (_getOperation(left) != null && _getOperation(left).equals(op))
    {
      MapOp swap = left;
      left = plus;
      plus = swap;
    }
    
    if (_getOperation(plus) != null && _getOperation(plus).equals(op))
    {
      MapOp a = left;
      MapOp b = plus.getInputs().get(0);
      MapOp c = plus.getInputs().get(1);

      // create the "b + (a + c)" version
      {
        MapOp newRoot = root.clone();
        MapOp newA = _findNewSubject(root, newRoot, a);
        MapOp newB = _findNewSubject(root, newRoot, b);
        _replaceNode(newRoot, newA, newB.clone());
        _replaceNode(newRoot, newB, newA.clone());

        result.add(newRoot);
      }

      // create the "c + (b + a)" version
      {
        MapOp newRoot = root.clone();
        MapOp newA = _findNewSubject(root, newRoot, a);
        MapOp newC = _findNewSubject(root, newRoot, c);
        _replaceNode(newRoot, newA, newC.clone());
        _replaceNode(newRoot, newC, newA.clone());
        
        result.add(newRoot);
      }      
    }
    
    return result;
  }

  private ArrayList<MapOp> _applySubtractionRule(MapOp root, RenderedImageMapOp subject)
  {
    MapOp newRoot = root.clone();
    MapOp newSubject = _findNewSubject(root, newRoot, subject);
    ArrayList<MapOp> result = new ArrayList<MapOp>();
    List<MapOp> newInputs = newSubject.getInputs();
    
    // Convert
    // a - b
    // to:
    // a + -b

    MapOp a = newInputs.get(0);
    MapOp b = newInputs.get(1);
    MapOp newB = null;

    // check to see if we can just swap the sign on a constant
    if (b instanceof RenderedImageMapOp)
    {
      Double bv = _getConstant(b);
      if (bv != null)
      {
        double v = -bv;
        newB = _createConstantMapOp(v);
      }
    }
    // if we couldn't do the constant trick.
    if (newB == null)
    {
      RawBinaryMathMapOp minus = new RawBinaryMathMapOp();
      minus.setFunctionName("UMinus");
      minus.addInput(b);
      newB = minus;
    }

    // create a new addition op
    RawBinaryMathMapOp add = new RawBinaryMathMapOp();
    add.setFunctionName("+");
    // put the original a side into the add
    add.addInput(a);
    // put our newly negated value into the b side
    add.addInput(newB);
    
    newRoot = _replaceNode(newRoot, newSubject, add);
    result.add(newRoot);

    return result;
  }

  private ArrayList<MapOp> _applyDivisionRule(MapOp root, RenderedImageMapOp subject)
  {
    MapOp newRoot = root.clone();
    MapOp newSubject = _findNewSubject(root, newRoot, subject);
    ArrayList<MapOp> result = new ArrayList<MapOp>();
    List<MapOp> newInputs = newSubject.getInputs();
    
    // Convert
    // a / b
    // to:
    // a * (1 / b)

    MapOp a = newInputs.get(0);
    MapOp b = newInputs.get(1);
    MapOp newB = null;

    // check to see if we can just manipulate the constant
    if (b instanceof RenderedImageMapOp)
    {
      Double bv = _getConstant(b);
      if (bv != null)
      {
        double v = 1.0 / bv;
        newB = _createConstantMapOp(v);
      }
    }
    // if we couldn't do the constant trick.
    if (newB == null)
    {
      RawBinaryMathMapOp divide = new RawBinaryMathMapOp();
      divide.setFunctionName("/");
      divide.addInput(_createConstantMapOp(1.0));
      divide.addInput(b);
      newB = divide;
    }

    // create a new multiplication op
    RawBinaryMathMapOp divide = new RawBinaryMathMapOp();
    divide.setFunctionName("*");
    // put the original a side into the add
    divide.addInput(a);
    // put our newly negated value into the b side
    divide.addInput(newB);
    
    newRoot = _replaceNode(newRoot, newSubject, divide);
    result.add(newRoot);

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

    String s = _getOperation(subject);
    if (s != null && (s.equals("+") || s.equals("*") || s.equals("/") || s.equals("-")))
    {
      result = true;
    }

    return result;
  }
}
