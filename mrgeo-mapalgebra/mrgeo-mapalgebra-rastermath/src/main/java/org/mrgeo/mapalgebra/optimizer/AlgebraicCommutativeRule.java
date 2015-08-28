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

import org.mrgeo.mapalgebra.MapOpHadoop;
import org.mrgeo.mapalgebra.RawBinaryMathMapOpHadoop;
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
  public ArrayList<MapOpHadoop> apply(MapOpHadoop root, MapOpHadoop subject)
  {
    ArrayList<MapOpHadoop> result = new ArrayList<MapOpHadoop>();

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

  private ArrayList<MapOpHadoop> _applySimpleRule(MapOpHadoop root, RenderedImageMapOp subject, String op)
  {
    ArrayList<MapOpHadoop> result = new ArrayList<MapOpHadoop>();
    List<MapOpHadoop> inputs = subject.getInputs();

    // Find this structure:
    // a + (b + c)
    // and rearrange as:
    // b + (a + c)
    // c + (b + a)
    
    MapOpHadoop left = inputs.get(0);
    MapOpHadoop plus = inputs.get(1);
    
    // if there is a + operation child, then manipulate it.
    if (_getOperation(left) != null && _getOperation(left).equals(op))
    {
      MapOpHadoop swap = left;
      left = plus;
      plus = swap;
    }
    
    if (_getOperation(plus) != null && _getOperation(plus).equals(op))
    {
      MapOpHadoop a = left;
      MapOpHadoop b = plus.getInputs().get(0);
      MapOpHadoop c = plus.getInputs().get(1);

      // create the "b + (a + c)" version
      {
        MapOpHadoop newRoot = root.clone();
        MapOpHadoop newA = _findNewSubject(root, newRoot, a);
        MapOpHadoop newB = _findNewSubject(root, newRoot, b);
        _replaceNode(newRoot, newA, newB.clone());
        _replaceNode(newRoot, newB, newA.clone());

        result.add(newRoot);
      }

      // create the "c + (b + a)" version
      {
        MapOpHadoop newRoot = root.clone();
        MapOpHadoop newA = _findNewSubject(root, newRoot, a);
        MapOpHadoop newC = _findNewSubject(root, newRoot, c);
        _replaceNode(newRoot, newA, newC.clone());
        _replaceNode(newRoot, newC, newA.clone());
        
        result.add(newRoot);
      }      
    }
    
    return result;
  }

  private ArrayList<MapOpHadoop> _applySubtractionRule(MapOpHadoop root, RenderedImageMapOp subject)
  {
    MapOpHadoop newRoot = root.clone();
    MapOpHadoop newSubject = _findNewSubject(root, newRoot, subject);
    ArrayList<MapOpHadoop> result = new ArrayList<MapOpHadoop>();
    List<MapOpHadoop> newInputs = newSubject.getInputs();
    
    // Convert
    // a - b
    // to:
    // a + -b

    MapOpHadoop a = newInputs.get(0);
    MapOpHadoop b = newInputs.get(1);
    MapOpHadoop newB = null;

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
      RawBinaryMathMapOpHadoop minus = new RawBinaryMathMapOpHadoop();
      minus.setFunctionName("UMinus");
      minus.addInput(b);
      newB = minus;
    }

    // create a new addition op
    RawBinaryMathMapOpHadoop add = new RawBinaryMathMapOpHadoop();
    add.setFunctionName("+");
    // put the original a side into the add
    add.addInput(a);
    // put our newly negated value into the b side
    add.addInput(newB);
    
    newRoot = _replaceNode(newRoot, newSubject, add);
    result.add(newRoot);

    return result;
  }

  private ArrayList<MapOpHadoop> _applyDivisionRule(MapOpHadoop root, RenderedImageMapOp subject)
  {
    MapOpHadoop newRoot = root.clone();
    MapOpHadoop newSubject = _findNewSubject(root, newRoot, subject);
    ArrayList<MapOpHadoop> result = new ArrayList<MapOpHadoop>();
    List<MapOpHadoop> newInputs = newSubject.getInputs();
    
    // Convert
    // a / b
    // to:
    // a * (1 / b)

    MapOpHadoop a = newInputs.get(0);
    MapOpHadoop b = newInputs.get(1);
    MapOpHadoop newB = null;

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
      RawBinaryMathMapOpHadoop divide = new RawBinaryMathMapOpHadoop();
      divide.setFunctionName("/");
      divide.addInput(_createConstantMapOp(1.0));
      divide.addInput(b);
      newB = divide;
    }

    // create a new multiplication op
    RawBinaryMathMapOpHadoop divide = new RawBinaryMathMapOpHadoop();
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

    String s = _getOperation(subject);
    if (s != null && (s.equals("+") || s.equals("*") || s.equals("/") || s.equals("-")))
    {
      result = true;
    }

    return result;
  }
}
