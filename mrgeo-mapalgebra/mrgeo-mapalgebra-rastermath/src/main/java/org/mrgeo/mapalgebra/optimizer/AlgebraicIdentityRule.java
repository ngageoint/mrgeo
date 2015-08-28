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
 * Algebraic Identity rule with some simple constant evaluation. E.g.
 * a - 0 = a
 * 5 + 3 = 8
 * b * 1 = b
 * etc.
 */
public class AlgebraicIdentityRule extends AlgebraicRule
{
  @Override
  public ArrayList<MapOpHadoop> apply(MapOpHadoop root, MapOpHadoop subject)
  {
    ArrayList<MapOpHadoop> result = new ArrayList<MapOpHadoop>();

    RenderedImageMapOp ri = (RenderedImageMapOp) subject;
    String s = _getOperation(ri);
    if (s.equals("+"))
    {
      MapOpHadoop newRoot = _applyAdditionRule(root, ri);
      if (newRoot != null)
      {
        result.add(newRoot);
      }
    }
    else if (s.equals("*"))
    {
      MapOpHadoop newRoot = _applyMultiplicationRule(root, ri);
      if (newRoot != null)
      {
        result.add(newRoot);
      }
    }
    else if (s.equals("-"))
    {
      MapOpHadoop newRoot = _applySubtractionRule(root, ri);
      if (newRoot != null)
      {
        result.add(newRoot);
      }
    }
    else if (s.equals("/"))
    {
      MapOpHadoop newRoot = _applyDivisionRule(root, ri);
      if (newRoot != null)
      {
        result.add(newRoot);
      }
    }

    return result;
  }

  private MapOpHadoop _applyAdditionRule(MapOpHadoop root, RenderedImageMapOp subject)
  {
    MapOpHadoop newRoot = root.clone();
    MapOpHadoop newSubject = _findNewSubject(root, newRoot, subject);
    MapOpHadoop result = null;
    ArrayList<MapOpHadoop> newInputs = new ArrayList<MapOpHadoop>();

    boolean allConst = true;

    // Create a new list if inputs that excludes all zeros.
    for (MapOpHadoop mo : newSubject.getInputs())
    {
      if (mo instanceof RenderedImageMapOp)
      {
        RenderedImageMapOp child = (RenderedImageMapOp) mo;
        Double v = _getConstant(child);
        if (v == null)
        {
          allConst = false;
          newInputs.add(mo);
        }
        else if (v != 0.0)
        {
          newInputs.add(mo);
        }
      }
      else
      {
        allConst = false;
        newInputs.add(mo);
      }
    }

    MapOpHadoop changedSubject = null;

    // if we don't have any inputs left, replace the subject w/ a zero constant.
    if (newInputs.size() == 0)
    {
      changedSubject = _createConstantMapOp(0.0);
    }
    else if (newInputs.size() == 1)
    {
      changedSubject = newInputs.get(0);
    }
    else if (allConst)
    {
      double v = 0.0;

      for (MapOpHadoop mo : newInputs)
      {
        RenderedImageMapOp child = (RenderedImageMapOp) mo;
        v += _getConstant(child);
      }
      changedSubject = _createConstantMapOp(v);
    }

    if (changedSubject != null)
    {
      result = _replaceNode(newRoot, newSubject, changedSubject);
    }

    return result;
  }

  private MapOpHadoop _applyMultiplicationRule(MapOpHadoop root, RenderedImageMapOp subject)
  {
    MapOpHadoop newRoot = root.clone();
    MapOpHadoop newSubject = _findNewSubject(root, newRoot, subject);
    MapOpHadoop result = null;
    ArrayList<MapOpHadoop> newInputs = new ArrayList<MapOpHadoop>();

    boolean allConst = true;
    boolean zeroConst = false;

    // Create a new list if inputs that excludes all zeros.
    for (MapOpHadoop mo : newSubject.getInputs())
    {
      if (mo instanceof RenderedImageMapOp)
      {
        RenderedImageMapOp child = (RenderedImageMapOp) mo;
        Double v = _getConstant(child);
        if (v == null)
        {
          allConst = false;
          newInputs.add(mo);
        }
        else if (v == 0.0)
        {
          zeroConst = true;
        }
        else if (v != 1.0)
        {
          newInputs.add(mo);
        }
      }
      else
      {
        allConst = false;
        newInputs.add(mo);
      }
    }

    MapOpHadoop changedSubject = null;

    if (zeroConst)
    {
      changedSubject = _createConstantMapOp(0.0);
    }
    // if we don't have any inputs left, replace the subject w/ a zero constant.
    else if (newInputs.size() == 0)
    {
      changedSubject = _createConstantMapOp(1.0);
    }
    else if (newInputs.size() == 1)
    {
      changedSubject = newInputs.get(0);
    }
    else if (allConst)
    {
      double v = 1.0;

      for (MapOpHadoop mo : newInputs)
      {
        RenderedImageMapOp child = (RenderedImageMapOp) mo;
        v *= _getConstant(child);
      }
      changedSubject = _createConstantMapOp(v);
    }

    if (changedSubject != null)
    {
      result = _replaceNode(newRoot, newSubject, changedSubject);
    }

    return result;
  }

  private MapOpHadoop _applySubtractionRule(MapOpHadoop root, RenderedImageMapOp subject)
  {
    MapOpHadoop newRoot = root.clone();
    MapOpHadoop newSubject = _findNewSubject(root, newRoot, subject);
    MapOpHadoop result = null;
    List<MapOpHadoop> newInputs = newSubject.getInputs();

    MapOpHadoop a = newInputs.get(0);
    MapOpHadoop b = newInputs.get(1);
    
    Double av = _getConstant(a);
    Double bv = _getConstant(b);

    MapOpHadoop changedSubject = null;
    
    if (av != null && bv != null)
    {
      double v = av - bv;
      changedSubject = _createConstantMapOp(v);
    }
    else if (bv != null && bv == 0.0)
    {
      changedSubject = newInputs.get(0);
    }

    if (changedSubject != null)
    {
      result = _replaceNode(newRoot, newSubject, changedSubject);
    }

    return result;
  }

  private MapOpHadoop _applyDivisionRule(MapOpHadoop root, RenderedImageMapOp subject)
  {
    MapOpHadoop newRoot = root.clone();
    MapOpHadoop newSubject = _findNewSubject(root, newRoot, subject);
    MapOpHadoop result = null;
    List<MapOpHadoop> newInputs = newSubject.getInputs();

    MapOpHadoop a = newInputs.get(0);
    MapOpHadoop b = newInputs.get(1);
    
    Double av = _getConstant(a);
    Double bv = _getConstant(b);

    MapOpHadoop changedSubject = null;
    
    if (bv != null && bv == 0.0)
    {
      throw new IllegalArgumentException("Division by zero. " + a.toString() + " / " + b.toString());
    }
    if (av != null && bv != null)
    {
      double v = av / bv;
      changedSubject = _createConstantMapOp(v);
    }
    else if (bv != null && bv == 1.0)
    {
      changedSubject = newInputs.get(0);
    }

    if (changedSubject != null)
    {
      result = _replaceNode(newRoot, newSubject, changedSubject);
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
