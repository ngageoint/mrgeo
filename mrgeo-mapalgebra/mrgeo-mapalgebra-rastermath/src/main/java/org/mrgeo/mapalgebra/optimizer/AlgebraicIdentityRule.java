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

import org.mrgeo.mapalgebra.MapOp;
import org.mrgeo.mapalgebra.RawBinaryMathMapOp;
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
  public ArrayList<MapOp> apply(MapOp root, MapOp subject)
  {
    ArrayList<MapOp> result = new ArrayList<MapOp>();

    RenderedImageMapOp ri = (RenderedImageMapOp) subject;
    String s = _getOperation(ri);
    if (s.equals("+"))
    {
      MapOp newRoot = _applyAdditionRule(root, ri);
      if (newRoot != null)
      {
        result.add(newRoot);
      }
    }
    else if (s.equals("*"))
    {
      MapOp newRoot = _applyMultiplicationRule(root, ri);
      if (newRoot != null)
      {
        result.add(newRoot);
      }
    }
    else if (s.equals("-"))
    {
      MapOp newRoot = _applySubtractionRule(root, ri);
      if (newRoot != null)
      {
        result.add(newRoot);
      }
    }
    else if (s.equals("/"))
    {
      MapOp newRoot = _applyDivisionRule(root, ri);
      if (newRoot != null)
      {
        result.add(newRoot);
      }
    }

    return result;
  }

  private MapOp _applyAdditionRule(MapOp root, RenderedImageMapOp subject)
  {
    MapOp newRoot = root.clone();
    MapOp newSubject = _findNewSubject(root, newRoot, subject);
    MapOp result = null;
    ArrayList<MapOp> newInputs = new ArrayList<MapOp>();

    boolean allConst = true;

    // Create a new list if inputs that excludes all zeros.
    for (MapOp mo : newSubject.getInputs())
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

    MapOp changedSubject = null;

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

      for (MapOp mo : newInputs)
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

  private MapOp _applyMultiplicationRule(MapOp root, RenderedImageMapOp subject)
  {
    MapOp newRoot = root.clone();
    MapOp newSubject = _findNewSubject(root, newRoot, subject);
    MapOp result = null;
    ArrayList<MapOp> newInputs = new ArrayList<MapOp>();

    boolean allConst = true;
    boolean zeroConst = false;

    // Create a new list if inputs that excludes all zeros.
    for (MapOp mo : newSubject.getInputs())
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

    MapOp changedSubject = null;

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

      for (MapOp mo : newInputs)
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

  private MapOp _applySubtractionRule(MapOp root, RenderedImageMapOp subject)
  {
    MapOp newRoot = root.clone();
    MapOp newSubject = _findNewSubject(root, newRoot, subject);
    MapOp result = null;
    List<MapOp> newInputs = newSubject.getInputs();

    MapOp a = newInputs.get(0);
    MapOp b = newInputs.get(1);
    
    Double av = _getConstant(a);
    Double bv = _getConstant(b);

    MapOp changedSubject = null;
    
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

  private MapOp _applyDivisionRule(MapOp root, RenderedImageMapOp subject)
  {
    MapOp newRoot = root.clone();
    MapOp newSubject = _findNewSubject(root, newRoot, subject);
    MapOp result = null;
    List<MapOp> newInputs = newSubject.getInputs();

    MapOp a = newInputs.get(0);
    MapOp b = newInputs.get(1);
    
    Double av = _getConstant(a);
    Double bv = _getConstant(b);

    MapOp changedSubject = null;
    
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
