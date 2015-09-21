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
import org.mrgeo.mapalgebra.old.RenderedImageMapOp;
import org.mrgeo.opimage.ConstantDescriptor;

import java.awt.image.renderable.RenderedImageFactory;

/**
 * A basic rule base class with some convenience methods.
 */
public abstract class AlgebraicRule extends BasicRule
{
  protected RenderedImageMapOp _createConstantMapOp(double v)
  {
    RenderedImageMapOp result = new RenderedImageMapOp();
    result.setRenderedImageFactory(new ConstantDescriptor());
    result.getParameters().add(v);
    return result;
  }

  /**
   * Returns the algebraic operation for a given node. (e.g. +, -, *, etc.)
   * 
   * @param ri
   *          The node to look at.
   * @return The string short name of the node or empty string if it is not
   *         applicable.
   */
  protected String _getOperation(MapOpHadoop mo)
  {
    String result = mo.getFunctionName();
    if (result == null)
    {
      result = "";
    }

    return result;
  }

  /**
   * @param ri
   *          MapOp to retrieve constant on
   * @return Returns the constant or null if it is not a constant.
   */
  protected Double _getConstant(MapOpHadoop mo)
  {
    Double result = null;
    if (mo instanceof RenderedImageMapOp)
    {
      RenderedImageMapOp ri = (RenderedImageMapOp) mo;

      RenderedImageFactory f = ri.getRenderedImageFactory();

      if (f instanceof ConstantDescriptor)
      {
        result = ri.getParameters().getDoubleParameter(0);
      }
    }

    return result;
  }

}
