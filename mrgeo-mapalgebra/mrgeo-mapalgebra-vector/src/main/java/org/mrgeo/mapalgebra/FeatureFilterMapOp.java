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

import org.mrgeo.featurefilter.FeatureFilter;
import org.mrgeo.progress.Progress;

import java.io.IOException;

public abstract class FeatureFilterMapOp extends VectorMapOp
{
  @Override
  public void addInput(MapOpHadoop n) throws IllegalArgumentException
  {
    if (!(n instanceof VectorMapOp))
    {
      throw new IllegalArgumentException("Only vector inputs are supported.");
    }
    if (_inputs.size() != 0)
    {
      throw new IllegalArgumentException("Only one input is supported.");
    }
    _inputs.add(n);
  }

  @Override
  public void build(Progress p) throws IOException
  {
    if (p != null)
    {
      p.starting();
    }

    MapOpHadoop mo = _inputs.get(0);

    InputFormatDescriptor result = 
      new FilteredInputFormatDescriptor(((VectorMapOp)mo).getVectorOutput(), getFilter());

    _output = result;
    
    if (p != null)
    {
      p.complete();
    }
  }

  public abstract FeatureFilter getFilter();
}
