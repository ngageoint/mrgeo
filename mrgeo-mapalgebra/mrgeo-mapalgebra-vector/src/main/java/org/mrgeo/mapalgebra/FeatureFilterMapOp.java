package org.mrgeo.mapalgebra;

import org.mrgeo.featurefilter.FeatureFilter;
import org.mrgeo.progress.Progress;

import java.io.IOException;

public abstract class FeatureFilterMapOp extends VectorMapOp
{
  @Override
  public void addInput(MapOp n) throws IllegalArgumentException
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

    MapOp mo = _inputs.get(0);

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
