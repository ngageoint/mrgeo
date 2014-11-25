package org.mrgeo.mapalgebra;

import org.mrgeo.featurefilter.BoundaryFeatureFilter;
import org.mrgeo.featurefilter.FeatureFilter;

public class BoundaryMapOp extends FeatureFilterMapOp
{
  BoundaryFeatureFilter _filter = new BoundaryFeatureFilter();
  
  @Override
  public FeatureFilter getFilter()
  {
    return _filter;
  }
  
  public static String[] register()
  {
    return new String[] { "boundary" };
  }

  @Override
  public String toString()
  {
    return String.format("BoundaryMapOp %s",
       _outputName == null ? "null" : _outputName.toString() );
  }
}
