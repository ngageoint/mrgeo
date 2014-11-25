package org.mrgeo.mapalgebra;

import org.mrgeo.featurefilter.ConvexHullFeatureFilter;
import org.mrgeo.featurefilter.FeatureFilter;

public class ConvexHullMapOp extends FeatureFilterMapOp
{
  ConvexHullFeatureFilter _filter = new ConvexHullFeatureFilter();
  
  @Override
  public FeatureFilter getFilter()
  {
    return _filter;
  }
  
  public static String[] register()
  {
    return new String[] { "convexhull" };
  }

  @Override
  public String toString()
  {
    return String.format("ConvexHullMapOp %s",
       _outputName == null ? "null" : _outputName.toString() );
  }

}
