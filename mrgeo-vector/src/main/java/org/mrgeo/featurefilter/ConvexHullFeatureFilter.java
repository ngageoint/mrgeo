package org.mrgeo.featurefilter;

import org.mrgeo.geometry.Geometry;
import org.mrgeo.geometry.GeometryFactory;

public class ConvexHullFeatureFilter extends BaseFeatureFilter
{
  private static final long serialVersionUID = 1L;

  @Override
  public Geometry filter(Geometry f)
  {
    return filterInPlace(f);
  }

  @Override
  public Geometry filterInPlace(Geometry f)
  {
    return GeometryFactory.fromJTS(f.toJTS().convexHull());
  }
}
