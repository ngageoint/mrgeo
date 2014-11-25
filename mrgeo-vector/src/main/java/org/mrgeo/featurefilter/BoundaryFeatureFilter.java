package org.mrgeo.featurefilter;


import org.mrgeo.geometry.Geometry;
import org.mrgeo.geometry.GeometryFactory;

public class BoundaryFeatureFilter extends BaseFeatureFilter
{
  private static final long serialVersionUID = 1L;

  @Override
  public Geometry filter(Geometry f)
  {
    // this filter in place creates a new feature, so we're doing this for
    // performance.
    return filterInPlace(f);
  }

  @Override
  public Geometry filterInPlace(Geometry f)
  {
    return GeometryFactory.fromJTS(f.toJTS().getBoundary(), f.getAllAttributes());
  }
}
