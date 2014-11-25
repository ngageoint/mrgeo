package org.mrgeo.featurefilter;

import org.mrgeo.geometry.Geometry;

public abstract class BaseFeatureFilter implements FeatureFilter
{
  private static final long serialVersionUID = 1L;

  @Override
  public Geometry filter(Geometry f)
  {
    return filterInPlace(f.createWritableClone());
  }
  
  @Override
  public abstract Geometry filterInPlace(Geometry g);
}