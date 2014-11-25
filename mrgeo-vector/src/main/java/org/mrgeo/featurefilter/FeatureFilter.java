package org.mrgeo.featurefilter;

import org.mrgeo.geometry.Geometry;

import java.io.Serializable;

public interface FeatureFilter extends Serializable
{
  public Geometry filter(Geometry g);
  
  /**
   * Filters the feature in place if possible and returns the result.
   * @param g
   */
  public Geometry filterInPlace(Geometry g);
}
