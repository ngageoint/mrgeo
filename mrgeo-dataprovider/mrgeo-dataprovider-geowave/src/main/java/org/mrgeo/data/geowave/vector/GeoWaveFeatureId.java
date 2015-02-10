package org.mrgeo.data.geowave.vector;

import org.mrgeo.data.vector.VectorFeatureId;

public class GeoWaveFeatureId implements VectorFeatureId
{
  private String featureId;
  
  public GeoWaveFeatureId(String featureId)
  {
    this.featureId = featureId;
  }

  public String getFeatureId()
  {
    return featureId;
  }
  
  @Override
  public String toString()
  {
    return featureId;
  }
}
