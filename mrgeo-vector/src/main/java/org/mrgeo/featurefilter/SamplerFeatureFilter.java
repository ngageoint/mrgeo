package org.mrgeo.featurefilter;

import org.mrgeo.geometry.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SamplerFeatureFilter extends BaseFeatureFilter
{
  @SuppressWarnings("unused")
  private static final Logger log = LoggerFactory.getLogger(SamplerFeatureFilter.class);

  private static final long serialVersionUID = 1L;
  String _uidColumn;
  double _samplePortion;
  
  /**
   * 
   * @param uidColumn - Column that contains a unique identifier
   * @param samplePortion - Percentage of values to keep -- 0 to 1.
   */
  public SamplerFeatureFilter(String uidColumn, double samplePortion)
  {
    _uidColumn = uidColumn;
    _samplePortion = samplePortion;
  }

  @Override
  public Geometry filterInPlace(Geometry g)
  {
    Geometry result = null;
    double h = Math.abs(g.getAttribute(_uidColumn).hashCode()) % 1000 / (1000.0);
    if (h < _samplePortion)
    {
      result = g;
    }
    return result;
  }
}
