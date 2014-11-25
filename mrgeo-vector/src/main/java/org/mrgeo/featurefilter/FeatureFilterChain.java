package org.mrgeo.featurefilter;


import org.mrgeo.geometry.Geometry;

import java.util.ArrayList;

public class FeatureFilterChain extends BaseFeatureFilter
{
  private static final long serialVersionUID = 1L;
  ArrayList<FeatureFilter> filterList = new ArrayList<FeatureFilter>();

  public FeatureFilterChain(FeatureFilter... filters)
  {
    for (FeatureFilter f : filters)
    {
      filterList.add(f);
    }
  }
  
  public void add(FeatureFilter filter)
  {
    filterList.add(filter);
  }

  @Override
  public Geometry filterInPlace(Geometry g)
  {
    Geometry result = g;
    
    for (FeatureFilter f : filterList)
    {
      if (result != null)
      {
        result = f.filterInPlace(result);
      }
    }

    return result;
  }
}
