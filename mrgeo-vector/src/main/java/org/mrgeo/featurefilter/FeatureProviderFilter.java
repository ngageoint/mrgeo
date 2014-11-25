package org.mrgeo.featurefilter;

import org.mrgeo.data.FeatureIterator;
import org.mrgeo.data.FeatureProvider;
import org.mrgeo.geometry.Geometry;

import java.io.IOException;

public class FeatureProviderFilter implements FeatureProvider
{
  private static final long serialVersionUID = 1L;
  FeatureProvider fp;
  FeatureFilter filter;

  public FeatureProviderFilter(FeatureProvider fp, FeatureFilter filter)
  {
    this.fp = fp;
    this.filter = filter;
  }

  @Override
  public FeatureIterator iterator()
  {
    try
    {
      return new FeatureProviderFilterIterator(fp.iterator(), filter);
    }
    catch (Exception e)
    {
      throw new RuntimeException(e);
    }
  }

  public class FeatureProviderFilterIterator extends SimplifiedFeatureIterator
  {
    FeatureIterator it;
    //FeatureFilter filter;
    //Feature current;
    //boolean primed;

    FeatureProviderFilterIterator(FeatureIterator iter, FeatureFilter featureFilter)
    {
      it = iter;
      filter = featureFilter;
    }
    
    @Override
    public void close() throws IOException
    {
      it.close();
    }

    @Override
    public Geometry simpleNext()
    {
      Geometry result = null;
      while (result == null && it.hasNext())
      {
        result = filter.filterInPlace(it.next());
      }
      return result;
    }
  }
}
