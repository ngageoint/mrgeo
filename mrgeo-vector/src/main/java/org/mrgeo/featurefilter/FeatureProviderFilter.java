/*
 * Copyright 2009-2015 DigitalGlobe, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

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
