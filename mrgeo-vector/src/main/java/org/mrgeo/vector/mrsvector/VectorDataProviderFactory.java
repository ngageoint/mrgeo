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

package org.mrgeo.vector.mrsvector;

import org.mrgeo.data.DataProviderNotFound;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

public class VectorDataProviderFactory
{
  private static List<MrsVectorDataProvider> mrsVectorProviders;

  public static MrsVectorDataProvider getMrsVectorDataProvider(final String name) throws DataProviderNotFound
  {
    initialize();
    for (MrsVectorDataProvider dp : mrsVectorProviders)
    {
      // TODO: Need to return the one that matches the requested name
//      if (dp.canOpen(name))
//      {
        return dp;
//      }
    }
    throw new DataProviderNotFound("Unable to find a data provider for " + name);
  }
  
  private static void initialize()
  {
    if (mrsVectorProviders == null)
    {
      mrsVectorProviders = new ArrayList<MrsVectorDataProvider>();
      // Find the mrsImageProviders
      ServiceLoader<MrsVectorDataProvider> dataProviderLoader = ServiceLoader.load(MrsVectorDataProvider.class);
      for (MrsVectorDataProvider dp : dataProviderLoader)
      {
        mrsVectorProviders.add(dp);
      }
    }
  }
}
