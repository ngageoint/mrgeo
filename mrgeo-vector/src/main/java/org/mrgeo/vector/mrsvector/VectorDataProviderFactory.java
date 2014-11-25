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
