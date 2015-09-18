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

package org.mrgeo.mapalgebra;

import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.mapalgebra.old.MapOpHadoop;
import org.mrgeo.mapalgebra.old.ResourceMapOpLoader;

import java.io.IOException;

public class RasterMapOpLoader implements ResourceMapOpLoader
{

  @Override
  public MapOpHadoop loadMapOpFromResource(String resourceName,
      final ProviderProperties providerProperties)
  {
    MrsImageDataProvider dp = null;
    try
    {
      dp = DataProviderFactory.getMrsImageDataProvider(resourceName, AccessMode.READ,
          providerProperties);
    }
    catch(IOException e)
    {
      // Ignore. The inability to load the pyramid is handled below.
    }
    if (dp != null)
    {
      MrsPyramidMapOp mapop = new MrsPyramidMapOp();
      mapop.setDataProvider(dp);

      return mapop;
    }
    return null;
  }
}
