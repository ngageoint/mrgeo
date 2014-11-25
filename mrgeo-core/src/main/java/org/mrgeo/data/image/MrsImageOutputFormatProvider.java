/*
 * Copyright 2009-2014 DigitalGlobe, Inc.
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

package org.mrgeo.data.image;

import org.apache.hadoop.mapreduce.Job;
import org.mrgeo.data.DataProviderException;
import org.mrgeo.data.tile.TiledOutputFormatContext;
import org.mrgeo.data.tile.TiledOutputFormatProvider;
import org.mrgeo.utils.HadoopUtils;

import java.io.IOException;

/**
 * Data plugins that wish to provide storage for image pyramids must
 * include a sub-class of this class.
 */
public abstract class MrsImageOutputFormatProvider implements TiledOutputFormatProvider
{
  protected TiledOutputFormatContext context;

  public MrsImageOutputFormatProvider(TiledOutputFormatContext context)
  {
    this.context = context;
  }

  /**
   * Sub-classes that override this method must call super.setupJob(job).
   */
  @Override
  public void setupJob(Job job) throws DataProviderException, IOException
  {
    try
    {
      HadoopUtils.addJarCache(job, getClass());
    }
    catch(IOException e)
    {
      throw new DataProviderException(e);
    }
  }
}
