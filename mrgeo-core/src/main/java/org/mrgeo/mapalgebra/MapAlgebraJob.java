/*
 * Copyright 2009-2016 DigitalGlobe, Inc.
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
 *
 */

package org.mrgeo.mapalgebra;

import org.apache.hadoop.conf.Configuration;
import org.mrgeo.aggregators.MeanAggregator;
import org.mrgeo.buildpyramid.BuildPyramid;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.ProtectionLevelUtils;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.mapreduce.job.JobCancelFailedException;
import org.mrgeo.mapreduce.job.RunnableJob;
import org.mrgeo.progress.Progress;
import org.mrgeo.utils.HadoopUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapAlgebraJob implements RunnableJob
{
  private static final Logger _log = LoggerFactory.getLogger(MapAlgebraJob.class);
  String _expression;
  String _output;
  Progress _progress;
  private String protectionLevel;
  private ProviderProperties providerProperties;

  public MapAlgebraJob(String expression, String output,
      final String protectionLevel,
      final ProviderProperties providerProperties)
  {
    _expression = expression;
    _output = output;
    this.protectionLevel = protectionLevel;
    this.providerProperties = providerProperties;
  }
  
  @Override
  public void setProgress(Progress p) {
    _progress = p;
  }
  
  @Override
  public void run()
  {
    try
    {
      _progress.starting();
      boolean valid = org.mrgeo.mapalgebra.MapAlgebra.validate(_expression, providerProperties);
      if (valid) {
        MrsImageDataProvider dp =
                DataProviderFactory.getMrsImageDataProvider(_output, DataProviderFactory.AccessMode.OVERWRITE, providerProperties);
        String useProtectionLevel = ProtectionLevelUtils.getAndValidateProtectionLevel(dp, protectionLevel);
        Configuration conf = HadoopUtils.createConfiguration();
        if (org.mrgeo.mapalgebra.MapAlgebra.mapalgebra(_expression, _output, conf,
                                                   providerProperties, useProtectionLevel)) {
          BuildPyramid.build(_output, new MeanAggregator(), conf, providerProperties);
        }
      }
    }
    catch (Exception e)
    {
      _log.error("Exception occurred while processing mapalgebra job " + e.getMessage(), e);
      _progress.failed(e.getMessage());
    }
  }
}

