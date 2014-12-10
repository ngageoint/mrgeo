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

package org.mrgeo.vector.mrsvector.mapalgebra;

import org.apache.hadoop.mapreduce.Job;
import org.mrgeo.featurefilter.FeatureFilter;
import org.mrgeo.featurefilter.FeatureFilterChain;
import org.mrgeo.format.AutoFeatureInputFormat;
import org.mrgeo.format.FilteredFeatureInputFormat;
import org.mrgeo.mapalgebra.InputFormatDescriptor;
import org.mrgeo.mapreduce.formats.TileClusterInfo;
import org.mrgeo.vector.mrsvector.MrsVectorPyramid;
import org.mrgeo.utils.Bounds;
import org.mrgeo.vector.formats.HdfsMrsVectorPyramidInputFormat;

import java.io.IOException;
import java.util.Set;

public class HdfsMrsVectorPyramidInputFormatDescriptor implements InputFormatDescriptor
{
  private FeatureFilterChain _chain = new FeatureFilterChain();
  private Set<String> inputs;
  private int zoomLevel;
  // TODO: Do something useful with overallTileClusterInfo. Probably need to
  // pass it to HdfsMrsVectorPyramidInputFormat.setInputInfo().
  private TileClusterInfo overallTileClusterInfo;

  public HdfsMrsVectorPyramidInputFormatDescriptor(final int zoomLevel, final Set<String> inputs,
      final TileClusterInfo overallTileClusterInfo)
  {
    this.inputs = inputs;
    this.zoomLevel = zoomLevel;
    this.overallTileClusterInfo = overallTileClusterInfo;
  }

  public int getZoomLevel()
  {
    return zoomLevel;
  }

  @Override
  public FeatureFilter getFilter()
  {
    return _chain;
  }

  @Override
  public void populateJobParameters(Job job) throws IOException
  {
    HdfsMrsVectorPyramidInputFormat.setInputInfo(job, zoomLevel, inputs);
    FilteredFeatureInputFormat.setInputFormat(job.getConfiguration(), AutoFeatureInputFormat.class);
    FilteredFeatureInputFormat.setFeatureFilter(job.getConfiguration(), _chain);
  }
  
  public Bounds calculateBounds() throws IOException
  {
    Bounds bounds = new Bounds();
    for (String input : inputs)
    {
      MrsVectorPyramid pyramid = MrsVectorPyramid.loadPyramid(input);
      if (pyramid != null)
      {
        bounds.expand(pyramid.getBounds());
      }
    }
    return bounds;
  }
}
