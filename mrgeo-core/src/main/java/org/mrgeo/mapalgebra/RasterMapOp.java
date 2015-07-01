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

package org.mrgeo.mapalgebra;

import org.mrgeo.aggregators.Aggregator;
import org.mrgeo.aggregators.MeanAggregator;
import org.mrgeo.aggregators.ModeAggregator;
import org.mrgeo.buildpyramid.BuildPyramidSpark;
import org.mrgeo.image.MrsImagePyramid;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.mrgeo.image.MrsImagePyramidMetadata.Classification;
import org.mrgeo.mapreduce.job.JobCancelledException;
import org.mrgeo.mapreduce.job.JobFailedException;
import org.mrgeo.progress.Progress;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.utils.HadoopUtils;

import java.awt.image.RenderedImage;
import java.io.IOException;

/**
 * Serves as a base class for all MapOp's that return a raster. Note that subclasses
 * should also determine whether or not to implement interfaces used during the
 * processing of map algebra, including BoundsCalculator, InputsCalculator,
 * MaximumZoomLevelCalculator, TileClusterInfoCalculator, and TileSizeCalculator.
 * Please refer to javadocs for those interfaces to determine if a subclassed MapOp
 * should implement them.
 */
public abstract class RasterMapOp extends MapOp implements OutputProducer
{
  /**
   * Creates a new empty pyramid and puts it on the temporary file list.
   * 
   * @param image
   * @return
   * @throws IOException 
   */
  public static MrsImagePyramid flushRasterMapOpOutput(MapOp op, int argumentNumber)
      throws IOException, JobFailedException, JobCancelledException
  {
    if (!(op instanceof RasterMapOp))
    {
      throw new IllegalArgumentException("Expected raster input data for argument " +
          argumentNumber);
    }

    RasterMapOp rasterOp = (RasterMapOp)op;
    // if our source is a file, then use it directly.
    if (rasterOp.getOutputName() == null)
    {
      throw new IllegalArgumentException("Invalid raster input data - no resource name " +
          "for argument " + argumentNumber);
    }
    return MrsImagePyramid.open(rasterOp.getOutputName(), op.getProviderProperties());
  }

  protected RenderedImage _output;
  protected String _outputName = null;

  /**
   * Returns the output object w/o blocking. This must be called after determineOutput has
   * completed.
   * 
   * @return
   * @throws IOException 
   */
  public RenderedImage getRasterOutput() throws IOException
  {
    return _output;
  }

  @Override
  public void postBuild(Progress p, boolean buildPyramid) throws IOException,
  JobFailedException, JobCancelledException
  {
    if (buildPyramid)
    {
      MrsImagePyramid pyramid = MrsImagePyramid.open(_outputName, getProviderProperties());
      MrsImagePyramidMetadata metadata = pyramid.getMetadata();

      Aggregator aggregator;
      if (metadata.getClassification() == Classification.Continuous)
      {
        aggregator = new MeanAggregator();
      }
      else
      {
        aggregator = new ModeAggregator();
      }
      BuildPyramidSpark.build(_outputName, aggregator, createConfiguration(), getProviderProperties());
    }
    p.complete();
  }

  /**
   * Returns the output resource name. This is _only_ relevant if the output has
   * already been calculated or it exists on disk for some other reason. If it doesn't exist and
   * needs to be calculated this method should return null. This is for optimization purposes only,
   * do not assume that this will be non-null.
   */
  @Override
  public String getOutputName()
  {
    return _outputName;
  }

  @Override
  public void setOutputName(final String output)
  {
    _outputName = output;
  }

  public String resolveOutputName() throws IOException
  {
    if (_outputName == null)
    {
      MrsImageDataProvider dp = DataProviderFactory.getMrsImageDataProvider(
          HadoopUtils.createRandomString(40),
          AccessMode.OVERWRITE,
          getProviderProperties());
      _outputName = dp.getResourceName();
      addTempResource(_outputName);
    }
    return _outputName;
  }

  /**
   * After a map op chain is executed, moveOutput will be called on the root map op. By default, the
   * map op's output is stored in a location other than where its final resting place will be, and
   * this method is responsible for moving the content to that location (e.g. toName).
   * 
   * @param fs
   * @param toName
   * @throws IOException
   */
  @Override
  public void moveOutput(final String toName) throws IOException
  {
    // make sure the toName doesn't exist, otherwise the move() will move the output into a directory
    // under the toName.
    DataProviderFactory.delete(toName, getProviderProperties());

    MrsImageDataProvider dp = DataProviderFactory.getMrsImageDataProvider(getOutputName(),
        AccessMode.READ, getProviderProperties());
    if (dp != null)
    {
      dp.move(toName);
    }
    _outputName = toName;
  }

  /**
   * Returns a clone of this MapOp and all its children. This will not include any intermediate
   * results or temporary variables involved in computation.
   */
  @Override
  public RasterMapOp clone()
  {
    final RasterMapOp result = (RasterMapOp) super.clone();
    result._output = null;
    return result;
  }
}
