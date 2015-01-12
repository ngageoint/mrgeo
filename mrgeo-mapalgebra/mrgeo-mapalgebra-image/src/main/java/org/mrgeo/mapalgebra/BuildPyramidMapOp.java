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

import java.awt.image.RenderedImage;
import java.io.IOException;
import java.util.Vector;

import org.mrgeo.aggregators.Aggregator;
import org.mrgeo.aggregators.AggregatorRegistry;
import org.mrgeo.buildpyramid.BuildPyramidDriver;
import org.mrgeo.image.MrsImagePyramid;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.mrgeo.image.MrsImagePyramidMetadata.Classification;
import org.mrgeo.mapalgebra.parser.ParserAdapter;
import org.mrgeo.mapalgebra.parser.ParserNode;
import org.mrgeo.mapreduce.job.JobCancelledException;
import org.mrgeo.mapreduce.job.JobFailedException;
import org.mrgeo.progress.Progress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BuildPyramidMapOp extends RasterMapOp
{
  private static Logger log = LoggerFactory.getLogger(BuildPyramidMapOp.class);

  private String aggregatorName;
  private RasterMapOp sourceRaster;
  
  @Override
  public void addInput(MapOp n) throws IllegalArgumentException
  {
    if (!(n instanceof RasterMapOp))
    {
      throw new IllegalArgumentException("Can only build pyramid for a raster input");
    }
    _inputs.add(n);
    sourceRaster = (RasterMapOp)n;
  }

  @Override
  public void build(Progress p) throws IOException, JobFailedException,
      JobCancelledException
  {
    MrsImagePyramid inputPyramid = RasterMapOp.flushRasterMapOpOutput(sourceRaster, 0);
    if (aggregatorName == null)
    {
      MrsImagePyramidMetadata.Classification classification = inputPyramid.getMetadata().getClassification();
      if (classification.equals(Classification.Categorical))
      {
        aggregatorName = "mode";
      }
      else
      {
        aggregatorName = "mean";
      }
    }
    p.starting();
    Class<? extends Aggregator> aggregatorClass = null;
    aggregatorClass = AggregatorRegistry.aggregatorRegistry.get(aggregatorName.toUpperCase());
    if (aggregatorClass == null)
    {
      throw new IllegalArgumentException("Unable to instantiate aggregator for " + aggregatorName);
    }
    String rasterInput = sourceRaster.getOutputName();
    try
    {
      BuildPyramidDriver.build(rasterInput, aggregatorClass.newInstance(), createConfiguration(),
          getProviderProperties());
    }
    catch (InstantiationException e)
    {
      String msg = "Unable to instantiate aggregator \"" + aggregatorName + "\" while building pyramid for " + rasterInput;
      log.error(msg);
      throw new JobFailedException(msg);
    }
    catch (IllegalAccessException e)
    {
      log.error("Unable to build pyramid for " + rasterInput + ": " + e);
      throw new JobFailedException("Unable to build pyramid for " + rasterInput);
    }
    catch (Exception e)
    {
      e.printStackTrace();
      log.error("Unable to build pyramid for " + rasterInput + ": " + e);
      throw new JobFailedException("Unable to build pyramid for " + rasterInput);
    }
    p.complete();
  }

  @Override
  public RenderedImage getRasterOutput() throws IOException
  {
    return sourceRaster.getRasterOutput();
  }

  @Override
  public String getOutputName()
  {
    return sourceRaster.getOutputName();
  }

  @Override
  public void moveOutput(String toName) throws IOException
  {
    sourceRaster.moveOutput(toName);
    _outputName = toName;
  }

  @Override
  public boolean isTempFile(final String p)
  {
    return (sourceRaster.isTempFile(p) || super.isTempFile(p));
  }

  @Override
  public Vector<ParserNode> processChildren(Vector<ParserNode> children, ParserAdapter parser)
  {
    if (children.size() < 1 || children.size() > 2)
    {
      throw new IllegalArgumentException("Usage: BuildPyramid(<raster input>, [aggregator])");
    }
    Vector<ParserNode> results = new Vector<ParserNode>();
    results.add(children.get(0));
    if (children.size() == 2)
    {
      aggregatorName = MapOp.parseChildString(children.get(1), "aggregator", parser);
    }
    return results;
  }

  @Override
  public String toString()
  {
    return "BuildPyramid for " + sourceRaster.getOutputName();
  }
}
