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
import org.mrgeo.aggregators.AggregatorRegistry;
import org.mrgeo.aggregators.MeanAggregator;
import org.mrgeo.buildpyramid.BuildPyramidSpark;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.image.MrsImagePyramid;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.mrgeo.image.MrsImagePyramidMetadata.Classification;
import org.mrgeo.mapalgebra.parser.ParserAdapter;
import org.mrgeo.mapalgebra.parser.ParserNode;
import org.mrgeo.mapreduce.job.JobCancelledException;
import org.mrgeo.mapreduce.job.JobFailedException;
import org.mrgeo.opimage.MrsPyramidDescriptor;
import org.mrgeo.progress.Progress;

import java.io.IOException;
import java.util.Vector;

public class ChangeClassificationMapOp extends RasterMapOp
{
  private RasterMapOp source = null;

  private Classification classification;
  Aggregator aggregator = new MeanAggregator();

  @Override
  public void addInput(final MapOp n) throws IllegalArgumentException
  {
    if (source == null)
    {
      if (!(n instanceof RasterMapOp))
      {
        throw new IllegalArgumentException("Only raster inputs are supported for the source input.");
      }
      source = (RasterMapOp)n;
      _inputs.add(n);
    }
  }

  public static String[] register()
  {
    return new String[] { "changeClassification" };
  }

  @Override
  public void moveOutput(final String toName) throws IOException
  {
    super.moveOutput(toName);
    _outputName = toName;
    MrsImageDataProvider dp = DataProviderFactory.getMrsImageDataProvider(_outputName,
        AccessMode.READ, getProviderProperties());
    _output = MrsPyramidDescriptor.create(dp);
  }

  @Override
  public Vector<ParserNode> processChildren(final Vector<ParserNode> children, final ParserAdapter parser)
  {
    final Vector<ParserNode> result = new Vector<ParserNode>();

    if (children.size() < 2)
    {
      throw new IllegalArgumentException(
          "ChangeClassificationMapOp usage: changeClassification(source raster, type, [aggregation type])");
    }

    result.add(children.get(0));

    String cls = parseChildString(children.get(1), "classification type", parser);
    if (cls.equalsIgnoreCase("categorical"))
    {
      classification = Classification.Categorical;
    }
    else
    {
      classification = Classification.Continuous;
    }
    
    if (children.size() > 2)
    {
      if (children.size() == 3)
      {
        String agg = parseChildString(children.get(2), "aggregation type", parser);
        Class<? extends Aggregator> aggregatorClass = AggregatorRegistry.aggregatorRegistry.get(agg.toUpperCase());
        if (aggregatorClass != null)
        {
          try
          {
            aggregator = aggregatorClass.newInstance();
          }
          catch (InstantiationException e)
          {
            throw new IllegalArgumentException("Invalid aggregator " + agg);
          }
          catch (IllegalAccessException e)
          {
            throw new IllegalArgumentException("IllegalAccessException while instantiating aggregator " + agg);
          }
        }
      }
      else
      {
        throw new IllegalArgumentException(
            "ChangeClassificationMapOp usage: changeClassification(source raster, type, [aggregation type])");
      }
    }

    return result;
  }

  @Override
  public String toString()
  {
    return String.format("changetype " + (source == null ? "null" : source.toString()));
  }

  @Override
  public void build(final Progress p) throws IOException, JobFailedException, JobCancelledException
  {
    if (p != null)
    {
      p.starting();
    }

    final MrsImagePyramid sourcepyramid = RasterMapOp.flushRasterMapOpOutput(source, 0);
    
    MrsImageDataProvider provider = DataProviderFactory.getMrsImageDataProvider(sourcepyramid.getName(),
        AccessMode.READ, getProviderProperties());
    MrsImagePyramidMetadata metadata = provider.getMetadataReader().read();

    _outputName = source.getOutputName();
    
    if (metadata.getClassification() != classification)
    {
      metadata.setClassification(classification);
      metadata.setResamplingMethod(AggregatorRegistry.aggregatorRegistry.inverse().get(aggregator.getClass()));
      
      provider.getMetadataWriter().write();

      if (sourcepyramid.hasPyramids())
      {
        try
        {
          BuildPyramidSpark.build(sourcepyramid.getName(),
              aggregator, createConfiguration(), p, jobListener, getProviderProperties());
        }
        catch (Exception e)
        {
          e.printStackTrace();
        }
      }
    }

    MrsImageDataProvider dp = DataProviderFactory.getMrsImageDataProvider(_outputName,
        AccessMode.READ, getProviderProperties());
    _output = MrsPyramidDescriptor.create(dp);

    if (p != null)
    {
      p.complete();
    }

  }

}
