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

package org.mrgeo.mapalgebra.old;

import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.mapalgebra.parser.ParserNode;
import org.mrgeo.mapreduce.job.JobCancelledException;
import org.mrgeo.mapreduce.job.JobFailedException;
import org.mrgeo.opimage.MrsPyramidDescriptor;
import org.mrgeo.progress.Progress;
import org.mrgeo.spark.MosaicDriver;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;

public class MosaicMapOpHadoop extends RasterMapOpHadoop implements InputsCalculator
{
  public static String[] register()
  {
    return new String[] { "mosaic" };
  }

  @Override
  public void addInput(MapOpHadoop n) throws IllegalArgumentException
  {
    if (!(n instanceof RasterMapOpHadoop))
    {
      throw new IllegalArgumentException("Can only mosaic raster inputs");
    }
    _inputs.add(n);

  }

  @Override
  public void build(Progress p) throws IOException, JobFailedException, JobCancelledException
  {
    p.starting();

    // check that we haven't already calculated ourselves
    if (_output == null)
    {
      String[] names = new String[_inputs.size()];
      int i = 0;
      for (MapOpHadoop input : _inputs)
      {
        names[i++] = ((RasterMapOpHadoop) (input)).getOutputName();
      }
      MosaicDriver.mosaic(names, getOutputName(), createConfiguration());

      MrsImageDataProvider dp = DataProviderFactory.getMrsImageDataProvider(getOutputName(),
          DataProviderFactory.AccessMode.READ, getProviderProperties());
      _output = MrsPyramidDescriptor.create(dp);
    }

    p.complete();
  }

  @Override
  public Vector<ParserNode> processChildren(Vector<ParserNode> children, ParserAdapterHadoop parser)
  {
    if (children.size() < 2)
    {
      throw new IllegalArgumentException("Usage: mosaic(<raster input1>, <raster input2>, ...)");
    }

    Vector<ParserNode> results = new Vector<ParserNode>();

    // make sure to keep these in order...  very important
    for (ParserNode child: children)
    {
      results.add(child);
    }

    return results;
  }

@Override
public Set<String> calculateInputs()
{
  Set<String> inputPyramids = new HashSet<String>();
  if (_outputName != null)
  {
    inputPyramids.add(_outputName);
  }
  return inputPyramids;
}

  @Override
  public String toString()
  {
    return "mosaic";
  }
}
