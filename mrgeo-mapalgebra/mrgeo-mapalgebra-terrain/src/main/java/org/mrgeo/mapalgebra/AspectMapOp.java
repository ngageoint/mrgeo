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
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.mapalgebra.old.MapOpHadoop;
import org.mrgeo.mapalgebra.old.ParserAdapterHadoop;
import org.mrgeo.mapalgebra.old.RasterMapOpHadoop;
import org.mrgeo.mapalgebra.parser.ParserNode;
import org.mrgeo.mapreduce.job.JobCancelledException;
import org.mrgeo.mapreduce.job.JobFailedException;
import org.mrgeo.opimage.MrsPyramidDescriptor;
import org.mrgeo.progress.Progress;
import org.mrgeo.spark.AspectDriver;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;

public class AspectMapOp extends RasterMapOpHadoop implements InputsCalculator
{
String units = "rad";

public static String[] register()
{
  return new String[] { "aspect" };
}

@Override
public void addInput(MapOpHadoop n) throws IllegalArgumentException
{
  if (!(n instanceof RasterMapOpHadoop))
  {
    throw new IllegalArgumentException("Can only run aspect() on raster inputs");
  }
  if (_inputs.size() >= 1)
  {
    throw new IllegalArgumentException("Can only run aspect() on a single raster input");
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

    String input = ((RasterMapOpHadoop) _inputs.get(0)).getOutputName();

    AspectDriver.aspect(input, units, getOutputName(), createConfiguration());

    MrsImageDataProvider dp = DataProviderFactory.getMrsImageDataProvider(getOutputName(),
        DataProviderFactory.AccessMode.READ, getProviderProperties());
    _output = MrsPyramidDescriptor.create(dp);
  }
  p.complete();
}

@Override
public Vector<ParserNode> processChildren(Vector<ParserNode> children, ParserAdapterHadoop parser)
{
  Vector<ParserNode> result = new Vector<ParserNode>();

  if (children.size() > 2)
  {
    throw new IllegalArgumentException(
        "Aspect takes one or two arguments. single-band raster elevation and optional unit format (\"deg\" or \"rad\")");
  }

  result.add(children.get(0));

  if (children.size() == 2)
  {
    String units = MapOpHadoop.parseChildString(children.get(1), "units", parser);
    if (!(units.equalsIgnoreCase("deg") || units.equalsIgnoreCase("rad")))
    {
      throw new IllegalArgumentException("Units must be \"deg\", or \"rad\".");
    }
    this.units = units;
  }

  return result;
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
  return "aspect()";
}
}