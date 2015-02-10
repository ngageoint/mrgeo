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

import org.mrgeo.mapalgebra.parser.ParserAdapter;
import org.mrgeo.mapalgebra.parser.ParserNode;
import org.mrgeo.mapreduce.job.JobCancelledException;
import org.mrgeo.mapreduce.job.JobFailedException;
import org.mrgeo.progress.Progress;
import org.mrgeo.spark.MosaicDriver;

import java.io.IOException;
import java.util.Vector;

public class MosaicMapOp extends RasterMapOp
{
  public static String[] register()
  {
    return new String[] { "mosaic" };
  }

  @Override
  public void addInput(MapOp n) throws IllegalArgumentException
  {
    if (!(n instanceof RasterMapOp))
    {
      throw new IllegalArgumentException("Can only mosaic raster inputs");
    }
    _inputs.add(n);

  }

  @Override
  public void build(Progress p) throws IOException, JobFailedException, JobCancelledException
  {
    p.starting();

    String[] names = new String[_inputs.size()];
    int i = 0;
    for (MapOp input:_inputs)
    {
      names[i++] = ((RasterMapOp)(input)).getOutputName();
    }
    MosaicDriver.mosaic(names, getOutputName());

    p.complete();
  }

  @Override
  public Vector<ParserNode> processChildren(Vector<ParserNode> children, ParserAdapter parser)
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
  public String toString()
  {
    return "mosaic";
  }
}
