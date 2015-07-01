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

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.fs.Path;
import org.mrgeo.mapalgebra.parser.ParserAdapter;
import org.mrgeo.mapalgebra.parser.ParserNode;
import org.mrgeo.mapreduce.RandomizeVectorDriver;
import org.mrgeo.mapreduce.job.JobCancelledException;
import org.mrgeo.mapreduce.job.JobFailedException;
import org.mrgeo.progress.Progress;
import org.mrgeo.progress.ProgressHierarchy;

public class RandomizeVectorMapOp extends VectorMapOp
{
//  private static final Logger _log = LoggerFactory.getLogger(RandomizeVectorMapOp.class);

  public static String[] register()
  {
    return new String[] { "RandomizeVector" };
  }

  @Override
  public void addInput(MapOp n) throws IllegalArgumentException
  {
    if (_inputs.size() == 0)
    {
      if (!(n instanceof VectorMapOp))
      {
        throw new IllegalArgumentException("The first parameter must be a vector input.");
      }
      _inputs.add(n);
    }
    else
    {
      throw new IllegalArgumentException("Only one input is supported.");
    }
  }

  @Override
  public void build(Progress p) throws IOException, JobFailedException, JobCancelledException
  {
    ProgressHierarchy ph = new ProgressHierarchy(p);
    ph.createChild(1.0f);
    ph.createChild(1.0f);

    RandomizeVectorDriver rvd = new RandomizeVectorDriver();
    MapOp inputMapOp = _inputs.get(0);
    String input = null;
    if (inputMapOp instanceof VectorMapOp)
    {
      input = ((VectorMapOp)inputMapOp).getOutputName();
    }
    else
    {
      // defensive code since all inputs should be VectorMapOp's - see addInput()
      throw new IllegalArgumentException("Invalid value for vector argument to RandomizeVector");
    }
    rvd.run(createConfiguration(), input, new Path(_outputName), p, jobListener,
        getProviderProperties());
    _output = new BasicInputFormatDescriptor(_outputName);
  }

  @Override
  public void moveOutput(String toName) throws IOException
  {
    super.moveOutput(toName);
    _outputName = toName;
    _output = new BasicInputFormatDescriptor(_outputName);
  }

  @Override
  public Vector<ParserNode> processChildren(final Vector<ParserNode> children, final ParserAdapter parser)
  {
    Vector<ParserNode> result = new Vector<ParserNode>();
    if (children.size() != 1)
    {
      throw new IllegalArgumentException(
          "RandomizeVector takes a single parameter - the path to the input vector to randomize");
    }
    result.add(children.get(0));
    return result;
  }

  @Override
  public String toString()
  {
    return String.format("RandomizeVectorMapOp %s",
        _outputName == null ? "null" : _outputName.toString() );
  }


}
