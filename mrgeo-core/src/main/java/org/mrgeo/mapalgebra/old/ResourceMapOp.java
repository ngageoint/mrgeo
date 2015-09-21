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

import org.mrgeo.mapreduce.job.JobCancelledException;
import org.mrgeo.mapreduce.job.JobFailedException;
import org.mrgeo.progress.Progress;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.adhoc.AdHocDataProvider;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.utils.HadoopUtils;

import java.io.IOException;

public class ResourceMapOp extends MapOpHadoop implements OutputProducer
{
  protected String _output;
  protected String _outputName = null;

  @Override
  public void addInput(MapOpHadoop n) throws IllegalArgumentException
  {
    throw new IllegalArgumentException("This ExecuteNode takes no arguments.");
  }

  /**
   * Returns the output object w/o blocking. This must be called after determineOutput has
   * completed.
   * 
   * @return
   */
  public String getOutputResourceName()
  {
    return _output;
  }

  @Override
  public void build(Progress p) throws IOException, JobFailedException, JobCancelledException
  {
    if (p != null)
    {
      p.complete();
    }
    _output = _outputName;
  }

  /**
   * Returns the resource name of this map op's output. This is _only_ relevant if the output has
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
   * this method is responsible for moving the content to that location.
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

    AdHocDataProvider dp = DataProviderFactory.getAdHocDataProvider(getOutputName(),
        AccessMode.READ, getProviderProperties());
    if (dp != null)
    {
      dp.move(toName);
    }
    _outputName = toName;
    _output = _outputName;
  }

  @Override
  public String toString()
  {
    return String.format("ResourceMapOp: %s", 
        _outputName == null ? "null": _outputName.toString());
  }

  /**
   * Returns a clone of this MapOp and all its children. This will not include any intermediate
   * results or temporary variables involved in computation.
   */
  @Override
  public ResourceMapOp clone()
  {
    final ResourceMapOp result = (ResourceMapOp) super.clone();
    result._output = null;
    return result;
  }

  @Override
  public void postBuild(Progress p, boolean buildPyramid) throws IOException, JobFailedException,
      JobCancelledException
  {
    p.complete();
  }
}
