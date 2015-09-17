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

package org.mrgeo.pig;

import org.apache.hadoop.fs.Path;
import org.mrgeo.mapalgebra.BasicInputFormatDescriptor;
import org.mrgeo.mapalgebra.InputFormatDescriptor;
import org.mrgeo.mapalgebra.old.VectorMapOpHadoop;
import org.mrgeo.mapalgebra.old.MapOpHadoop;
import org.mrgeo.mapreduce.job.JobCancelledException;
import org.mrgeo.mapreduce.job.JobFailedException;
import org.mrgeo.progress.Progress;
import org.mrgeo.progress.ProgressHierarchy;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.utils.HadoopUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;

public class PigMapOp extends VectorMapOpHadoop
{
  private static final Logger log = LoggerFactory.getLogger(PigMapOp.class);
  private static String _script;
  private ArrayList<Path> _inputPaths;
  
  @Override
  public void addInput(MapOpHadoop n) throws IllegalArgumentException
  {
    if (!(n instanceof VectorMapOpHadoop))
    {
      throw new IllegalArgumentException("Only vector inputs are supported.");
    }
    _inputs.add(n);
  }

  public static String[] register()
  {
    // returning a "" means don't register this MapOp
    return new String[] { "" };
  }


  /**
   * Recursively traverse the root node in-order.
   * http://en.wikipedia.org/wiki/Tree_traversal#Example
   * 
   * If any node is missing an input, connect the input to the MapOp input. All
   * inputs should be on disk when this is called. If the number of inputs
   * needed doesn't match the number available an exception will be thrown.
   */
  protected void _connectInputs()
  {
    int i = 1;
    for (Path p : _inputPaths)
    {
      String key = String.format("{%%%d}", i);
      _script = _script.replace(key, p.toString());
      i++;
    }
  }

  /**
   * Go through each of the MapOp inputs and write the values out to disk. If
   * the values are already on disk then this can be skipped. This may take a
   * while.
   * 
   * @throws IOException
   * @throws JobFailedException
   */
  protected void _writeInputs(Progress p) throws IOException, JobFailedException, JobCancelledException
  {
    ProgressHierarchy ph = new ProgressHierarchy(p);
    for (MapOpHadoop input : _inputs)
    {
      if (input != null && (input instanceof VectorMapOpHadoop))
      {
        if (((VectorMapOpHadoop)input).getOutputName() == null)
        {
          ph.createChild(1.0f);
        }
      }
    }

    _inputPaths = new ArrayList<Path>();

    int pi = 0;
    // go through all the inputs
    for (MapOpHadoop input : _inputs)
    {
      if (input != null && (input instanceof VectorMapOpHadoop))
      {
        Path inputPath = new Path(((VectorMapOpHadoop)input).getOutputName());
  
        // if the inputPath doesn't exist then we need to calculate it.
        if (inputPath == null)
        {
          inputPath = new Path(HadoopFileUtils.getTempDir(), HadoopUtils.createRandomString(40) + ".tsv");
          addTempFile(inputPath.toString());
          addTempFile(inputPath.toString() + ".columns");
  
  //        MapAlgebraExecutionerv1.writeVectorOutput((InputFormatDescriptor) input.getOutput(),
  //          inputPath, getConf(), ph.getChild(pi++), jobListener);
          VectorMapOpHadoop.writeVectorOutput((InputFormatDescriptor) ((VectorMapOpHadoop) input).getVectorOutput(),
                                              inputPath, createConfiguration(), ph.getChild(pi++));
        }

        _inputPaths.add(inputPath);
      }
    }
  }

  @Override
  public void build(Progress p) throws IOException, 
  JobFailedException, JobCancelledException
  {
    ProgressHierarchy ph = new ProgressHierarchy(p);
    ph.createChild(1f);
    ph.createChild(1f);

    // Make sure all the inputs are on disk
    _writeInputs(ph.getChild(0));

    // Connect the MapOp inputs to Pig Inputs
    _connectInputs();

    PigQuerier pq = new PigQuerier();
    log.warn(_script);
    log.warn(_outputName.toString());
    pq.query(_script, new Path(_outputName), createConfiguration());
    ph.getChild(1).complete();
    _output = new BasicInputFormatDescriptor(_outputName);
  }
  
  public static void setScript(String script)
  {
    _script = script;
  }

  public void setScriptFile(String file) throws FileNotFoundException, IOException
  {
    File f = new File(file);
    
    byte[] buffer = new byte[(int)f.length()];
    
    FileInputStream fis = new FileInputStream(f);
    fis.read(buffer);
    fis.close();

    setScript(new String(buffer));
  }
  
  
  @Override
  public String toString()
  {
    return String.format("PigMapOp %s",
        _outputName == null ? "null" : _outputName.toString() );
  }
}
