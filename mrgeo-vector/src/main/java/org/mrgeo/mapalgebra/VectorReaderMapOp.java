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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.vector.VectorDataProvider;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.mapreduce.job.JobCancelledException;
import org.mrgeo.mapreduce.job.JobFailedException;
import org.mrgeo.progress.Progress;

public class VectorReaderMapOp extends VectorMapOp
{
  private VectorDataProvider dp;
  private ProviderProperties providerProperties;

  public VectorReaderMapOp(String vectorName)
  {
    _outputName = vectorName;
  }

  public VectorReaderMapOp(VectorDataProvider dp, ProviderProperties providerProperties)
  {
    this.dp = dp;
    _outputName = dp.getPrefixedResourceName();
    this.providerProperties = providerProperties;
  }

  // Declare the default constructor private so no one can use it
  @SuppressWarnings("unused")
  private VectorReaderMapOp()
  {
  }

  @Override
  public void addInput(MapOp n) throws IllegalArgumentException
  {
    throw new IllegalArgumentException("This ExecuteNode takes no arguments.");
  }

  @Override
  public void build(Progress p) throws IOException, JobFailedException, JobCancelledException
  {
    if (p != null)
    {
      p.complete();
    }
    if (dp != null)
    {
      _output = new BasicInputFormatDescriptor(dp, providerProperties);
    }
    else
    {
      _output = new BasicInputFormatDescriptor(_outputName);
    }
  }

  // TODO: This method should use DataProviderFactory to perform the move like the
  // other map ops once we migrate all the vector map ops to use data providers
  // instead of HDFS files directly.
  @Override
  public void moveOutput(String toName) throws IOException
  {
    if (dp != null)
    {
      // Do the move through the data provider
      dp.move(toName);
    }
    else
    {
      // If there is no vector data provider, fall-back to the existing HDFS
      // code.
      Path toPath = new Path(toName);
      Path sourcePath = new Path(_outputName);
      Configuration conf = createConfiguration();
      FileSystem sourceFs = HadoopFileUtils.getFileSystem(conf, sourcePath);
      FileSystem destFs = HadoopFileUtils.getFileSystem(conf, toPath);
      if (!FileUtil.copy(sourceFs, sourcePath, destFs, toPath, false, false, conf))
      {
        throw new IOException("Error copying '" + _outputName +
            "' to '" + toName.toString() + "'");
      }
      // Now copy the 
      Path sourceColumns = new Path(_outputName + ".columns");
      if (sourceFs.exists(sourceColumns))
      {
        Path toColumns = new Path(toName.toString() + ".columns");
        if (FileUtil.copy(sourceFs, sourceColumns, destFs, toColumns, false, false, conf) == false)
        {
          throw new IOException("Error copying columns file '" + sourceColumns.toString() +
              "' to '" + toColumns.toString());
        }
      }
    }
    _outputName = toName;
    _output = new BasicInputFormatDescriptor(_outputName);
  }

//  public void setPath(Path path)
//  {
//    _outputName = path;
//  }
  
  @Override
  public String toString()
  {
    return String.format("VectorReaderMapOp: %s", 
        _outputName == null ? "null": _outputName.toString());
  }
}
