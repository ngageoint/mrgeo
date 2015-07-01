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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.DataProviderNotFound;
import org.mrgeo.data.vector.VectorDataProvider;
import org.mrgeo.format.FeatureInputFormatFactory;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.utils.HadoopUtils;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

public class VectorFileMapOpLoader implements ResourceMapOpLoader
{
  private static String basePath = null;

  public static void setVectorBasePath(String path)
  {
    basePath = path;
  }

  @Override
  public MapOp loadMapOpFromResource(String resourceName,
      final Properties providerProperties) throws IOException
  {
    try
    {
      VectorDataProvider dp = DataProviderFactory.getVectorDataProvider(resourceName, AccessMode.READ, providerProperties);
      if (dp != null)
      {
        VectorReaderMapOp vrn = new VectorReaderMapOp(dp, providerProperties);
        return vrn;
      }
    }
    catch(DataProviderNotFound e)
    {
      // Fall-back to old code for reading from HDFS until we have an HDFS
      // implementation for the vector data provider API.
    }
    // This code is HDFS-specific for now because we do not have a vector
    // data provider implemented for file-based vector data yet. Once that
    // is done, this code can be re-factored to use it.
    Path resourcePath = resolveNameToPath(resourceName);
    if (resourcePath != null)
    {
      if (FeatureInputFormatFactory.getInstance().isRecognized(resourcePath))
      {
        FileSystem fs = HadoopFileUtils.getFileSystem(resourcePath);
        if (fs.exists(resourcePath))
        {
          VectorReaderMapOp vrn = new VectorReaderMapOp(resourceName);
          return vrn;
        }
      }
    }
    return null;
  }

  // This functionality will eventually be inside of an HDFS data provider for
  // vector data...
  private static Path resolveNameToPath(final String input) throws IOException
  {
    // It could be either HDFS or local file system
    File f = new File(input);
    if (f.exists())
    {
      try
      {
        return new Path(new URI("file://" + input));
      }
      catch (URISyntaxException e)
      {
        // The URI is invalid, so let's continue to try to open it in HDFS
      }
    }
    try
    {
      Path p = new Path(input);
  
      FileSystem fs = HadoopFileUtils.getFileSystem(p);
      if (fs.exists(p))
      {
        return p;
      }
  
      Path basePath = new Path(getVectorBasePath());
      p = new Path(basePath, input);
      fs = HadoopFileUtils.getFileSystem(p);
      if (fs.exists(p))
      {
        return p;
      }
    }
    catch(IllegalArgumentException e)
    {
      // The URI is invalid
    }

    return null;
  }

  private static String getVectorBasePath()
  {
    if (basePath == null)
    {
      basePath = HadoopUtils.getDefaultVectorBaseDirectory();
    }
    return basePath;
  }
}
