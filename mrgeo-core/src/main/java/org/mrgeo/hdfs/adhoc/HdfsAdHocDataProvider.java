/*
 * Copyright 2009-2016 DigitalGlobe, Inc.
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
 *
 */

package org.mrgeo.hdfs.adhoc;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.mrgeo.data.DataProviderException;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.adhoc.AdHocDataProvider;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.utils.HadoopUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;

public class HdfsAdHocDataProvider extends AdHocDataProvider
{
  private Configuration conf;

  @Override
  public void setupJob(Job job) throws DataProviderException
  {
    super.setupJob(job);
    
    try
    {
      if (!HadoopFileUtils.exists(getConfiguration(), getResourcePath()))
      {
        HadoopFileUtils.create(getConfiguration(), getResourceName());
      }
    }
    catch (IOException e)
    {
      throw new DataProviderException("Error setting up HDFS ad hoc provider", e);
    }
  }

  private Path resourcePath;
  /**
   * Stores a list of the files in the HDFS directory corresponding to the resource name the
   * provider was constructed with. Internal code should *always* call initializeFiles() prior to
   * accessing this data member.
   */
  private final ArrayList<Path> files = new ArrayList<Path>();

  public HdfsAdHocDataProvider(final Configuration conf,
      final ProviderProperties providerProperties) throws IOException
  {
    super(HadoopFileUtils.unqualifyPath(new Path(HadoopFileUtils.getTempDir(conf),
        HadoopUtils.createRandomString(10))).toString());
    this.conf = conf;
  }

  public HdfsAdHocDataProvider(final Configuration conf, final String resourceName,
      final ProviderProperties providerProperties) throws IOException
  {
    super(resourceName);
    this.conf = conf;
  }

  @Override
  public OutputStream add() throws IOException
  {
    return add(HadoopUtils.createRandomString(20));
  }

  @Override
  public OutputStream add(final String name) throws IOException
  {
    Path resource;
    try
    {
      resource = getResourcePath();
    }
    catch (final IOException e)
    {
      HadoopFileUtils.create(getConfiguration(), getResourceName());
      resource = getResourcePath();
    }

    final FileSystem fs = HadoopFileUtils.getFileSystem(getConfiguration(), resource);

    final Path path = new Path(resource, name);
    final OutputStream stream = fs.create(path, true);

    initializeFiles(getConfiguration());
    files.add(path);

    return stream;
  }

  @Override
  public void delete() throws IOException
  {
    HadoopFileUtils.delete(getConfiguration(), getResourcePath());
  }

  @Override
  public void move(final String toResource) throws IOException
  {
    try
    {
      HadoopFileUtils.move(getConfiguration(),
                           getResourcePath(), determineResourcePath(getConfiguration(), toResource));
    }
    catch (URISyntaxException e)
    {
      throw new IOException(e);
    }
  }

  @Override
  public InputStream get(final int index) throws IOException
  {
    initializeFiles(getConfiguration());
    if (index >= 0 && index < files.size())
    {
      return HadoopFileUtils.open(getConfiguration(), files.get(index));
    }

    throw new IOException("Index out of range (" + index + ") ");
  }

  @Override
  public InputStream get(final String name) throws IOException
  {
    initializeFiles(getConfiguration());

    for (Path p: files)
    {
      String s = FilenameUtils.getBaseName(p.toString());
      if (s.equalsIgnoreCase(name))
      {
        return HadoopFileUtils.open(getConfiguration(), p);
      }
    }

    throw new IOException("Resource not found: " + name + "(" + new Path(getResourcePath(), name) + ")");
  }

  @Override
  public String getName(int index) throws IOException
  {
    initializeFiles(getConfiguration());
    if (index >= 0 && index < files.size())
    {
      return files.get(index).toString();
    }

    throw new IOException("Index out of range (" + index + ") ");
  }

  public Path getResourcePath() throws IOException
  {
    if (resourcePath == null)
    {
      try
      {
        resourcePath = determineResourcePath(getConfiguration(), getResourceName());
      }
      catch (URISyntaxException e)
      {
        throw new IOException(e);
      }
    }
    return resourcePath;
  }

  private static Path determineResourcePath(final Configuration conf,
      final String resourceName) throws IOException, URISyntaxException
  {
    return HadoopFileUtils.resolveName(conf, resourceName, false);
  }

  @Override
  public int size() throws IOException
  {
    initializeFiles(getConfiguration());
    return files.size();
  }

  public static boolean canOpen(final Configuration conf,
      final String name, final ProviderProperties providerProperties) throws IOException
  {
    try
    {
      Path p = determineResourcePath(conf, name);
      FileSystem fs = HadoopFileUtils.getFileSystem(conf, p);
      return fs.exists(p);
    }
    catch(URISyntaxException e)
    {
      // The requested resource is not a legitimate HDFS source
    }
    return false;
  }

  public static boolean canWrite(final String name, final Configuration conf,
      final ProviderProperties providerProperties) throws IOException
  {
    // I think Ad hoc should always be able to write...
    
    return true;
//    Path p = HadoopFileUtils.resolveName(name);
//    FileSystem fs = HadoopFileUtils.getFileSystem(p);
//    
//    // Don't allow writing to an existing location
//    if (!fs.exists(p))
//    {
//      return true;
//    }
//    return false;
  }

  private void initializeFiles(final Configuration conf) throws IOException
  {
    // read any existing files into the file list.
    files.clear();

    final Path resource = getResourcePath();
    if (HadoopFileUtils.exists(conf, resource))
    {
      final FileSystem fs = HadoopFileUtils.getFileSystem(conf, resource);
      final FileStatus[] status = fs.listStatus(resource);

      for (final FileStatus f : status)
      {
        files.add(f.getPath());
      }
    }
  }

  public static boolean exists(final Configuration conf, String name,
      final ProviderProperties providerProperties) throws IOException
  {
    try
    {
      return HadoopFileUtils.exists(determineResourcePath(conf, name));
    }
    catch (URISyntaxException e)
    {
      // The name may not be an HDFS source, so in that case we return false
    }
    return false;
  }

  public static void delete(final Configuration conf, String name,
      final ProviderProperties providerProperties) throws IOException
  {
    try
    {
      HadoopFileUtils.delete(determineResourcePath(conf, name));
    }
    catch (URISyntaxException e)
    {
      throw new IOException(e);
    }
  }

  private Configuration getConfiguration()
  {
    return conf;
  }
}
