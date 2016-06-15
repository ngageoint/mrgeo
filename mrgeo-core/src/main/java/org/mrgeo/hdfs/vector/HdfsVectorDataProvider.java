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

package org.mrgeo.hdfs.vector;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.vector.*;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsVectorDataProvider extends VectorDataProvider
{
private static Logger log = LoggerFactory.getLogger(HdfsVectorDataProvider.class);

private Configuration conf;
private Path resourcePath;
private ProviderProperties providerProperties;

public HdfsVectorDataProvider(final Configuration conf,
    final String prefix, final String resourceName, final ProviderProperties providerProperties)
{
  super(prefix, resourceName);
  this.conf = conf;
  this.providerProperties = providerProperties;
}

public String getResolvedResourceName(final boolean mustExist) throws IOException
{
  return resolveNameToPath(conf, getResourceName(), providerProperties, mustExist).toUri().toString();
  //return new Path(getImageBasePath(), getResourceName()).toUri().toString();
}

public Path getResourcePath(boolean mustExist) throws IOException
{
  if (resourcePath == null)
  {
    resourcePath = resolveName(getConfiguration(), getResourceName(),
        providerProperties, mustExist);
  }
  return resourcePath;
}

@Override
public VectorMetadataReader getMetadataReader()
{
  // TODO Auto-generated method stub
  return null;
}

@Override
public VectorMetadataWriter getMetadataWriter()
{
  // TODO Auto-generated method stub
  return null;
}

@Override
public VectorReader getVectorReader() throws IOException
{
  String resourceName = getResolvedResourceName(true);
  Path resourcePath = new Path(resourceName);
  if (isSourceDelimited(resourcePath, true, getConfiguration()))
  {
    return new DelimitedVectorReader(this, new VectorReaderContext(), conf);
  }
  else if (isSourceShapefile(resourcePath))
  {
    return new ShapefileVectorReader(this, new VectorReaderContext(), conf);
  }
  throw new IOException("Unable to create vector reader for " + resourceName);
}

@Override
public VectorReader getVectorReader(VectorReaderContext context) throws IOException
{
  String resourceName = getResolvedResourceName(true);
  Path resourcePath = new Path(resourceName);
  if (isSourceDelimited(resourcePath, true, getConfiguration()))
  {
    return new DelimitedVectorReader(this, context, conf);
  }
  else if (isSourceShapefile(resourcePath))
  {
    return new ShapefileVectorReader(this, context, conf);
  }
  throw new IOException("Unable to create vector reader for " + resourceName);
}

@Override
public VectorWriter getVectorWriter() throws IOException
{
  String resourceName = getResolvedResourceName(false);
  Path resourcePath = new Path(resourceName);
  if (isSourceDelimited(resourcePath, false, getConfiguration()))
  {
    return new DelimitedVectorWriter(this, conf);
  }
  // TODO:
//    else if (isSourceShapefile(resourcePath))
//    {
//      return new ShapefileVectorWriter(context);
//    }
  throw new IOException("Unable to create vector output format provider for " + resourceName);
}

@Override
public RecordReader<FeatureIdWritable, Geometry> getRecordReader() throws IOException
{
  String resourceName = getResolvedResourceName(true);
  Path resourcePath = new Path(resourceName);
  if (isSourceDelimited(resourcePath, true, getConfiguration()))
  {
    return new DelimitedVectorRecordReader();
  }
  else if (isSourceShapefile(resourcePath))
  {
    return new ShpInputFormat.ShpRecordReader();
  }
  throw new IOException("Unable to create vector reader for " + resourceName);
}

@Override
public RecordWriter<FeatureIdWritable, Geometry> getRecordWriter()
{
  // TODO Auto-generated method stub
  return null;
}

@Override
public VectorInputFormatProvider getVectorInputFormatProvider(VectorInputFormatContext context) throws IOException
{
  String resourceName = getResolvedResourceName(true);
  Path resourcePath = new Path(resourceName);
  if (isSourceDelimited(resourcePath, true, getConfiguration()))
  {
    return new DelimitedVectorInputFormatProvider(context);
  }
  else if (isSourceShapefile(resourcePath))
  {
    return new ShapefileVectorInputFormatProvider(context);
  }
  throw new IOException("Unable to create vector input format provider for " + resourceName);
}

@Override
public VectorOutputFormatProvider getVectorOutputFormatProvider(VectorOutputFormatContext context) throws IOException
{
  String resourceName = getResolvedResourceName(false);
  Path resourcePath = new Path(resourceName);
  if (isSourceDelimited(resourcePath, false, getConfiguration()))
  {
    return new DelimitedVectorOutputFormatProvider(this, context);
  }
//    else if (isSourceShapefile(resourcePath))
//    {
//      return new ShapefileVectorInputFormatProvider(context);
//    }
  throw new IOException("Unable to create vector output format provider for " + resourceName);
}

@Override
public void delete() throws IOException
{
  // TODO Auto-generated method stub

}

@Override
public void move(String toResource) throws IOException
{
  // TODO Auto-generated method stub

}

public static boolean isSourceDelimited(Path source, boolean mustExist,
    Configuration conf) throws IOException
{
  if (source != null)
  {
    String lowerPath = source.toString().toLowerCase();
    if (lowerPath.endsWith(".tsv") || lowerPath.endsWith(".csv"))
    {
      if (mustExist)
      {
        FileSystem fs = HadoopFileUtils.getFileSystem(conf, source);
        return fs.exists(source);
      }
      return true;
    }
  }
  return false;
}

public static boolean isSourceShapefile(Path source)
{
  if (source != null)
  {
    String lowerPath = source.toString().toLowerCase();
    return lowerPath.endsWith(".shp");
  }
  return false;
}

public static boolean canOpen(final Configuration conf, String input,
    final ProviderProperties providerProperties) throws IOException
{
  Path p;
  try
  {
    p = resolveName(conf, input, providerProperties, false);
      if (isSourceDelimited(p, true, conf))
      {
        return true;
      }
      if (isSourceShapefile(p))
      {
        return true;
      }
  }
  catch (IOException ignored)
  {
  }
  return false;
}

public static boolean exists(final Configuration conf, String input,
    final ProviderProperties providerProperties) throws IOException
{
  return resolveNameToPath(conf, input, providerProperties, true) != null;
}

public static void delete(final Configuration conf, String input,
    final ProviderProperties providerProperties) throws IOException
{
  Path p = resolveNameToPath(conf, input, providerProperties, false);
  if (p != null)
  {
    if (isSourceDelimited(p, false, conf))
    {
      HadoopFileUtils.delete(conf, p);
      Path columns = new Path(p.toString() + ".columns");
      if (HadoopFileUtils.exists(conf, columns))
      {
        HadoopFileUtils.delete(conf, columns);
      }
    }
    else if (isSourceShapefile(p))
    {
      HadoopFileUtils.delete(conf, p);
      String fileName = p.toString();
      // We know the file ends in ".shp"
      String fileWithoutExt = fileName.substring(0, fileName.length() - 3);
      String[] subExts = { "shx", "idx", "dbf", "prj", "shp.xml", "sbn", "sbx", "avl" };
      for (String ext : subExts)
      {
        Path subFile = new Path(fileWithoutExt + ext);
        if (HadoopFileUtils.exists(conf, subFile))
        {
          HadoopFileUtils.delete(conf, subFile);
        }
      }
    }
  }
}

public static boolean canWrite(final Configuration conf, String input,
    final ProviderProperties providerProperties) throws IOException
{
  // The return value of resolveNameToPath will be null if the input
  // path does not exist. It wil throw an exception if there is a problem
  // with building the path.
  Path p = resolveNameToPath(conf, input, providerProperties, true);
  return (p == null);
}

private static Path resolveName(final Configuration conf, final String input,
    final ProviderProperties providerProperties, final boolean mustExist) throws IOException
{
  Path result = resolveNameToPath(conf, input, providerProperties, mustExist);
  // Check to see if the source is one of the supported formats
  if (result != null && (isSourceDelimited(result, mustExist, conf) || isSourceShapefile(result)))
  {
    return result;
  }

  throw new IOException("Invalid image: " + input);
}

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "method only makes complete URI out of the name")
private static Path resolveNameToPath(final Configuration conf, final String input,
    final ProviderProperties providerProperties, final boolean mustExist) throws IOException
{
  if (input.indexOf('/') >= 0)
  {
    // The requested input is absolute. If it exists in the local file
    // system, then return that path. Otherwise, check HDFS, and if it
    // doesn't exist there either, but mustExist is false, return the
    // HDFS path (this resource is being created). We never create resources
    // in the local file system.
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
    Path p = null;
    try
    {
      p = new Path(input);
    }
    catch(IllegalArgumentException e)
    {
      // If the path is a bad URI (which can happen if this is not an HDFS resource),
      // then an IllegalArgumentException is thrown.
      return null;
    }
    if (mustExist)
    {
      FileSystem fs = HadoopFileUtils.getFileSystem(conf, p);
      if (fs.exists(p))
      {
        return p;
      }
    }
    else
    {
      return p;
    }
    return null;
  }
  else
  {
    // The input is relative, check it using the image base path.
    Path p = null;
    try
    {
      p = new Path(getBasePath(conf), input);
    }
    catch(IllegalArgumentException e)
    {
      // If the path is a bad URI (which can happen if this is not an HDFS resource),
      // then an IllegalArgumentException is thrown.
      return null;
    }
    if (mustExist)
    {
      FileSystem fs = HadoopFileUtils.getFileSystem(conf, p);
      if (fs.exists(p))
      {
        return p;
      }
    }
    else
    {
      return p;
    }
    return null;
  }
}

static Path getBasePath(final Configuration conf)
{
  String basePathKey = "hdfs." + MrGeoConstants.MRGEO_HDFS_VECTOR;
  Path basePath = null;
  String strBasePath = null;
  // First check to see if the vector base path is defined in the configuration.
  // This happens when it's being called from within code that runs on a
  // Hadoop data node.
  // If it's not there, then check for the base path definition in the mrgeo conf,
  // which will only work when it's called from the name node.
  if (conf != null)
  {
    strBasePath = conf.get(basePathKey);
  }
  if (strBasePath == null)
  {
    strBasePath = MrGeoProperties.getInstance().getProperty(MrGeoConstants.MRGEO_HDFS_VECTOR);
  }
  if (strBasePath != null)
  {
    try
    {
      basePath = new Path(new URI(strBasePath));
    }
    catch (URISyntaxException e)
    {
      log.error("Invalid HDFS base path for vectors: " + strBasePath, e);
    }
  }
  // Use default if we couldn't get the base path
  if (basePath == null)
  {
    basePath = new Path("/mrgeo/vectors");
  }
  return basePath;
}

private Configuration getConfiguration()
{
  return conf;
}
}
