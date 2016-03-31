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

package org.mrgeo.hdfs.image;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.image.*;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.image.MrsImageWriter;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.hdfs.input.image.HdfsMrsPyramidRecordReader;
import org.mrgeo.hdfs.input.image.HdfsMrsPyramidInputFormatProvider;
import org.mrgeo.hdfs.metadata.HdfsMrsPyramidMetadataReader;
import org.mrgeo.hdfs.metadata.HdfsMrsPyramidMetadataWriter;
import org.mrgeo.hdfs.output.image.HdfsMrsPyramidOutputFormatProvider;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsMrsImageDataProvider extends MrsImageDataProvider
{
  private static Logger log = LoggerFactory.getLogger(HdfsMrsImageDataProvider.class);
  public final static String METADATA = "metadata";

  private Configuration conf;
  private HdfsMrsPyramidMetadataReader metaReader = null;
  private HdfsMrsPyramidMetadataWriter metaWriter = null;
  private Path resourcePath;

  public HdfsMrsImageDataProvider(final Configuration conf,
      final String resourceName, final ProviderProperties providerProperties)
  {
    super(resourceName);
    this.conf = conf;
    this.providerProperties = providerProperties;
  }

  public String getResolvedResourceName(final boolean mustExist) throws IOException
  {
    try
    {
      return resolveNameToPath(conf, getResourceName(), providerProperties, mustExist, true).toUri().toString();
    }
    catch (IllegalArgumentException e)
    {
      throw new IOException(e);
    }
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
  public MrsImageInputFormatProvider getImageInputFormatProvider(ImageInputFormatContext context)
  {
    return new HdfsMrsPyramidInputFormatProvider(context);
  }

  @Override
  public MrsImageOutputFormatProvider getTiledOutputFormatProvider(ImageOutputFormatContext context)
  {
    return new HdfsMrsPyramidOutputFormatProvider(this, context);
  }

  @Override
  public RecordReader<TileIdWritable, RasterWritable> getRecordReader()
  {
    return new HdfsMrsPyramidRecordReader();
  }

  @Override
  public RecordWriter<TileIdWritable, RasterWritable> getRecordWriter()
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public MrsImageReader getMrsTileReader(MrsPyramidReaderContext context) throws IOException
  {
    return new HdfsMrsImageReader(this, context);
  }

  @Override
  public void delete(int zoomlevel) throws IOException
  {
    delete(getConfiguration(), new Path(getResourcePath(true), "" + zoomlevel).toString(),
        providerProperties);
  }

  @Override
  public MrsImageWriter getMrsTileWriter(MrsPyramidWriterContext context)
  {
    return new HdfsMrsImageWriter(this, context);
  }

  @Override
  public MrsPyramidMetadataReader getMetadataReader(MrsPyramidMetadataReaderContext context)
  {
    if (metaReader == null)
    {
      metaReader = new HdfsMrsPyramidMetadataReader(this, getConfiguration(), context);
    }
    return metaReader;
  }

  @Override
  public MrsPyramidMetadataWriter getMetadataWriter(MrsPyramidMetadataWriterContext context)
  {
    if (metaWriter == null)
    {
      metaWriter = new HdfsMrsPyramidMetadataWriter(this, getConfiguration(), context);
    }
    return metaWriter;
  }

  @Override
  public void delete() throws IOException
  {
    delete(getConfiguration(), getResourceName(), providerProperties);
  }

  @Override
  public void move(final String toResource) throws IOException
  {
    try
    {
      HadoopFileUtils.move(getConfiguration(),
                           resolveNameToPath(getConfiguration(), getResourceName(), providerProperties, true, true),
                           new Path(toResource));
    }
    catch (IllegalArgumentException e)
    {
      throw new IOException(e);
    }
  }

  @Override
  public boolean validateProtectionLevel(final String protectionLevel)
  {
    // TODO: This method should validate that the protection level logical
    // expression passed in is formulated correctly - e.g. "A|B|C" is good
    // but "A|(B&C" is bad because it's missing the right parenthesis.
    return true;
  }

  /**
   * When this code attempts to construct a Path object from the input parameter,
   * if the the throwExceptionIfInvalid flag is true, this function will not
   * intercept the IllegalArgumentException when it is thrown. If that flag is
   * false, then the IllegalArgumentException is caught, and this function will
   * return a null value.
   *
   * @param conf
   * @param input
   * @param providerProperties
   * @param mustExist
   * @param throwExceptionIfInvalid
   * @return
   * @throws IOException
   */
  private static Path resolveNameToPath(final Configuration conf, final String input,
      final ProviderProperties providerProperties, final boolean mustExist,
      final boolean throwExceptionIfInvalid) throws IOException
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
      if (throwExceptionIfInvalid)
      {
        p = new Path(input);
      }
      else
      {
        try
        {
          p = new Path(input);
        }
        catch (IllegalArgumentException e)
        {
          // If the resource is not a legitimate HDFS path, then this is the wrong
          // provider, so return null;
          return null;
        }
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
      if (throwExceptionIfInvalid)
      {
        p = new Path(getBasePath(conf), input);
      }
      else
      {
        try
        {
          p = new Path(getBasePath(conf), input);
        }
        catch (IllegalArgumentException e)
        {
          // If the resource is not a legitimate HDFS path, then this is the wrong
          // provider, so return null;
          return null;
        }
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
    String basePathKey = "hdfs." + MrGeoConstants.MRGEO_HDFS_IMAGE;
    Path basePath = null;
    String strBasePath = null;
    // First check to see if thte image base path is defined in the configuration.
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
      strBasePath = MrGeoProperties.getInstance().getProperty(MrGeoConstants.MRGEO_HDFS_IMAGE);
    }
    if (strBasePath != null)
    {
      try
      {
        basePath = new Path(new URI(strBasePath));
      }
      catch (URISyntaxException e)
      {
        log.error("Invalid HDFS base path for images: " + strBasePath, e);
      }
    }
    // Use default if we couldn't get the base path
    if (basePath == null)
    {
      basePath = new Path("/mrgeo/images");
    }
    return basePath;
  }

  private static Path resolveName(final Configuration conf, final String input,
      final ProviderProperties providerProperties, final boolean mustExist) throws IOException
  {
    Path result = resolveNameToPath(conf, input, providerProperties, mustExist, false);
    if (result != null && (!mustExist || hasMetadata(conf, result)))
    {
      return result;
    }

    throw new IOException("Invalid image: " + input);
  }

  private static boolean hasMetadata(final Configuration conf, final Path p)
  {
    FileSystem fs;
    try
    {
      fs = HadoopFileUtils.getFileSystem(conf, p);
      if (fs.exists(p))
      {
        return (fs.exists(new Path(p, METADATA)));
      }
    }
    catch (IOException e)
    {
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
      if (p != null)
      {
        return hasMetadata(conf, p);
      }
    }
    catch (IOException e)
    {
    }
    return false;
  }
  
  public static boolean exists(final Configuration conf, String input,
      final ProviderProperties providerProperties) throws IOException
  {
    return resolveNameToPath(conf, input, providerProperties, true, false) != null;
  }
  
  public static void delete(final Configuration conf, String input,
      final ProviderProperties providerProperties) throws IOException
  {
    try
    {
      Path p = resolveNameToPath(conf, input, providerProperties, false, true);
      // In case the resource was moved since it was created
      if (p != null)
      {
        HadoopFileUtils.delete(conf, p);
      }
    }
    catch (IllegalArgumentException e)
    {
      throw new IOException(e);
    }
  }

  public static boolean canWrite(final Configuration conf, String input,
      final ProviderProperties providerProperties) throws IOException
  {
    // The return value of resolveNameToPath will be null if the input
    // path does not exist.
    try
    {
      Path p = resolveNameToPath(conf, input, providerProperties, true, true);
      return (p == null);
    }
    catch (IllegalArgumentException e)
    {
    }
    return false;
  }

//  public static MrsImagePyramidMetadata load(final Path path) throws JsonGenerationException,
//  JsonMappingException, IOException
//  {
//    // attach to hdfs and create an input stream for the file
//    FileSystem fs = HadoopFileUtils.getFileSystem(path);
//    final InputStream is = HadoopFileUtils.open(path); // fs.open(path);
//    try
//    {
//      // load the metadata from the input stream
//      final MrsImagePyramidMetadata metadata = MrsImagePyramidMetadata.load(is);
//
//      // set the fully qualified path for the metadata file
//      Path fullPath = path.makeQualified(fs);
//      metadata.setPyramid(fullPath.getParent().toString());
//
//      return metadata;
//    }
//    finally
//    {
//      is.close();
//    }
//  }

  //  @Override
  //  public boolean canOpen(final String dataSource)
  //  {
  //    Path dataSourcePath = new Path(dataSource);
  //    try
  //    {
  //      FileSystem fs = HadoopFileUtils.getFileSystem(dataSourcePath);
  //      return true;
  ////      return fs.exists(dataSourcePath);
  //    }
  //    catch(IOException e)
  //    {
  //      // ignore, return false if the path doesn't exist
  //    }
  //    return false;
  //  }
  //
  //  @Override
  //  public InputFormatProvider getInputFormatProvider(final ImageInputFormatContext inputFormatConfig)
  //  {
  //    if (inputFormatConfig instanceof MrsImageTiledInputFormatContext)
  //    {
  //      return new HdfsMrsPyramidInputFormatProvider((MrsImageTiledInputFormatContext)inputFormatConfig);
  //    }
  //    else if (inputFormatConfig instanceof MrsImagePyramidSingleInputFormatConfig)
  //    {
  //      return new HdfsMrsImagePyramidSingleInputFormatProvider((MrsImagePyramidSingleInputFormatConfig)inputFormatConfig);
  //    }
  //    return null;
  //  }
  //
  //  @Override
  //  public OutputFormatProvider getOutputFormatProvider(final OutputFormatConfig outputFormatConfig)
  //  {
  //    if (outputFormatConfig instanceof ImageOutputFormatContext)
  //    {
  //      return new HdfsMrsPyramidOutputFormatProvider((ImageOutputFormatContext)outputFormatConfig);
  //    }
  //    return null;
  //  }

  private Configuration getConfiguration()
  {
    return conf;
  }
}
