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

package org.mrgeo.hdfs.metadata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.data.image.MrsImagePyramidMetadataWriter;
import org.mrgeo.data.image.MrsImagePyramidMetadataWriterContext;
import org.mrgeo.hdfs.image.HdfsMrsImageDataProvider;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author tim.tisler
 * @version $Revision: 1.0 $
 */
public class HdfsMrsImagePyramidMetadataWriter implements MrsImagePyramidMetadataWriter
{
  private static final Logger log = LoggerFactory.getLogger(HdfsMrsImagePyramidMetadataWriter.class);

  private final MrsImageDataProvider provider;
  private Configuration conf;

  /**
   * Constructor for HdfsMrsImagePyramidMetadataWriter.
   * @param provider MrsImageDataProvider
   * @param context MrsImagePyramidMetadataWriterContext
   */
  public HdfsMrsImagePyramidMetadataWriter(MrsImageDataProvider provider,
      Configuration conf,
      MrsImagePyramidMetadataWriterContext context)
  {
    this.provider = provider;
    this.conf = conf;
  }
  
  /**
   * Write the (already loaded) metadata for the provider to HDFS
   * @throws IOException
   * @see org.mrgeo.data.image.MrsImagePyramidMetadataWriter#write()
   */
  @Override
  public void write() throws IOException
  {
    MrsImagePyramidMetadata metadata = provider.getMetadataReader(null).read();

    write(metadata);
  }
  /**
   * Write a provided metadata object to HDFS.
   * @param metadata MrsImagePyramidMetadata
   * @throws IOException 
   * @see org.mrgeo.data.image.MrsImagePyramidMetadataWriter#write(MrsImagePyramidMetadata)
  */
  @Override
  public void write(MrsImagePyramidMetadata metadata) throws IOException
  {
    if (!(provider instanceof HdfsMrsImageDataProvider))
    {
      throw new IOException("Expected an instance of HdfsMrsImageDataprovider instead of " +
        provider.getClass().getCanonicalName());
    }
    final Path path = new Path(((HdfsMrsImageDataProvider)provider).getResolvedResourceName(true),
        HdfsMrsImageDataProvider.METADATA);

    final FileSystem fs = HadoopFileUtils.getFileSystem(conf, path);
    if (fs.exists(path))
    {
      fs.delete(path, false);
    }

    log.debug("Saving metadata to " + path.toString());
    FSDataOutputStream os = null;
    try
    {
      os = HadoopFileUtils.getFileSystem(conf, path).create(path);
      metadata.save(os);
    }
    catch (Exception e)
    {
      throw new IOException(e);
    }
    finally
    {
      if (os != null)
      {
        os.close();
      }
    }

    // reload the metadata after a save;
    provider.getMetadataReader(null).reload();
  }

}
