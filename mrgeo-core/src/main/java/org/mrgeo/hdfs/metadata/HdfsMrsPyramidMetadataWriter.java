/*
 * Copyright 2009-2017. DigitalGlobe, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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
import org.mrgeo.data.image.MrsPyramidMetadataWriter;
import org.mrgeo.data.image.MrsPyramidMetadataWriterContext;
import org.mrgeo.hdfs.image.HdfsMrsImageDataProvider;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.image.MrsPyramidMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author tim.tisler
 * @version $Revision: 1.0 $
 */
public class HdfsMrsPyramidMetadataWriter implements MrsPyramidMetadataWriter
{
private static final Logger log = LoggerFactory.getLogger(HdfsMrsPyramidMetadataWriter.class);

private final MrsImageDataProvider provider;
private Configuration conf;

/**
 * Constructor for HdfsMrsPyramidMetadataWriter.
 *
 * @param provider MrsImageDataProvider
 * @param context  MrsPyramidMetadataWriterContext
 */
public HdfsMrsPyramidMetadataWriter(MrsImageDataProvider provider,
    Configuration conf,
    MrsPyramidMetadataWriterContext context)
{
  this.provider = provider;
  this.conf = conf;
}

/**
 * Write the (already loaded) metadata for the provider to HDFS
 *
 * @throws IOException
 * @see MrsPyramidMetadataWriter#write()
 */
@Override
public void write() throws IOException
{
  MrsPyramidMetadata metadata = provider.getMetadataReader(null).read();

  write(metadata);
}

/**
 * Write a provided metadata object to HDFS.
 *
 * @param metadata MrsImagePyramidMetadata
 * @throws IOException
 * @see MrsPyramidMetadataWriter#write(MrsPyramidMetadata)
 */
@Override
public void write(MrsPyramidMetadata metadata) throws IOException
{
  if (!(provider instanceof HdfsMrsImageDataProvider))
  {
    throw new IOException("Expected an instance of HdfsMrsImageDataprovider instead of " +
        provider.getClass().getCanonicalName());
  }
  final Path path = new Path(((HdfsMrsImageDataProvider) provider).getResolvedResourceName(true),
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
