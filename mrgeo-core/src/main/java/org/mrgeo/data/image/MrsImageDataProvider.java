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

package org.mrgeo.data.image;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.mrgeo.data.DataProviderException;
import org.mrgeo.data.ProtectionLevelValidator;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.rdd.RasterRDD;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.image.MrsPyramidMetadata;

import java.io.IOException;

/**
 * This class is what the MrGeo core code calls to make use of image pyramids
 * for its various operations, including:
 * <ul>
 * <li> map/reducing over input images
 * <li> map/reducing where the output is an image pyramid
 * <li> reading and writing metadata
 * <li> reading and writing image tiles.
 * </ul>
 * <p>
 * A data plugin that wishes to store image data must extent this class and
 * implement its abstract methods.
 * 
 * This class also contains a series of static methods that are conveniences
 * for configuring Spark jobs that use image pyramids as input data.
 */
public abstract class MrsImageDataProvider implements ProtectionLevelValidator
{
private String resourceName;

protected ProviderProperties providerProperties;

protected MrsImageDataProvider()
{
  resourceName = null;
}

/**
 * Sub-classes which use the default constructor must subsequently call
 * this method to assign the resource name,
 *
 * @param resourceName
 */
protected void setResourceName(String resourceName)
{
  this.resourceName = resourceName;
}

public String getResourceName()
{
  return resourceName;
}

public abstract void delete() throws IOException;

public abstract void move(String toResource) throws IOException;

/**
 * Override this method if your data provider needs to perform Hadoop job
 * setup the same way for handling image input and output.
 *
 * @param job
 * @throws DataProviderException
 * @throws IOException
 */
public void setupJob(final Job job) throws DataProviderException
{
}

/**
 * Override this method if your data provider needs to perform Spark job
 * setup the same way for handling image input and output.
 *
 * @param conf
 * @throws DataProviderException
 * @throws IOException
 */
public Configuration setupSparkJob(final Configuration conf) throws DataProviderException
{
  return conf;
}

public ProviderProperties getProviderProperties()
{
  return providerProperties;
}

public MrsImageDataProvider(final String resourceName)
{
  this.resourceName = resourceName;
}


public MrsPyramidMetadataReader getMetadataReader()
{
  return getMetadataReader(null);
}

/**
 * Return an instance of a class that can read metadata for this resource.
 *
 * @return
 */
public abstract MrsPyramidMetadataReader getMetadataReader(
    MrsPyramidMetadataReaderContext context);

public MrsPyramidMetadataWriter getMetadataWriter()
{
  return getMetadataWriter(null);
}

/**
 * Return an instance of a class that can write metadata for this resource.
 *
 * @return
 */
public abstract MrsPyramidMetadataWriter getMetadataWriter(
    MrsPyramidMetadataWriterContext context);

public MrsImageReader getMrsTileReader(final int zoomlevel) throws IOException
{
  final MrsPyramidReaderContext context = new MrsPyramidReaderContext();
  context.setZoomlevel(zoomlevel);
  return getMrsTileReader(context);
}

/**
 * Return an instance of a MrsImageReader class to be used for reading tiled data. This method may
 * be invoked by callers regardless of whether they are running within a Spark job or not.
 *
 * @return
 * @throws IOException
 */
public abstract MrsImageReader getMrsTileReader(MrsPyramidReaderContext context) throws IOException;

public MrsImageWriter getMrsTileWriter(final int zoomlevel,
    final String protectionLevel) throws IOException
{
  final MrsPyramidWriterContext context = new MrsPyramidWriterContext(zoomlevel, 0,
      protectionLevel);
  return getMrsTileWriter(context);
}

public abstract void delete(final int zoomlevel) throws IOException;

public abstract MrsImageWriter getMrsTileWriter(MrsPyramidWriterContext context) throws IOException;

/**
 * Return an instance of a RecordReader class to be used in Spark jobs for reading tiled
 * data.
 *
 * @return
 */
public abstract RecordReader<TileIdWritable, RasterWritable> getRecordReader();

/**
 * Return an instance of a RecordWriter class to be used in Spark jobs for writing tiled
 * data.
 *
 * @return
 */
public abstract RecordWriter<TileIdWritable, RasterWritable> getRecordWriter();

/**
 * Return an instance of an InputFormat class to be used in Spark jobs for processing tiled
 * data.
 *
 * @return
 */
public abstract MrsImageInputFormatProvider getImageInputFormatProvider(
    final ImageInputFormatContext context);

/**
 * Return an instance of an OutputFormat class to be used in Spark jobs for producing tiled
 * data.
 *
 * @return
 */
public abstract MrsImageOutputFormatProvider getTiledOutputFormatProvider(
    final ImageOutputFormatContext context);

}