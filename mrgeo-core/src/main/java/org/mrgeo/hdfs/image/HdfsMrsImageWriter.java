/*
 * Copyright 2009-2014 DigitalGlobe, Inc.
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

package org.mrgeo.hdfs.image;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.mrgeo.data.image.MrsImagePyramidWriterContext;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.MrsTileWriter;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.utils.HadoopUtils;

import java.awt.image.Raster;
import java.io.IOException;

public class HdfsMrsImageWriter extends MrsTileWriter<Raster>
{
  final private HdfsMrsImageDataProvider provider;
  final private MrsImagePyramidWriterContext context;
  // Do not use the following variable directly, instead call
  // getConfiguration() since it is created on-demand
  private Configuration conf;

  private MapFile.Writer writer = null;


  // image = path to mapfile directory- e.g., /hdfs/path/to/mapfile (will contain data and index
  // dirs)
  public HdfsMrsImageWriter(HdfsMrsImageDataProvider provider, MrsImagePyramidWriterContext context)
  {
    this.provider = provider;
    this.context = context;

  }

  @Override
  public void append(final TileIdWritable k, final Raster raster) throws IOException
  {
    if (writer == null)
    {
      openWriter();

    }
    writer.append(k, RasterWritable.toWritable(raster));
  }


  private void openWriter() throws IOException
  {
    //Path imagePath = provider.getResourcePath(true);
    Path imagePath = provider.getResourcePath(false);
    if (context != null)
    {
      imagePath = new Path(provider.getResourcePath(true), context.getZoomlevel() + "/part-" + String.format("%05d", context.getPartNum()));
    }
    else
    {
      throw new IOException("Context is not present.  Look at this if it's correct behaviour...  May need to uncomment the line below this...");
      //imagePath = new Path(imagePath, "/part-00000");      
    }

    final FileSystem fs = HadoopFileUtils.getFileSystem(getConfiguration(), imagePath);

    Configuration localConf = HadoopUtils.createConfiguration();
    // set the packet size way up... this should be a little bigger than the size of 1 512x512x3
    // tile
    localConf.set("dfs.client.write-packet-size", "786500");

    writer = new MapFile.Writer(conf, imagePath,
        MapFile.Writer.keyClass(TileIdWritable.class.asSubclass(WritableComparable.class)),
        MapFile.Writer.valueClass(RasterWritable.class.asSubclass(Writable.class)),
        MapFile.Writer.compression(SequenceFile.CompressionType.RECORD));

    writer.setIndexInterval(1);
  }

  @Override
  public void close() throws IOException
  {
    if (writer != null)
    {
      writer.close();
    }
  }

  @Override
  public String getName() throws IOException
  {
    Path imagePath = provider.getResourcePath(true);
    if (context != null)
    {
      imagePath = new Path(provider.getResourcePath(true), context.getZoomlevel() + "/part-" + String.format("%05d", context.getPartNum()));
    }
    else
    {
      throw new IOException("Context is not present.  Look at this if it's correct behaviour...  May need to uncomment the line below this...");
      //imagePath = new Path(imagePath, "/part-00000");
    }

    return imagePath.toUri().toString();
  }

  private Configuration getConfiguration()
  {
    if (conf == null)
    {
      conf = HadoopUtils.createConfiguration();
    }
    return conf;
  }
}
