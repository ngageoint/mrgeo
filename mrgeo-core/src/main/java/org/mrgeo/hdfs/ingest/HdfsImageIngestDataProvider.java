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

package org.mrgeo.hdfs.ingest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.mrgeo.data.ingest.ImageIngestDataProvider;
import org.mrgeo.data.ingest.ImageIngestRawInputFormatProvider;
import org.mrgeo.data.ingest.ImageIngestTiledInputFormatProvider;
import org.mrgeo.data.ingest.ImageIngestWriterContext;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.MrsTileWriter;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.utils.HadoopUtils;

import java.awt.image.Raster;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

public class HdfsImageIngestDataProvider extends ImageIngestDataProvider
{
  private Configuration conf;

  private static class HdfsSequenceWriter extends MrsTileWriter<Raster>
  {
    private SequenceFile.Writer writer;

    private String image;
    // image = path to sequence file directory- e.g., /hdfs/path/to/seqfile (will contain part-<xxxxx> dirs)
    public HdfsSequenceWriter(final String image) throws IOException
    {
      final Configuration conf = HadoopUtils.createConfiguration();
      final Path imagePath = new Path(image);
      final FileSystem fs = HadoopFileUtils.getFileSystem(imagePath);

      this.image = image;
      // set the packet size way up... this should be a little bigger than the size of 1 512x512x3
      // tile
      conf.set("dfs.client.write-packet-size", "786500");

      writer = SequenceFile.createWriter(fs, conf, imagePath, TileIdWritable.class, RasterWritable.class,
          SequenceFile.CompressionType.RECORD);
    }

    @Override
    public void append(final TileIdWritable key, final Raster raster) throws IOException
    {
      writer.append(key, RasterWritable.toWritable(raster));
    }



    @Override
    public void close() throws IOException
    {
      writer.close();
    }

    @Override
    public String getName()
    {
      return image;
    }
//    public long getLength()
//    {
//      try
//      {
//        return writer.getLength();
//      }
//      catch (final IOException e)
//      {
//        throw new MrsImageException(e);
//      }
//    }

  }

  public HdfsImageIngestDataProvider(Configuration conf, String resourceName)
  {
    super(resourceName);
    this.conf = conf;
  }


  @Override
  public InputStream openImage() throws IOException
  {

    Path path = new Path(getResourceName());

    final FileSystem fs = HadoopFileUtils.getFileSystem(conf, path);

    if (fs.exists(path))
    {
      final InputStream stream = fs.open(path, 131072); // give open a 128K buffer

      Configuration localConf = HadoopUtils.createConfiguration();
      // see if were compressed
      final CompressionCodecFactory factory = new CompressionCodecFactory(localConf);
      final CompressionCodec codec = factory.getCodec(path);

      if (codec != null)
      {
        return new HadoopFileUtils.CompressedSeekableStream(codec.createInputStream(stream));
      }

      return stream;
    }

    throw new FileNotFoundException("File not found: " + path.toUri().toString());
  }


  @Override
  public ImageIngestTiledInputFormatProvider getTiledInputFormat()
  {
    return new HdfsImageIngestTiledInputFormatProvider();
  }

  @Override
  public MrsTileWriter<Raster> getMrsTileWriter(ImageIngestWriterContext context) throws IOException
  {
    Path imagePath = new Path(getResourceName());
    if (context != null)
    {
      imagePath = new Path(imagePath, context.getZoomlevel() + "/part-" + String.format("%05d", context.getPartNum()));
    }
    else
    {
      throw new IOException("Context is not present.  Look at this if it's correct behaviour...  May need to uncomment the line below this...");
      //imagePath = new Path(imagePath, "/part-00000");      
    }
    return new HdfsSequenceWriter(imagePath.toString());
  }


  @Override
  public ImageIngestRawInputFormatProvider getRawInputFormat()
  {
    return new HdfsImageIngestRawInputFormatProvider();
  }

  @Override
  public void delete() throws IOException
  {
    delete(conf, getResourceName());
  }

  @Override
  public void move(final String toResource) throws IOException
  {
    HadoopFileUtils.move(conf, getResourceName(), toResource);
  }

  public static boolean canOpen(final Configuration conf, final String name) throws IOException
  {
    final Path imagePath = new Path(name);
    final FileSystem fs = HadoopFileUtils.getFileSystem(conf, imagePath);
    return fs.exists(imagePath) && fs.isFile(imagePath);
  }

  public static boolean canWrite(final Configuration conf, final String name) throws IOException
  {
    final Path imagePath = new Path(name);
    final FileSystem fs = HadoopFileUtils.getFileSystem(conf, imagePath);
    return !fs.exists(imagePath);
  }


  public static boolean exists(Configuration conf, String name) throws IOException
  {
    return HadoopFileUtils.exists(conf, name);
  }


  public static void delete(Configuration conf, String name) throws IOException
  {
    HadoopFileUtils.delete(conf, name);
  }
}
