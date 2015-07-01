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

package org.mrgeo.vector.mrsvector;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class VectorTileWritable extends BytesWritable
{

  public VectorTileWritable()
  {
  }

  public VectorTileWritable(final byte[] bytes)
  {
    super(bytes);
  }

  public static VectorTile toMrsVector(final VectorTileWritable writable, final CompressionCodec codec,
    final Decompressor decompressor) throws IOException
  {
    decompressor.reset();
    final ByteArrayInputStream bis = new ByteArrayInputStream(writable.getBytes(), 0, writable.getLength());
    final CompressionInputStream gis = codec.createInputStream(bis, decompressor);
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    IOUtils.copyBytes(gis, baos, 1024 * 1024 * 2, true);

    byte[] data = baos.toByteArray();
    return VectorTile.fromProtobuf(data, 0, data.length);
  }

  public static VectorTile toMrsVector(final VectorTileWritable writable)
    throws IOException
  {
    return VectorTile.fromProtobuf(writable.getBytes(), 0, writable.getLength());
  }

  public static VectorTileWritable toWritable(final VectorTile vector, final CompressionCodec codec,
    final Compressor compressor) throws IOException
  {
    compressor.reset();

    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final CompressionOutputStream cos = codec.createOutputStream(baos, compressor);

    vector.toProtobuf(cos);

    return new VectorTileWritable(baos.toByteArray());
  }

  public static VectorTileWritable toWritable(final VectorTile feature)
    throws IOException
  {
    byte[] bytes = feature.toProtobuf();

    return new VectorTileWritable(bytes);
  }

}
