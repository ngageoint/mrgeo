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

package org.mrgeo.data.raster;

import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.test.LocalRunnerTest;
import org.mrgeo.test.TestUtils;

import java.awt.image.DataBuffer;
import java.io.IOException;

/* This test handles all four cases of RasterWritable:
 * 1. No Compression and No Payload
 * 2. No Compression and Payload
 * 3. Compression and No Payload
 * 4. Compression and Payload 
 * 
 * For the compression test to use the correct codec, which at the time of this writing, is 
 * org.apache.hadoop.io.GzipCodec, the java.library.path needs to be set. If it
 * is not set, the test uses org.apache.hadoop.io.compress.DefaultCodec and will pass happily. 
 * 
 * To use the correct codec, please copy the following into the VM args section of your Eclipse 
 * run configuration for this test :
 * 		-Djava.library.path=/usr/local/hadoop/bin/../lib/native/Linux-amd64-64
 *  
 */
public class RasterWritableTest extends LocalRunnerTest
{
private MrGeoRaster srcRaster;

private int RASTER_SIZE = 10;
private double PIXEL_VALUE = 1.0;
private int NUM_ENTRIES = 5;

//	private CompressionCodec codec;
//	private Decompressor decompressor;
//	private Compressor compressor;

private static Path outputHdfs;

@BeforeClass
public static void init() throws IOException
{
  outputHdfs = TestUtils.composeOutputHdfs(RasterWritableTest.class);
}

@Before
public void setUp() throws IOException
{
  srcRaster = TestUtils.createConstRaster(RASTER_SIZE, RASTER_SIZE,
      DataBuffer.TYPE_FLOAT, PIXEL_VALUE);
}

@Test
@Category(UnitTest.class)
public void testNoCompressNoPayloadRaster() throws IOException
{
  testRaster("testNoCompressNoPayloadRaster");
}

private void testRaster(String testName) throws IOException
{
  Path rasterFilePath = new Path(outputHdfs, "raster" + testName + ".seq");

  FileSystem fs = rasterFilePath.getFileSystem(conf);
  SequenceFile.Writer writer =
      SequenceFile.createWriter(fs, conf, rasterFilePath, TileIdWritable.class, RasterWritable.class);

  TileIdWritable key = new TileIdWritable();
  RasterWritable value;

  TileIdWritable payload = null;
  for (int i = 0; i < NUM_ENTRIES; i++)
  {
    key.set(i);

    value = RasterWritable.toWritable(TestUtils.createConstRaster(RASTER_SIZE, RASTER_SIZE,
        DataBuffer.TYPE_FLOAT, i));

    writer.append(key, value);
  }

  writer.close();

  int foundNumEntries = 0;
  TileIdWritable foundKey = new TileIdWritable();
  RasterWritable foundValue = new RasterWritable();

  payload = new TileIdWritable();
  SequenceFile.Reader reader = new SequenceFile.Reader(fs, rasterFilePath, conf);
  while (reader.next(foundKey, foundValue))
  {
    // check key
    Assert.assertEquals(foundNumEntries, foundKey.get());

    // check if api returns no payload raster (compressed/uncompressed) correctly
    MrGeoRaster destRaster = RasterWritable.toMrGeoRaster(foundValue);

    TestUtils.compareRasterToConstant(destRaster, foundNumEntries, -1);

    foundNumEntries++;
  }
  reader.close();
  Assert.assertEquals(NUM_ENTRIES, foundNumEntries);
}
}
