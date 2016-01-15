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

package org.mrgeo.hdfs.image;

import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.core.Defs;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.data.image.*;
import org.mrgeo.data.tile.TiledInputFormatContext;
import org.mrgeo.data.tile.TiledOutputFormatContext;
import org.mrgeo.hdfs.input.image.HDFSMrsImagePyramidRecordReader;
import org.mrgeo.hdfs.input.image.HdfsMrsImagePyramidInputFormatProvider;
import org.mrgeo.hdfs.metadata.HdfsMrsImagePyramidMetadataReader;
import org.mrgeo.hdfs.metadata.HdfsMrsImagePyramidMetadataWriter;
import org.mrgeo.hdfs.output.image.HdfsMrsImagePyramidOutputFormatProvider;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.test.LocalRunnerTest;
import org.mrgeo.utils.Bounds;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class HdfsMrsImageDataProviderTest extends LocalRunnerTest
{
  private static String all_ones = Defs.INPUT + "all-ones";
  HdfsMrsImageDataProvider provider = null;

  @BeforeClass
  static public void setup() throws IOException
  {
    File f = new File(all_ones);
    all_ones = f.toURI().toString();

    // strip the final "/" from the path
    all_ones = all_ones.substring(0, all_ones.length() - 1);
  }


  @Before
  public void init()
  {
    MrGeoProperties.getInstance().setProperty(MrGeoConstants.MRGEO_HDFS_IMAGE, (new File(Defs.INPUT)).toURI().toString());
    provider = new HdfsMrsImageDataProvider(conf, all_ones, null);
  }

  @After
  public void tearDown() throws IOException
  {
  }

  @Test
  @Category(UnitTest.class)
  public void testGetResolvedResourceName() throws Exception
  {
    Assert.assertEquals("Resolved Resource name is wrong!", all_ones, provider.getResolvedResourceName(true));
  }

  @Category(UnitTest.class)
  public void testGetResolvedResourceNameDoesNotExist() throws Exception
  {
    HdfsMrsImageDataProvider provider = new HdfsMrsImageDataProvider(conf, "new-name", null);
    Assert.assertNull(provider.getResolvedResourceName(false));
  }

  @Category(UnitTest.class)
  public void testGetResolvedResourceNameForWrite() throws Exception
  {
    HdfsMrsImageDataProvider provider = new HdfsMrsImageDataProvider(conf, "new-name", null);
    Assert.assertNotNull(provider.getResolvedResourceName(true));
  }

  @Test(expected = IOException.class)
  @Category(UnitTest.class)
  public void testGetResolvedResourceNameInvalidUri() throws Exception
  {
    HdfsMrsImageDataProvider badprovider = new HdfsMrsImageDataProvider(conf, "abcd:bad-name", null);
    badprovider.getResolvedResourceName(true);
  }

  @Test
  @Category(UnitTest.class)
  public void testGetResourcePath() throws Exception
  {
    Assert.assertEquals("Resolved Resource name is wrong!", all_ones, provider.getResourcePath(true).toUri().toString());
  }

  @Test(expected = NullPointerException.class)
  @Category(UnitTest.class)
  public void testGetResourceNull() throws Exception
  {
    HdfsMrsImageDataProvider badprovider = new HdfsMrsImageDataProvider(conf, null, null);
    badprovider.getResourcePath(true);
  }

  @Test
  @Category(UnitTest.class)
  public void testGetResourceLocal() throws Exception
  {
    String name = all_ones.replace("file:", "");

    HdfsMrsImageDataProvider local = new HdfsMrsImageDataProvider(conf, name, null);

    Assert
        .assertEquals("Resolved Resource name is wrong!", "file://" + name, local.getResourcePath(true).toUri().toString());
  }

  @Test
  @Category(UnitTest.class)
  public void testGetResourceBasePath() throws Exception
  {
    String name = "all-ones";

    HdfsMrsImageDataProvider base = new HdfsMrsImageDataProvider(conf, name, null);
    Assert.assertEquals("Resolved Resource name is wrong!", all_ones, base.getResolvedResourceName(true));
  }


  @Test
  @Category(UnitTest.class)
  public void testGetTiledInputFormatProvider() throws Exception
  {
    String input = all_ones;
    TiledInputFormatContext context = new TiledInputFormatContext(10, MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT_INT, input, null);

    MrsImageInputFormatProvider p = provider.getTiledInputFormatProvider(context);

    Assert.assertEquals("Wrong class", HdfsMrsImagePyramidInputFormatProvider.class, p.getClass());
  }

  @Test
  @Category(UnitTest.class)
  public void testGetTiledOutputFormatProvider() throws Exception
  {
    TiledOutputFormatContext context = new TiledOutputFormatContext("foo", new Bounds(), 10, MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT_INT, "");

    MrsImageOutputFormatProvider p = provider.getTiledOutputFormatProvider(context);

    Assert.assertEquals("Wrong class", HdfsMrsImagePyramidOutputFormatProvider.class, p.getClass());

  }

  @Test
  @Category(UnitTest.class)
  public void testGetRecordReader() throws Exception
  {
    Assert.assertEquals("Wrong class", HDFSMrsImagePyramidRecordReader.class, provider.getRecordReader().getClass());
  }

  @Test
  @Category(UnitTest.class)
  public void testGetRecordWriter() throws Exception
  {
    Assert.assertEquals("Wrong class", null, provider.getRecordWriter());
  }

  @Test
  @Category(UnitTest.class)
  public void testGetMrsTileReader() throws Exception
  {
    MrsImagePyramidReaderContext context = new MrsImagePyramidReaderContext(10);

    Assert.assertEquals("Wrong class", HdfsMrsImageReader.class, provider.getMrsTileReader(context).getClass());
  }

  @Test
  @Category(UnitTest.class)
  public void testGetMrsTileWriter() throws Exception
  {
    MrsImagePyramidWriterContext context = new MrsImagePyramidWriterContext(10, 1, "");

    Assert.assertEquals("Wrong class", HdfsMrsImageWriter.class, provider.getMrsTileWriter(context).getClass());
  }

  @Test
  @Category(UnitTest.class)
  public void testGetMetadataReader() throws Exception
  {
    MrsImagePyramidMetadataReaderContext context = new MrsImagePyramidMetadataReaderContext();

    MrsImagePyramidMetadataReader reader = provider.getMetadataReader(context);

    Assert.assertEquals("Wrong class", HdfsMrsImagePyramidMetadataReader.class, reader.getClass());

    Assert.assertEquals("Should be same object", reader, provider.getMetadataReader(context));
  }

  @Test
  @Category(UnitTest.class)
  public void testGetMetadataWriter() throws Exception
  {
    MrsImagePyramidMetadataWriterContext context = new MrsImagePyramidMetadataWriterContext();

    MrsImagePyramidMetadataWriter writer = provider.getMetadataWriter(context);

    Assert.assertEquals("Wrong class", HdfsMrsImagePyramidMetadataWriter.class, writer.getClass());

    Assert.assertEquals("Should be same object", writer, provider.getMetadataWriter(context));
  }

  @Test
  @Category(UnitTest.class)
  public void testDelete() throws Exception
  {
    String name = HadoopFileUtils.createUniqueTmpPath().toString();
    HadoopFileUtils.copyToHdfs(all_ones, name);
    HdfsMrsImageDataProvider deleter = new HdfsMrsImageDataProvider(conf, name, null);

    Assert.assertTrue("Image should exist!", HadoopFileUtils.exists(name));
    Assert.assertNotNull("Image doesn't exist!", deleter.getResourceName());

    deleter.delete();

    Assert.assertFalse("Image was not deleted!", HadoopFileUtils.exists(name));
  }

  @Test
  @Category(UnitTest.class)
  public void testDeleteStatic() throws Exception
  {
    String name = HadoopFileUtils.createUniqueTmpPath().toString();
    HadoopFileUtils.copyToHdfs(all_ones, name);

    Assert.assertTrue("Image should exist!", HadoopFileUtils.exists(name));
    HdfsMrsImageDataProvider.delete(conf, name, null);

    Assert.assertFalse("Image was not deleted!", HadoopFileUtils.exists(name));

  }

  @Test
  @Category(UnitTest.class)
  public void testDeleteMissing() throws Exception
  {
    // should just quietly do nothing
    HdfsMrsImageDataProvider.delete(conf, "bad-name", null);
  }

  @Test(expected = IOException.class)
  @Category(UnitTest.class)
  public void testDeleteBadUri() throws Exception
  {
    HdfsMrsImageDataProvider.delete(conf, "abcd:bad-name", null);
  }

  @Test(expected = NullPointerException.class)
  @Category(UnitTest.class)
  public void testDeleteBadNull() throws Exception
  {
    HdfsMrsImageDataProvider.delete(conf, null, null);
  }

  @Test
  @Category(UnitTest.class)
  public void testMove() throws Exception
  {
    String name = HadoopFileUtils.createUniqueTmpPath().toString();
    HadoopFileUtils.copyToHdfs(all_ones, name);

    HdfsMrsImageDataProvider mover = new HdfsMrsImageDataProvider(conf, name, null);

    Assert.assertTrue("Image should exist!", HadoopFileUtils.exists(name));
    Assert.assertNotNull("Image doesn't exist!", mover.getResourceName());

    String newname = HadoopFileUtils.createUniqueTmpPath().toString();
    mover.move(newname);

    Assert.assertFalse("Image was not moved!", HadoopFileUtils.exists(name));
    Assert.assertTrue("Image was not moved!", HadoopFileUtils.exists(newname));

    // cleanup
    HdfsMrsImageDataProvider.delete(conf, newname, null);

    Assert.assertFalse("Image was not deleted!", HadoopFileUtils.exists(newname));

  }

  @Test
  @Category(UnitTest.class)
  public void testCanOpen() throws Exception
  {
    Assert.assertTrue("Can not open file!", HdfsMrsImageDataProvider.canOpen(conf, all_ones, null));
  }

  @Test
  @Category(UnitTest.class)
  public void testCanOpenMissing() throws Exception
  {
    Assert.assertFalse("Can not open file!", HdfsMrsImageDataProvider.canOpen(conf, "missing", null));
  }

  @Test
  @Category(UnitTest.class)
  public void testCanOpenBadUri() throws Exception
  {
    boolean result = HdfsMrsImageDataProvider.canOpen(conf, "abcd:bad-name", null);
    Assert.assertFalse(result);
  }

  @Test(expected = NullPointerException.class)
  @Category(UnitTest.class)
  public void testCanOpenNull() throws Exception
  {
    HdfsMrsImageDataProvider.canOpen(conf, null, null);
  }

  @Test
  @Category(UnitTest.class)
  public void testExists() throws Exception
  {
    Assert.assertTrue("Can not open file!", HdfsMrsImageDataProvider.exists(conf, all_ones, null));
  }

  @Test
  @Category(UnitTest.class)
  public void testExistsMissing() throws Exception
  {
    Assert.assertFalse("Can not open file!", HdfsMrsImageDataProvider.exists(conf, "missing", null));
  }

  @Test
  @Category(UnitTest.class)
  public void testExistsBadUri() throws Exception
  {
    Assert.assertFalse(HdfsMrsImageDataProvider.exists(conf, "abcd:bad-name", null));
  }

  @Test(expected = NullPointerException.class)
  @Category(UnitTest.class)
  public void testExistsNull() throws Exception
  {
    HdfsMrsImageDataProvider.exists(conf, null, null);
  }

  @Test
  @Category(UnitTest.class)
  public void testCanWrite() throws Exception
  {
    Assert.assertFalse("Can not write existing file!", HdfsMrsImageDataProvider.canWrite(conf, all_ones, null));
  }

  @Test
  @Category(UnitTest.class)
  public void testCanWriteMissing() throws Exception
  {
    Assert.assertTrue("Can not write!", HdfsMrsImageDataProvider.canWrite(conf, "missing", null));
  }

  @Test
  @Category(UnitTest.class)
  public void testCanWriteBadUri() throws Exception
  {
    Assert.assertFalse(HdfsMrsImageDataProvider.canWrite(conf, "abcd:bad-name", null));
  }

  @Test(expected = NullPointerException.class)
  @Category(UnitTest.class)
  public void testCanWriteNull() throws Exception
  {
    HdfsMrsImageDataProvider.canWrite(conf, null, null);
  }
}