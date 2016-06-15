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

import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.core.Defs;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.test.LocalRunnerTest;
import org.mrgeo.utils.HadoopUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class HdfsMrsImageDataProviderFactoryTest extends LocalRunnerTest
{
  HdfsMrsImageDataProviderFactory factory;
  Configuration conf;
  ProviderProperties providerProperties;


  private static String all_ones = Defs.INPUT + "all-ones";

  private String oldImageBase;
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
    oldImageBase = MrGeoProperties.getInstance().getProperty(MrGeoConstants.MRGEO_HDFS_IMAGE, "");

    MrGeoProperties.getInstance().setProperty(MrGeoConstants.MRGEO_HDFS_IMAGE, (new File(Defs.INPUT)).toURI().toString());
    factory = new HdfsMrsImageDataProviderFactory();
    conf = HadoopUtils.createConfiguration();
    factory.initialize(conf);
    providerProperties = new ProviderProperties();
  }

  @After
  public void teardown()
  {
    MrGeoProperties.getInstance().setProperty(MrGeoConstants.MRGEO_HDFS_IMAGE, oldImageBase);
  }

  @Test
  @Category(UnitTest.class)
  public void testGetPrefix() throws Exception
  {
    Assert.assertEquals("Bad prefix", "hdfs", factory.getPrefix());
  }

  @Test
  @Category(UnitTest.class)
  public void testCreateMrsImageDataProvider2() throws Exception
  {
    String name = "foo";
    MrsImageDataProvider provider = factory.createMrsImageDataProvider(name, providerProperties);

    Assert.assertNotNull("Provider not created!", provider);
    Assert.assertEquals("Name not set properly", name, provider.getResourceName());
  }

  @Test
  @Category(UnitTest.class)
  public void testCanOpen2() throws Exception
  {
    Assert.assertTrue("Can not open image!", factory.canOpen("all-ones", providerProperties));
  }

  @Test
  @Category(UnitTest.class)
  public void testCanOpenMissing2() throws Exception
  {
    Assert.assertFalse("Can not open image!", factory.canOpen( "missing", providerProperties));
  }

  @Test
  @Category(UnitTest.class)
  public void testCanOpenBadUri2() throws Exception
  {
    boolean result = factory.canOpen( "abcd:bad-name", providerProperties);
    Assert.assertFalse(result);
  }

  @Test(expected = NullPointerException.class)
  @Category(UnitTest.class)
  public void testCanOpenNull2() throws Exception
  {
    factory.canOpen( null, providerProperties);
  }

  @Test
  @Category(UnitTest.class)
  public void testExists2() throws Exception
  {
    Assert.assertTrue("Can not open file!", factory.exists(all_ones, providerProperties));
  }

  @Test
  @Category(UnitTest.class)
  public void testExistsMissing2() throws Exception
  {
    Assert.assertFalse("Can not open file!", factory.exists( "missing", providerProperties));
  }

  @Test
  @Category(UnitTest.class)
  public void testExistsBadUri2() throws Exception
  {
    Assert.assertFalse(factory.exists( "abcd:bad-name", providerProperties));
  }

  @Test(expected = NullPointerException.class)
  @Category(UnitTest.class)
  public void testExistsNull2() throws Exception
  {
    factory.exists( null, providerProperties);
  }

  @Test
  @Category(UnitTest.class)
  public void testCanWrite2() throws Exception
  {
    Assert.assertFalse("Can not write existing file!", factory.canWrite(all_ones, providerProperties));
  }

  @Test
  @Category(UnitTest.class)
  public void testCanWriteMissing2() throws Exception
  {
    Assert.assertTrue("Can not write!", factory.canWrite( "missing", providerProperties));
  }

  @Test
  @Category(UnitTest.class)
  public void testCanWriteBadUri2() throws Exception
  {
    Assert.assertFalse(factory.canWrite( "abcd:bad-name", providerProperties));
  }

  @Test(expected = NullPointerException.class)
  @Category(UnitTest.class)
  public void testCanWriteNull2() throws Exception
  {
    factory.canWrite( null, providerProperties);
  }

  @Test
  @Category(UnitTest.class)
  public void testListImages() throws Exception
  {
    File dir = new File(Defs.INPUT);
    File[] files = dir.listFiles();

    List<String> base = new ArrayList<String>();

    for (File file: files)
    {
      if (file.isDirectory())
      {
        File metadataFile = new File(file, "metadata");
        if (metadataFile.exists())
        {
          base.add(file.getName());
        }
      }
    }
    Collections.sort(base);

    String[] images = factory.listImages(providerProperties);
    Arrays.sort(images);

    Assert.assertEquals("Wrong number of images!", base.size(), images.length);

    for (int i = 0; i < base.size(); i++)
    {
      Assert.assertEquals("Bad image", base.get(i), images[i]);
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testListImagesNoImages() throws Exception
  {
    Path p = HadoopFileUtils.createUniqueTmpPath();
    HadoopFileUtils.create(p);

    try
    {
      MrGeoProperties.getInstance().setProperty(MrGeoConstants.MRGEO_HDFS_IMAGE, p.toUri().toString());
      // test with conf
      String[] images = factory.listImages(providerProperties);

      Assert.assertNotNull("Shouldn't be null!", images);
      Assert.assertEquals("Length should be 0!", 0, images.length);
    }
    finally
    {
      HadoopFileUtils.delete(p);
    }
  }


  @Test(expected = FileNotFoundException.class)
  @Category(UnitTest.class)
  public void testListImagesBadBase() throws Exception
  {
    MrGeoProperties.getInstance().setProperty(MrGeoConstants.MRGEO_HDFS_IMAGE, "bad-name");
    factory.listImages(providerProperties);
  }

  @Test(expected = IOException.class)
  @Category(UnitTest.class)
  public void testListImagesBadUri() throws Exception
  {
    MrGeoProperties.getInstance().setProperty(MrGeoConstants.MRGEO_HDFS_IMAGE, "abcd:bad-name");
    factory.listImages(providerProperties);
  }

  @Test
  @Category(UnitTest.class)
  public void testListImagesNoBase1() throws Exception
  {
    MrGeoProperties.getInstance().remove(MrGeoConstants.MRGEO_HDFS_IMAGE);

    // should fall back to /mrgeo/images
    FileSystem fs = HadoopFileUtils.getFileSystem(conf);
    if (fs.exists(new Path("/mrgeo/images")))
    {
      String[] images = factory.listImages(providerProperties);

      // can't really test _what_ listImages will return, just that it isn't null
      Assert.assertNotNull("Shouldn't be null!", images);
    }
    else
    {
      try
      {
        factory.listImages(providerProperties);

        Assert.fail("This should have thrown a FileNotFoundException!");
      }
      catch (FileNotFoundException e)
      {
      }
    }

  }

  @Test
  @Category(UnitTest.class)
  public void testDelete2() throws Exception
  {
    String name = HadoopFileUtils.createUniqueTmpPath().toString();
    HadoopFileUtils.copyToHdfs(all_ones, name);

    Assert.assertTrue("Image should exist!", HadoopFileUtils.exists(name));
    factory.delete(name, providerProperties);

    Assert.assertFalse("Image was not deleted!", HadoopFileUtils.exists(name));

  }
}