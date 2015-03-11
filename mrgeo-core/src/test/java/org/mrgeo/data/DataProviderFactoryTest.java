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

package org.mrgeo.data;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.core.Defs;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.adhoc.AdHocDataProvider;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.data.ingest.ImageIngestDataProvider;
import org.mrgeo.data.vector.VectorDataProvider;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.test.LocalRunnerTest;
import org.mrgeo.utils.HadoopUtils;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class DataProviderFactoryTest extends LocalRunnerTest
{
  private static String all_ones;
  private static String all_ones_input = Defs.INPUT + "all-ones";
  private static String test_tsv;
  private static String test_tsv_filename = "test1.tsv";
  private static String test_tsv_columns_filename = test_tsv_filename + ".columns";
  private static String test_tsv_input = Defs.INPUT + test_tsv_filename;
  private Properties providerProperties;

  @BeforeClass
  static public void setup() throws IOException
  {
    File f = new File(all_ones_input);
    all_ones = "file://" + f.getCanonicalPath();
    File f1 = new File(test_tsv_input);
    test_tsv = "file://" + f1.getCanonicalPath();
  }

  @Before
  public void init()
  {
    providerProperties = new Properties();
    DataProviderFactory.initialize(HadoopUtils.createConfiguration(), providerProperties);
    DataProviderFactory.invalidateCache();
  }


  @Test
  @Category(UnitTest.class)
  public void testGetMrsImageDataProviderForReadWhenMissing() throws IOException
  {
    // Resource does not exist
    {
      String missingResource = "DoesNotExist";
      try
      {
        @SuppressWarnings("unused")
        MrsImageDataProvider dp = DataProviderFactory.getMrsImageDataProvider(
            missingResource, AccessMode.READ, providerProperties);
        Assert.fail("Expected DataProviderNotFound exception");
      }
      catch(DataProviderNotFound e)
      {
        // expected
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testGetMrsImageDataProviderForReadWhenExists() throws IOException
  {
    MrsImageDataProvider dp = DataProviderFactory.getMrsImageDataProvider(all_ones,
        AccessMode.READ, providerProperties);
    Assert.assertNotNull(dp);
  }

  @Test
  @Category(UnitTest.class)
  public void testGetMrsImageDataProviderForOverwriteWhenMissing() throws IOException
  {
    // Using an existing resource
    String missingResource = "DoesNotExist";
    MrsImageDataProvider dp = DataProviderFactory.getMrsImageDataProvider(missingResource,
        AccessMode.OVERWRITE, providerProperties);
    Assert.assertNotNull(dp);
    Assert.assertFalse(HadoopFileUtils.exists(missingResource));
  }


  @Test
  @Category(UnitTest.class)
  public void testGetMrsImageDataProviderForOverwriteWhenExists() throws IOException
  {
    // Using an existing resource
    File f = File.createTempFile(HadoopUtils.createRandomString(10), "");
    String tmpDir = "file://" + f.getCanonicalPath();

    HadoopFileUtils.copyToHdfs(all_ones, tmpDir, true);
    String tempPath = "file://" + new File(f, "all-ones").toString();
    try
    {
      MrsImageDataProvider dp = DataProviderFactory.getMrsImageDataProvider(
          tempPath.toString(), AccessMode.OVERWRITE, providerProperties);
      Assert.assertNotNull(dp);
      Assert.assertFalse(HadoopFileUtils.exists(tempPath));
    }
    finally
    {
      HadoopFileUtils.delete(tmpDir);
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testGetMrsImageDataProviderForOverwriteAfterRead() throws IOException
  {
    // Using an existing resource
    File f = File.createTempFile(HadoopUtils.createRandomString(10), "");
    String tmpDir = "file://" + f.getCanonicalPath();

    HadoopFileUtils.copyToHdfs(all_ones, tmpDir, true);
    String tempPath = "file://" + new File(f, "all-ones").toString();
    try
    {
      MrsImageDataProvider dp = DataProviderFactory.getMrsImageDataProvider(tempPath,
          AccessMode.READ, providerProperties);
      dp = DataProviderFactory.getMrsImageDataProvider(tempPath, AccessMode.OVERWRITE,
          providerProperties);
      Assert.assertNotNull(dp);
      Assert.assertFalse(HadoopFileUtils.exists(tempPath));
    }
    finally
    {
      HadoopFileUtils.delete(tmpDir);
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testGetMrsImageDataProviderForWriteWhenMissing() throws IOException
  {
    // Using an existing resource
    String missingResource = "DoesNotExist";
    MrsImageDataProvider dp = DataProviderFactory.getMrsImageDataProvider(missingResource,
        AccessMode.WRITE, providerProperties);
    Assert.assertNotNull(dp);
  }

  @Test
  @Category(UnitTest.class)
  public void testGetMrsImageDataProviderForWriteWhenExisting() throws IOException
  {
    try
    {
      @SuppressWarnings("unused")
      MrsImageDataProvider dp = DataProviderFactory.getMrsImageDataProvider(all_ones,
          AccessMode.WRITE, providerProperties);
      Assert.fail("Should not be able to use WRITE with an existing resource");
    }
    catch(DataProviderNotFound e)
    {
      // expected
    }
  }

  @Test(expected = DataProviderNotFound.class)
  @Category(UnitTest.class)
  public void testGetMrsImageDataProviderForWriteAfterRead() throws IOException
  {
    // Using an existing resource
    Path testPath = HadoopFileUtils.createUniqueTmp();
    try
    {
      @SuppressWarnings("unused")
      MrsImageDataProvider dp = DataProviderFactory.getMrsImageDataProvider(
          testPath.toString(), AccessMode.READ, providerProperties);
      dp = DataProviderFactory.getMrsImageDataProvider(testPath.toString(),
          AccessMode.WRITE, providerProperties);

      Assert.fail("Should not be able to get a WRITE provider when the resource exists");
    }
    finally
    {
      HadoopFileUtils.delete(testPath);
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testGetAdHocDataProviderForReadWhenMissing() throws IOException
  {
    // Resource does not exist
    {
      String missingResource = "DoesNotExist";
      try
      {
        @SuppressWarnings("unused")
        AdHocDataProvider dp = DataProviderFactory.getAdHocDataProvider(missingResource,
            AccessMode.READ, providerProperties);
        Assert.fail("Expected DataProviderNotFound exception");
      }
      catch(DataProviderNotFound e)
      {
        // expected
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testGetAdHocDataProviderForReadWhenExists() throws IOException
  {
    // Using an existing resource
    Path testPath = HadoopFileUtils.createUniqueTmp();
    try
    {
      AdHocDataProvider dp = DataProviderFactory.getAdHocDataProvider(testPath.toString(),
          AccessMode.READ, providerProperties);
      Assert.assertNotNull(dp);
    }
    finally
    {
      HadoopFileUtils.delete(testPath);
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testGetAdHocDataProviderForOverwriteWhenMissing() throws IOException
  {
    // Using an existing resource
    String missingResource = "DoesNotExist";
    AdHocDataProvider dp = DataProviderFactory.getAdHocDataProvider(missingResource,
        AccessMode.OVERWRITE, providerProperties);
    Assert.assertNotNull(dp);
    Assert.assertFalse(HadoopFileUtils.exists(missingResource));
  }

  @Test
  @Category(UnitTest.class)
  public void testGetAdHocDataProviderForOverwriteWhenExists() throws IOException
  {
    // Using an existing resource
    Path testPath = HadoopFileUtils.createUniqueTmp();
    try
    {
      AdHocDataProvider dp = DataProviderFactory.getAdHocDataProvider(testPath.toString(),
          AccessMode.OVERWRITE, providerProperties);
      Assert.assertNotNull(dp);
      Assert.assertFalse(HadoopFileUtils.exists(testPath));
    }
    finally
    {
      HadoopFileUtils.delete(testPath);
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testGetAdHocDataProviderForOverwriteAfterRead() throws IOException
  {
    // Using an existing resource
    Path testPath = HadoopFileUtils.createUniqueTmp();
    try
    {
      AdHocDataProvider dp = DataProviderFactory.getAdHocDataProvider(testPath.toString(),
          AccessMode.READ, providerProperties);
      dp = DataProviderFactory.getAdHocDataProvider(testPath.toString(),
          AccessMode.OVERWRITE, providerProperties);
      Assert.assertNotNull(dp);
      Assert.assertFalse(HadoopFileUtils.exists(testPath));
    }
    finally
    {
      HadoopFileUtils.delete(testPath);
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testGetAdHocDataProviderForWriteWhenMissing() throws IOException
  {
    // Using an existing resource
    String missingResource = "DoesNotExist";
    AdHocDataProvider dp = DataProviderFactory.getAdHocDataProvider(missingResource,
        AccessMode.WRITE, providerProperties);
    Assert.assertNotNull(dp);
  }

  @Test
  @Category(UnitTest.class)
  public void testGetAdHocDataProviderForWriteWhenExisting() throws IOException
  {
    // Using an existing resource
    Path testPath = HadoopFileUtils.createUniqueTmp();
    try
    {
      try
      {
        AdHocDataProvider dp = DataProviderFactory.getAdHocDataProvider(testPath.toString(),
            AccessMode.WRITE, providerProperties);
        Assert.assertNotNull(dp);
//        Assert.fail("Should not be able to use WRITE with an existing resource");
      }
      catch(DataProviderNotFound e)
      {
        Assert.fail("Should be able to perform a WRITE even if the ad hoc resource exists");
        // expected
      }
    }
    finally
    {
      HadoopFileUtils.delete(testPath);
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testGetAdHocDataProviderForWriteAfterRead() throws IOException
  {
    // Using an existing resource
    Path testPath = HadoopFileUtils.createUniqueTmp();
    try
    {
      AdHocDataProvider dp = DataProviderFactory.getAdHocDataProvider(testPath.toString(),
          AccessMode.READ, providerProperties);
      try
      {
        dp = DataProviderFactory.getAdHocDataProvider(testPath.toString(),
            AccessMode.WRITE, providerProperties);
        Assert.assertNotNull(dp);
//        Assert.fail("Should not be able to get a WRITE provider when the resource exists");
      }
      catch(DataProviderNotFound e)
      {
        Assert.fail("Should be able to perform a WRITE even if the ad hoc resource exists");
        // expected
      }
    }
    finally
    {
      HadoopFileUtils.delete(testPath);
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testGetImageIngestDataProviderForReadWithMissing() throws IOException
  {
    String missingResource = "DoesNotExist";
    try
    {
      @SuppressWarnings("unused")
      ImageIngestDataProvider dp = DataProviderFactory.getImageIngestDataProvider(missingResource, AccessMode.READ);
      Assert.fail("Expected DataProviderNotFound exception");
    }
    catch(DataProviderNotFound e)
    {
      // expected
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testGetImageIngestDataProviderForReadWithDirectory() throws IOException
  {
    // Using an existing resource
    Path testPath = HadoopFileUtils.createUniqueTmp();
    try
    {
      @SuppressWarnings("unused")
      ImageIngestDataProvider dp = DataProviderFactory.getImageIngestDataProvider(testPath.toString(), AccessMode.READ);
      Assert.fail("Expected DataProviderNotFound exception because the resource is not a file");
    }
    catch(DataProviderNotFound e)
    {
      // expected
    }
    finally
    {
      HadoopFileUtils.delete(testPath);
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testGetImageIngestDataProviderForReadWithFile() throws IOException
  {
    // Using an existing resource
    Path testPath = HadoopFileUtils.createUniqueTmp();
    Path testFile = new Path(testPath, "test.txt");
    try
    {
      // Now try with a file - should work
      FileSystem fs = HadoopFileUtils.getFileSystem(testPath);
      FSDataOutputStream os = fs.create(testFile);
      try
      {
        os.writeUTF("test");
      }
      finally
      {
        os.close();
      }
      ImageIngestDataProvider dp = DataProviderFactory.getImageIngestDataProvider(testFile.toString(), AccessMode.READ);
      Assert.assertNotNull(dp);
    }
    finally
    {
      HadoopFileUtils.delete(testFile);
      HadoopFileUtils.delete(testPath);
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testGetImageIngestDataProviderForOverwriteWhenMissing() throws IOException
  {
    String missingResource = HadoopUtils.createRandomString(10);
    ImageIngestDataProvider dp = DataProviderFactory.getImageIngestDataProvider(missingResource, AccessMode.OVERWRITE);
    Assert.assertNotNull(dp);
    Assert.assertFalse(HadoopFileUtils.exists(missingResource));
  }

  @Test
  @Category(UnitTest.class)
  public void testGetImageIngestDataProviderForOverwriteWhenExists() throws IOException
  {
    // Using an existing resource
    Path testPath = HadoopFileUtils.createUniqueTmp();
    try
    {
      ImageIngestDataProvider dp = DataProviderFactory.getImageIngestDataProvider(testPath.toString(), AccessMode.OVERWRITE);
      Assert.assertNotNull(dp);
      Assert.assertFalse(HadoopFileUtils.exists(testPath));
    }
    finally
    {
      HadoopFileUtils.delete(testPath);
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testGetImageIngestDataProviderForOverwriteAfterRead() throws IOException
  {
    // Using an existing resource
    Path testPath = HadoopFileUtils.createUniqueTmp();
    Path testFile = new Path(testPath, "test.txt");
    try
    {
      // Now try with a file - should work
      FileSystem fs = HadoopFileUtils.getFileSystem(testPath);
      FSDataOutputStream os = fs.create(testFile);
      try
      {
        os.writeUTF("test");
      }
      finally
      {
        os.close();
      }
      ImageIngestDataProvider dp = DataProviderFactory.getImageIngestDataProvider(testFile.toString(), AccessMode.READ);
      dp = DataProviderFactory.getImageIngestDataProvider(testFile.toString(), AccessMode.OVERWRITE);
      Assert.assertNotNull(dp);
      Assert.assertFalse(HadoopFileUtils.exists(testFile));
    }
    finally
    {
      HadoopFileUtils.delete(testFile);
      HadoopFileUtils.delete(testPath);
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testGetImageIngestDataProviderForWriteWhenMissing() throws IOException
  {
    // Using an existing resource
    String missingResource = "DoesNotExist";
    ImageIngestDataProvider dp = DataProviderFactory.getImageIngestDataProvider(missingResource, AccessMode.WRITE);
    Assert.assertNotNull(dp);
  }

  @Test
  @Category(UnitTest.class)
  public void testGetImageIngestDataProviderForWriteWhenExisting() throws IOException
  {
    // Using an existing resource
    Path testPath = HadoopFileUtils.createUniqueTmp();
    try
    {
      try
      {
        @SuppressWarnings("unused")
        ImageIngestDataProvider dp = DataProviderFactory.getImageIngestDataProvider(testPath.toString(), AccessMode.WRITE);
        Assert.fail("Should not be able to use WRITE with an existing resource");
      }
      catch(DataProviderNotFound e)
      {
        // expected
      }
    }
    finally
    {
      HadoopFileUtils.delete(testPath);
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testGetImageIngestDataProviderForWriteAfterRead() throws IOException
  {
    // Using an existing resource
    Path testPath = HadoopFileUtils.createUniqueTmp();
    Path testFile = new Path(testPath, "test.txt");
    try
    {
      // Now try with a file - should work
      FileSystem fs = HadoopFileUtils.getFileSystem(testPath);
      FSDataOutputStream os = fs.create(testFile);
      try
      {
        os.writeUTF("test");
      }
      finally
      {
        os.close();
      }
      ImageIngestDataProvider dp = DataProviderFactory.getImageIngestDataProvider(testFile.toString(), AccessMode.READ);
      try
      {
        dp = DataProviderFactory.getImageIngestDataProvider(testFile.toString(), AccessMode.WRITE);
        Assert.fail("Should not be able to get a WRITE provider when the resource exists");
      }
      catch(DataProviderNotFound e)
      {
        // expected
      }
    }
    finally
    {
      HadoopFileUtils.delete(testFile);
      HadoopFileUtils.delete(testPath);
    }
    // Using an existing resource
//    Path testPath = HadoopFileUtils.createUniqueTmp();
//    try
//    {
//      @SuppressWarnings("unused")
//      ImageIngestDataProvider dp = DataProviderFactory.getImageIngestDataProvider(testPath.toString(), AccessMode.READ);
//      try
//      {
//        dp = DataProviderFactory.getImageIngestDataProvider(testPath.toString(), AccessMode.WRITE);
//        Assert.fail("Should not be able to get a WRITE provider when the resource exists");
//      }
//      catch(DataProviderNotFound e)
//      {
//        // expected
//      }
//    }
//    finally
//    {
//      HadoopFileUtils.delete(testPath);
//    }
  }

  @Test
  @Category(UnitTest.class)
  public void testGetVectorDataProviderForReadWhenMissing() throws IOException
  {
    // Resource does not exist
    {
      String missingResource = "DoesNotExist";
      try
      {
        @SuppressWarnings("unused")
        VectorDataProvider dp = DataProviderFactory.getVectorDataProvider(
            missingResource, AccessMode.READ, providerProperties);
        Assert.fail("Expected DataProviderNotFound exception");
      }
      catch(DataProviderNotFound e)
      {
        // expected
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testGetVectorDataProviderForReadWhenExists() throws IOException
  {
    VectorDataProvider dp = DataProviderFactory.getVectorDataProvider(test_tsv,
        AccessMode.READ, providerProperties);
    Assert.assertNotNull(dp);
  }

  @Test
  @Category(UnitTest.class)
  public void testGetVectorDataProviderForOverwriteWhenMissing() throws IOException
  {
    // Using an existing resource
    String missingResource = "DoesNotExist";
    VectorDataProvider dp = DataProviderFactory.getVectorDataProvider(missingResource,
        AccessMode.OVERWRITE, providerProperties);
    Assert.assertNotNull(dp);
    Assert.assertFalse(HadoopFileUtils.exists(missingResource));
  }


  @Test
  @Category(UnitTest.class)
  public void testGetVectorDataProviderForOverwriteWhenExists() throws IOException
  {
    // Using an existing resource
    Path tmpDir = HadoopFileUtils.createUniqueTmp();

    Path testTsvPath = new Path(test_tsv);
    HadoopFileUtils.copyToHdfs(testTsvPath.getParent(), tmpDir, test_tsv_filename);
    HadoopFileUtils.copyToHdfs(testTsvPath.getParent(), tmpDir, test_tsv_columns_filename);
    Path hdfsTsvPath = new Path(tmpDir, test_tsv_filename);
    Path hdfsColumnsPath = new Path(tmpDir, test_tsv_columns_filename);
    try
    {
      VectorDataProvider dp = DataProviderFactory.getVectorDataProvider(
          hdfsTsvPath.toString(), AccessMode.OVERWRITE, providerProperties);
      Assert.assertNotNull(dp);
      Assert.assertFalse(HadoopFileUtils.exists(hdfsTsvPath));
      Assert.assertFalse(HadoopFileUtils.exists(hdfsColumnsPath));
    }
    finally
    {
      HadoopFileUtils.delete(tmpDir);
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testGetVectorDataProviderForOverwriteAfterRead() throws IOException
  {
    // Using an existing resource
    Path tmpDir = HadoopFileUtils.createUniqueTmp();

    Path testTsvPath = new Path(test_tsv);
    HadoopFileUtils.copyToHdfs(testTsvPath.getParent(), tmpDir, test_tsv_filename);
    HadoopFileUtils.copyToHdfs(testTsvPath.getParent(), tmpDir, test_tsv_columns_filename);
    Path hdfsTsvPath = new Path(tmpDir, test_tsv_filename);
    Path hdfsColumnsPath = new Path(tmpDir, test_tsv_columns_filename);
    try
    {
      VectorDataProvider dp = DataProviderFactory.getVectorDataProvider(hdfsTsvPath.toString(),
          AccessMode.READ, providerProperties);
      dp = DataProviderFactory.getVectorDataProvider(hdfsTsvPath.toString(), AccessMode.OVERWRITE,
          providerProperties);
      Assert.assertNotNull(dp);
      Assert.assertFalse(HadoopFileUtils.exists(hdfsTsvPath));
      Assert.assertFalse(HadoopFileUtils.exists(hdfsColumnsPath));
    }
    finally
    {
      HadoopFileUtils.delete(tmpDir);
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testGetVectorDataProviderForWriteWhenMissing() throws IOException
  {
    // Using an existing resource
    String missingResource = "DoesNotExist";
    VectorDataProvider dp = DataProviderFactory.getVectorDataProvider(missingResource,
        AccessMode.WRITE, providerProperties);
    Assert.assertNotNull(dp);
  }

  @Test
  @Category(UnitTest.class)
  public void testGetVectorDataProviderForWriteWhenExisting() throws IOException
  {
    try
    {
      @SuppressWarnings("unused")
      VectorDataProvider dp = DataProviderFactory.getVectorDataProvider(test_tsv,
          AccessMode.WRITE, providerProperties);
      Assert.fail("Should not be able to use WRITE with an existing resource");
    }
    catch(DataProviderNotFound e)
    {
      // expected
    }
  }

  @Test(expected = DataProviderNotFound.class)
  @Category(UnitTest.class)
  public void testGetVectorDataProviderForWriteAfterRead() throws IOException
  {
    // Using an existing resource
    Path testPath = HadoopFileUtils.createUniqueTmp();
    try
    {
      @SuppressWarnings("unused")
      VectorDataProvider dp = DataProviderFactory.getVectorDataProvider(
          testPath.toString(), AccessMode.READ, providerProperties);
      dp = DataProviderFactory.getVectorDataProvider(testPath.toString(),
          AccessMode.WRITE, providerProperties);

      Assert.fail("Should not be able to get a WRITE provider when the resource exists");
    }
    finally
    {
      HadoopFileUtils.delete(testPath);
    }
  }


  Map<String, String> oldConfValues = null;
  Map<String, String> oldPropValues = null;
  Map<String, String> oldMrGeoValues = null;

  private void setupPreferred(Configuration conf, Properties p, String confVal,
      String pVal, String defVal, String mrgeoVal, String defMrgeoVal)
  {

    if (conf != null)
    {
      oldConfValues = new HashMap<>();

      oldConfValues.put(DataProviderFactory.PREFERRED_ADHOC_PROVIDER_NAME,
          conf.get(DataProviderFactory.PREFERRED_ADHOC_PROVIDER_NAME, null));
      oldConfValues.put(DataProviderFactory.PREFERRED_INGEST_PROVIDER_NAME,
          conf.get(DataProviderFactory.PREFERRED_INGEST_PROVIDER_NAME, null));
      oldConfValues.put(DataProviderFactory.PREFERRED_MRSIMAGE_PROVIDER_NAME,
          conf.get(DataProviderFactory.PREFERRED_MRSIMAGE_PROVIDER_NAME, null));
      oldConfValues.put(DataProviderFactory.PREFERRED_VECTOR_PROVIDER_NAME,
          conf.get(DataProviderFactory.PREFERRED_VECTOR_PROVIDER_NAME, null));

      if (confVal == null)
      {
        conf.unset(DataProviderFactory.PREFERRED_ADHOC_PROVIDER_NAME);
        conf.unset(DataProviderFactory.PREFERRED_INGEST_PROVIDER_NAME);
        conf.unset(DataProviderFactory.PREFERRED_MRSIMAGE_PROVIDER_NAME);
        conf.unset(DataProviderFactory.PREFERRED_VECTOR_PROVIDER_NAME);
      }
      else
      {
        conf.set(DataProviderFactory.PREFERRED_ADHOC_PROVIDER_NAME, confVal);
        conf.set(DataProviderFactory.PREFERRED_INGEST_PROVIDER_NAME, confVal);
        conf.set(DataProviderFactory.PREFERRED_MRSIMAGE_PROVIDER_NAME, confVal);
        conf.set(DataProviderFactory.PREFERRED_VECTOR_PROVIDER_NAME, confVal);
      }
    }

    if (p != null)
    {
      oldPropValues = new HashMap<>();

      oldPropValues.put(DataProviderFactory.PREFERRED_ADHOC_PROPERTYNAME,
          p.getProperty(DataProviderFactory.PREFERRED_ADHOC_PROPERTYNAME, null));
      oldPropValues.put(DataProviderFactory.PREFERRED_INGEST_PROPERTYNAME,
          p.getProperty(DataProviderFactory.PREFERRED_INGEST_PROPERTYNAME, null));
      oldPropValues.put(DataProviderFactory.PREFERRED_MRSIMAGE_PROPERTYNAME,
          p.getProperty(DataProviderFactory.PREFERRED_MRSIMAGE_PROPERTYNAME, null));
      oldPropValues.put(DataProviderFactory.PREFERRED_VECTOR_PROPERTYNAME,
          p.getProperty(DataProviderFactory.PREFERRED_VECTOR_PROPERTYNAME, null));
      oldPropValues.put(DataProviderFactory.PREFERRED_PROPERTYNAME,
          p.getProperty(DataProviderFactory.PREFERRED_PROPERTYNAME, null));

      if (pVal == null)
      {
        p.remove(DataProviderFactory.PREFERRED_ADHOC_PROPERTYNAME);
        p.remove(DataProviderFactory.PREFERRED_INGEST_PROPERTYNAME);
        p.remove(DataProviderFactory.PREFERRED_MRSIMAGE_PROPERTYNAME);
        p.remove(DataProviderFactory.PREFERRED_VECTOR_PROPERTYNAME);
      }
      else
      {
        p.setProperty(DataProviderFactory.PREFERRED_ADHOC_PROPERTYNAME, pVal);
        p.setProperty(DataProviderFactory.PREFERRED_INGEST_PROPERTYNAME, pVal);
        p.setProperty(DataProviderFactory.PREFERRED_MRSIMAGE_PROPERTYNAME, pVal);
        p.setProperty(DataProviderFactory.PREFERRED_VECTOR_PROPERTYNAME, pVal);
      }

      if (defVal == null)
      {
        p.remove(DataProviderFactory.PREFERRED_PROPERTYNAME);
      }
      else
      {
        p.setProperty(DataProviderFactory.PREFERRED_PROPERTYNAME, defVal);
      }
    }

    Properties mp = MrGeoProperties.getInstance();

    oldMrGeoValues = new HashMap<>();

    oldMrGeoValues.put(DataProviderFactory.PREFERRED_ADHOC_PROPERTYNAME,
        mp.getProperty(DataProviderFactory.PREFERRED_ADHOC_PROPERTYNAME, null));
    oldMrGeoValues.put(DataProviderFactory.PREFERRED_INGEST_PROPERTYNAME,
        mp.getProperty(DataProviderFactory.PREFERRED_INGEST_PROPERTYNAME, null));
    oldMrGeoValues.put(DataProviderFactory.PREFERRED_MRSIMAGE_PROPERTYNAME,
        mp.getProperty(DataProviderFactory.PREFERRED_MRSIMAGE_PROPERTYNAME, null));
    oldMrGeoValues.put(DataProviderFactory.PREFERRED_VECTOR_PROPERTYNAME,
        mp.getProperty(DataProviderFactory.PREFERRED_VECTOR_PROPERTYNAME, null));
    oldMrGeoValues.put(DataProviderFactory.PREFERRED_PROPERTYNAME,
        mp.getProperty(DataProviderFactory.PREFERRED_PROPERTYNAME, null));

    if (mrgeoVal == null)
    {
      mp.remove(DataProviderFactory.PREFERRED_ADHOC_PROPERTYNAME);
      mp.remove(DataProviderFactory.PREFERRED_INGEST_PROPERTYNAME);
      mp.remove(DataProviderFactory.PREFERRED_MRSIMAGE_PROPERTYNAME);
      mp.remove(DataProviderFactory.PREFERRED_VECTOR_PROPERTYNAME);
    }
    else
    {
      mp.setProperty(DataProviderFactory.PREFERRED_ADHOC_PROPERTYNAME, mrgeoVal);
      mp.setProperty(DataProviderFactory.PREFERRED_INGEST_PROPERTYNAME, mrgeoVal);
      mp.setProperty(DataProviderFactory.PREFERRED_MRSIMAGE_PROPERTYNAME, mrgeoVal);
      mp.setProperty(DataProviderFactory.PREFERRED_VECTOR_PROPERTYNAME, mrgeoVal);
    }

    if (defMrgeoVal == null)
    {
      mp.remove(DataProviderFactory.PREFERRED_PROPERTYNAME);
    }
    else
    {
      mp.setProperty(DataProviderFactory.PREFERRED_PROPERTYNAME, defMrgeoVal);
    }

  }

  private void teardownPreferred(Configuration conf, Properties p)
  {
    if (conf != null && oldConfValues != null)
    {
      for (Map.Entry<String, String> val : oldConfValues.entrySet())
      {
        if (val.getValue() == null)
        {
          conf.unset(val.getKey());
        }
        else
        {
          conf.set(val.getKey(), val.getValue());
        }
      }
    }

    if (p != null && oldPropValues != null)
    {
      for (Map.Entry<String, String> val : oldPropValues.entrySet())
      {
        if (val.getValue() == null)
        {
          p.remove(val.getKey());
        }
        else
        {
          p.setProperty(val.getKey(), val.getValue());
        }
      }
    }

    if (oldMrGeoValues != null)
    {
      Properties mp = MrGeoProperties.getInstance();
      for (Map.Entry<String, String> val : oldMrGeoValues.entrySet())
      {
        if (val.getValue() == null)
        {
          mp.remove(val.getKey());
        }
        else
        {
          mp.setProperty(val.getKey(), val.getValue());
        }
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testPreferredProviderFromConf1() throws Exception
  {
    String good = "good";
    String bad = "bad";

    setupPreferred(conf, providerProperties, good, null, null, null, null);
    DataProviderFactory.initialize(conf, providerProperties);

    Assert.assertEquals("Bad adhoc preferred provider!", good, DataProviderFactory.preferredAdHocProviderName);
    Assert.assertEquals("Bad image preferred provider!", good, DataProviderFactory.preferredImageProviderName);
    Assert.assertEquals("Bad ingest preferred provider!", good, DataProviderFactory.preferredIngestProviderName);
    Assert.assertEquals("Bad vector preferred provider!", good, DataProviderFactory.preferredVectorProviderName);

    teardownPreferred(conf, providerProperties);
  }

  @Test
  @Category(UnitTest.class)
  public void testPreferredProviderFromConf2() throws Exception
  {

    String good = "good";
    String bad = "bad";

    setupPreferred(conf, providerProperties, good, bad, null, null, null);
    DataProviderFactory.initialize(conf, providerProperties);

    Assert.assertEquals("Bad adhoc preferred provider!", good, DataProviderFactory.preferredAdHocProviderName);
    Assert.assertEquals("Bad image preferred provider!", good, DataProviderFactory.preferredImageProviderName);
    Assert.assertEquals("Bad ingest preferred provider!", good, DataProviderFactory.preferredIngestProviderName);
    Assert.assertEquals("Bad vector preferred provider!", good, DataProviderFactory.preferredVectorProviderName);

    teardownPreferred(conf, providerProperties);
  }

  @Test
  @Category(UnitTest.class)
  public void testPreferredProviderFromConf3() throws Exception
  {

    String good = "good";
    String bad = "bad";

    setupPreferred(conf, providerProperties, good, null, bad, null, null);
    DataProviderFactory.initialize(conf, providerProperties);

    Assert.assertEquals("Bad adhoc preferred provider!", good, DataProviderFactory.preferredAdHocProviderName);
    Assert.assertEquals("Bad image preferred provider!", good, DataProviderFactory.preferredImageProviderName);
    Assert.assertEquals("Bad ingest preferred provider!", good, DataProviderFactory.preferredIngestProviderName);
    Assert.assertEquals("Bad vector preferred provider!", good, DataProviderFactory.preferredVectorProviderName);

    teardownPreferred(conf, providerProperties);
  }

  @Test
  @Category(UnitTest.class)
  public void testPreferredProviderFromConf4() throws Exception
  {
    String good = "good";
    String bad = "bad";

    setupPreferred(conf, providerProperties, good, null, null, bad, null);
    DataProviderFactory.initialize(conf, providerProperties);

    Assert.assertEquals("Bad adhoc preferred provider!", good, DataProviderFactory.preferredAdHocProviderName);
    Assert.assertEquals("Bad image preferred provider!", good, DataProviderFactory.preferredImageProviderName);
    Assert.assertEquals("Bad ingest preferred provider!", good, DataProviderFactory.preferredIngestProviderName);
    Assert.assertEquals("Bad vector preferred provider!", good, DataProviderFactory.preferredVectorProviderName);

    teardownPreferred(conf, providerProperties);
  }

  @Test
  @Category(UnitTest.class)
  public void testPreferredProviderFromConf5() throws Exception
  {
    String good = "good";
    String bad = "bad";

    setupPreferred(conf, providerProperties, good, null, null, null, bad);
    DataProviderFactory.initialize(conf, providerProperties);

    Assert.assertEquals("Bad adhoc preferred provider!", good, DataProviderFactory.preferredAdHocProviderName);
    Assert.assertEquals("Bad image preferred provider!", good, DataProviderFactory.preferredImageProviderName);
    Assert.assertEquals("Bad ingest preferred provider!", good, DataProviderFactory.preferredIngestProviderName);
    Assert.assertEquals("Bad vector preferred provider!", good, DataProviderFactory.preferredVectorProviderName);

    teardownPreferred(conf, providerProperties);
  }

  @Test
  @Category(UnitTest.class)
  public void testPreferredProviderFromProp() throws Exception
  {
    String good = "good";
    String bad = "bad";

    setupPreferred(conf, providerProperties, null, good, bad, bad, bad);
    DataProviderFactory.initialize(conf, providerProperties);

    Assert.assertEquals("Bad adhoc preferred provider!", good, DataProviderFactory.preferredAdHocProviderName);
    Assert.assertEquals("Bad image preferred provider!", good, DataProviderFactory.preferredImageProviderName);
    Assert.assertEquals("Bad ingest preferred provider!", good, DataProviderFactory.preferredIngestProviderName);
    Assert.assertEquals("Bad vector preferred provider!", good, DataProviderFactory.preferredVectorProviderName);

    teardownPreferred(conf, providerProperties);
  }

  @Test
  @Category(UnitTest.class)
  public void testPreferredProviderFromDefProp() throws Exception
  {

    String good = "good";

    setupPreferred(conf, providerProperties, null, null, good, null, null);
    DataProviderFactory.initialize(conf, providerProperties);

    Assert.assertEquals("Bad adhoc preferred provider!", good, DataProviderFactory.preferredAdHocProviderName);
    Assert.assertEquals("Bad image preferred provider!", good, DataProviderFactory.preferredImageProviderName);
    Assert.assertEquals("Bad ingest preferred provider!", good, DataProviderFactory.preferredIngestProviderName);
    Assert.assertEquals("Bad vector preferred provider!", good, DataProviderFactory.preferredVectorProviderName);

    teardownPreferred(conf, providerProperties);
  }
  @Test
  @Category(UnitTest.class)
  public void testPreferredProviderFromMrGeoProp() throws Exception
  {
    String good = "good";

    setupPreferred(conf, providerProperties, null, null, null, good, null);
    DataProviderFactory.initialize(conf, providerProperties);

    Assert.assertEquals("Bad adhoc preferred provider!", good, DataProviderFactory.preferredAdHocProviderName);
    Assert.assertEquals("Bad image preferred provider!", good, DataProviderFactory.preferredImageProviderName);
    Assert.assertEquals("Bad ingest preferred provider!", good, DataProviderFactory.preferredIngestProviderName);
    Assert.assertEquals("Bad vector preferred provider!", good, DataProviderFactory.preferredVectorProviderName);

    teardownPreferred(conf, providerProperties);
  }

  @Test
  @Category(UnitTest.class)
  public void testPreferredProviderFromDefMrGeoProp() throws Exception
  {
    String good = "good";

    setupPreferred(conf, providerProperties, null, null, null, null, good);
    DataProviderFactory.initialize(conf, providerProperties);

    Assert.assertEquals("Bad adhoc preferred provider!", good, DataProviderFactory.preferredAdHocProviderName);
    Assert.assertEquals("Bad image preferred provider!", good, DataProviderFactory.preferredImageProviderName);
    Assert.assertEquals("Bad ingest preferred provider!", good, DataProviderFactory.preferredIngestProviderName);
    Assert.assertEquals("Bad vector preferred provider!", good, DataProviderFactory.preferredVectorProviderName);

    teardownPreferred(conf, providerProperties);
  }

  @Test
  @Category(UnitTest.class)
  public void testPreferredProviderFromDefault() throws Exception
  {
    String hdfs = "hdfs";

    setupPreferred(conf, providerProperties, null, null, null, null, null);
    DataProviderFactory.initialize(conf, providerProperties);

    Assert.assertEquals("Bad adhoc preferred provider!", hdfs, DataProviderFactory.preferredAdHocProviderName);
    Assert.assertEquals("Bad image preferred provider!", hdfs, DataProviderFactory.preferredImageProviderName);
    Assert.assertEquals("Bad ingest preferred provider!", hdfs, DataProviderFactory.preferredIngestProviderName);
    Assert.assertEquals("Bad vector preferred provider!", hdfs, DataProviderFactory.preferredVectorProviderName);

    teardownPreferred(conf, providerProperties);
  }

}
