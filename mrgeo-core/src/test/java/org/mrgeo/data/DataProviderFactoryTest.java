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

package org.mrgeo.data;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import org.mrgeo.data.vector.VectorDataProvider;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.test.LocalRunnerTest;
import org.mrgeo.utils.HadoopUtils;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

@SuppressWarnings("all") // test code, not included in production
public class DataProviderFactoryTest extends LocalRunnerTest
{
private static String all_ones;
private static String all_ones_input = Defs.INPUT + "all-ones";
private static String test_tsv;
private static String test_tsv_filename = "test1.tsv";
private static String test_tsv_columns_filename = test_tsv_filename + ".columns";
private static String test_tsv_input = Defs.INPUT + test_tsv_filename;
//Map<String, String> oldConfValues = null;
//Map<String, String> oldPropValues = null;
//Map<String, String> oldMrGeoValues = null;
private ProviderProperties providerProperties;

@BeforeClass
static public void setup() throws IOException
{
  File f = new File(all_ones_input);
  all_ones = "file://" + f.getCanonicalPath();
  File f1 = new File(test_tsv_input);
  test_tsv = "file://" + f1.getCanonicalPath();
}

@Before
public void init() throws DataProviderException
{
  providerProperties = new ProviderProperties();
  DataProviderFactory.initialize(HadoopUtils.createConfiguration());
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
    catch (DataProviderNotFound e)
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
  catch (DataProviderNotFound e)
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
    catch (DataProviderNotFound e)
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
    catch (DataProviderNotFound e)
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
    catch (DataProviderNotFound e)
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
    catch (DataProviderNotFound e)
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
  catch (DataProviderNotFound e)
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

@Test
@Category(UnitTest.class)
public void testPreferredProviderFromConf1() throws Exception
{
  String good = "good";
  String bad = "bad";

  setupPreferred(conf, good, null, null);
  DataProviderFactory.initialize(conf);

  Assert.assertEquals("Bad adhoc preferred provider!", good, DataProviderFactory.preferredAdHocProviderName);
  Assert.assertEquals("Bad image preferred provider!", good, DataProviderFactory.preferredImageProviderName);
  Assert.assertEquals("Bad vector preferred provider!", good, DataProviderFactory.preferredVectorProviderName);

  teardownPreferred(conf);
}

@Test
@Category(UnitTest.class)
public void testPreferredProviderFromConf4() throws Exception
{
  String good = "good";
  String bad = "bad";

  setupPreferred(conf, good, bad, null);
  DataProviderFactory.initialize(conf);

  Assert.assertEquals("Bad adhoc preferred provider!", good, DataProviderFactory.preferredAdHocProviderName);
  Assert.assertEquals("Bad image preferred provider!", good, DataProviderFactory.preferredImageProviderName);
  Assert.assertEquals("Bad vector preferred provider!", good, DataProviderFactory.preferredVectorProviderName);

  teardownPreferred(conf);
}

@Test
@Category(UnitTest.class)
public void testPreferredProviderFromConf5() throws Exception
{
  String good = "good";
  String bad = "bad";

  setupPreferred(conf, good, null, bad);
  DataProviderFactory.initialize(conf);

  Assert.assertEquals("Bad adhoc preferred provider!", good, DataProviderFactory.preferredAdHocProviderName);
  Assert.assertEquals("Bad image preferred provider!", good, DataProviderFactory.preferredImageProviderName);
  Assert.assertEquals("Bad vector preferred provider!", good, DataProviderFactory.preferredVectorProviderName);

  teardownPreferred(conf);
}

@Test
@Category(UnitTest.class)
public void testPreferredProviderFromMrGeoProp() throws Exception
{
  String good = "good";

  setupPreferred(conf, null, good, null);
  DataProviderFactory.initialize(conf);

  Assert.assertEquals("Bad adhoc preferred provider!", good, DataProviderFactory.preferredAdHocProviderName);
  Assert.assertEquals("Bad image preferred provider!", good, DataProviderFactory.preferredImageProviderName);
  Assert.assertEquals("Bad vector preferred provider!", good, DataProviderFactory.preferredVectorProviderName);

  teardownPreferred(conf);
}

@Test
@Category(UnitTest.class)
public void testPreferredProviderFromDefMrGeoProp() throws Exception
{
  String good = "good";

  setupPreferred(conf, null, null, good);
  DataProviderFactory.initialize(conf);

  Assert.assertEquals("Bad adhoc preferred provider!", good, DataProviderFactory.preferredAdHocProviderName);
  Assert.assertEquals("Bad image preferred provider!", good, DataProviderFactory.preferredImageProviderName);
  Assert.assertEquals("Bad vector preferred provider!", good, DataProviderFactory.preferredVectorProviderName);

  teardownPreferred(conf);
}

@Test
@Category(UnitTest.class)
public void testPreferredProviderFromDefault() throws Exception
{
  String hdfs = "hdfs";

  setupPreferred(conf, null, null, null);
  DataProviderFactory.initialize(conf);

  Assert.assertEquals("Bad adhoc preferred provider!", hdfs, DataProviderFactory.preferredAdHocProviderName);
  Assert.assertEquals("Bad image preferred provider!", hdfs, DataProviderFactory.preferredImageProviderName);
  Assert.assertEquals("Bad vector preferred provider!", hdfs, DataProviderFactory.preferredVectorProviderName);

  teardownPreferred(conf);
}

private void setupPreferred(Configuration conf, String confVal,
    String mrgeoVal, String defMrgeoVal)
{
  MrGeoProperties.resetProperties();
  if (conf != null)
  {
//    oldConfValues = new HashMap<>();
//
//    oldConfValues.put(DataProviderFactory.PREFERRED_ADHOC_PROVIDER_NAME,
//        conf.get(DataProviderFactory.PREFERRED_ADHOC_PROVIDER_NAME, null));
//    oldConfValues.put(DataProviderFactory.PREFERRED_MRSIMAGE_PROVIDER_NAME,
//        conf.get(DataProviderFactory.PREFERRED_MRSIMAGE_PROVIDER_NAME, null));
//    oldConfValues.put(DataProviderFactory.PREFERRED_VECTOR_PROVIDER_NAME,
//        conf.get(DataProviderFactory.PREFERRED_VECTOR_PROVIDER_NAME, null));

    if (confVal == null)
    {
      conf.unset(DataProviderFactory.PREFERRED_ADHOC_PROVIDER_NAME);
      conf.unset(DataProviderFactory.PREFERRED_MRSIMAGE_PROVIDER_NAME);
      conf.unset(DataProviderFactory.PREFERRED_VECTOR_PROVIDER_NAME);
    }
    else
    {
      conf.set(DataProviderFactory.PREFERRED_ADHOC_PROVIDER_NAME, confVal);
      conf.set(DataProviderFactory.PREFERRED_MRSIMAGE_PROVIDER_NAME, confVal);
      conf.set(DataProviderFactory.PREFERRED_VECTOR_PROVIDER_NAME, confVal);
    }
  }

  Properties mp = MrGeoProperties.getInstance();

//  oldMrGeoValues = new HashMap<>();
//
//  oldMrGeoValues.put(DataProviderFactory.PREFERRED_ADHOC_PROPERTYNAME,
//      mp.getProperty(DataProviderFactory.PREFERRED_ADHOC_PROPERTYNAME, null));
//  oldMrGeoValues.put(DataProviderFactory.PREFERRED_MRSIMAGE_PROPERTYNAME,
//      mp.getProperty(DataProviderFactory.PREFERRED_MRSIMAGE_PROPERTYNAME, null));
//  oldMrGeoValues.put(DataProviderFactory.PREFERRED_VECTOR_PROPERTYNAME,
//      mp.getProperty(DataProviderFactory.PREFERRED_VECTOR_PROPERTYNAME, null));
//  oldMrGeoValues.put(DataProviderFactory.PREFERRED_PROPERTYNAME,
//      mp.getProperty(DataProviderFactory.PREFERRED_PROPERTYNAME, null));

  if (mrgeoVal == null)
  {
    mp.remove(DataProviderFactory.PREFERRED_ADHOC_PROPERTYNAME);
    mp.remove(DataProviderFactory.PREFERRED_MRSIMAGE_PROPERTYNAME);
    mp.remove(DataProviderFactory.PREFERRED_VECTOR_PROPERTYNAME);
  }
  else
  {
    mp.setProperty(DataProviderFactory.PREFERRED_ADHOC_PROPERTYNAME, mrgeoVal);
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

private void teardownPreferred(Configuration conf)
{
  MrGeoProperties.resetProperties();
//  if (conf != null && oldConfValues != null)
//  {
//    for (Map.Entry<String, String> val : oldConfValues.entrySet())
//    {
//      if (val.getValue() == null)
//      {
//        conf.unset(val.getKey());
//      }
//      else
//      {
//        conf.set(val.getKey(), val.getValue());
//      }
//    }
//  }
//
//  if (oldMrGeoValues != null)
//  {
//    Properties mp = MrGeoProperties.getInstance();
//    for (Map.Entry<String, String> val : oldMrGeoValues.entrySet())
//    {
//      if (val.getValue() == null)
//      {
//        mp.remove(val.getKey());
//      }
//      else
//      {
//        mp.setProperty(val.getKey(), val.getValue());
//      }
//    }
//  }
}

}
