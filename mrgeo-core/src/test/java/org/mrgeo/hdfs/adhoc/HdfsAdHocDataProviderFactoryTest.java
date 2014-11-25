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

package org.mrgeo.hdfs.adhoc;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.data.adhoc.AdHocDataProvider;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.utils.HadoopUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;

public class HdfsAdHocDataProviderFactoryTest
{
  HdfsAdHocDataProviderFactory factory;
  AdHocDataProvider provider = null;
  Configuration conf;

  @Before
  public void setup()
  {
    conf = HadoopUtils.createConfiguration();
    factory = new HdfsAdHocDataProviderFactory();
  }

  @After
  public void teardown() throws IOException
  {
    if (provider != null)
    {
      provider.delete();
    }
  }


  @Test
  @Category(UnitTest.class)
  public void testGetPrefix() throws Exception
  {
    Assert.assertEquals("Prefix is wrong!", "hdfs", factory.getPrefix());
  }

  @Test
  @Category(UnitTest.class)
  public void testCreateAdHocDataProvider1() throws Exception
  {
    provider = factory.createAdHocDataProvider(conf);
    Assert.assertEquals("Created wrong provider type", HdfsAdHocDataProvider.class, provider.getClass());

  }

  @Test
  @Category(UnitTest.class)
  public void testCreateAdHocDataProvider2() throws Exception
  {
    provider = factory.createAdHocDataProvider((Properties) null);
    Assert.assertEquals("Created wrong provider type", HdfsAdHocDataProvider.class, provider.getClass());

  }

  @Test
  @Category(UnitTest.class)
  public void testCreateAdHocDataProviderName1() throws Exception
  {
    String name = "foo";
    provider = factory.createAdHocDataProvider(name, conf);
    Assert.assertEquals("Created wrong provider type", HdfsAdHocDataProvider.class, provider.getClass());

    Path path = new Path(provider.getResourceName());
    Assert.assertEquals("Created wrong name!", name, path.getName());
  }

  @Test
  @Category(UnitTest.class)
  public void testCreateAdHocDataProviderName2() throws Exception
  {
    String name = "foo";
    provider = factory.createAdHocDataProvider(name, (Properties)null);
    Assert.assertEquals("Created wrong provider type", HdfsAdHocDataProvider.class, provider.getClass());

    Path path = new Path(provider.getResourceName());
    Assert.assertEquals("Created wrong name!", name, path.getName());
  }

  @Test
  @Category(UnitTest.class)
  public void testCanOpen() throws Exception
  {
    provider = factory.createAdHocDataProvider(conf);
    String name = provider.getResourceName();

    OutputStream outputStream = provider.add();
    outputStream.close();

    Assert.assertTrue("Should be able to open!", factory.canOpen(name, conf));
  }

  @Test
  @Category(UnitTest.class)
  public void testCanOpenNothingWritten() throws Exception
  {
    provider = factory.createAdHocDataProvider(conf);
    String name = provider.getResourceName();

    Assert.assertFalse("Should not be able to open!", factory.canOpen(name, conf));
  }

  @Test
  @Category(UnitTest.class)
  public void testCanOpenMissing() throws Exception
  {
    Assert.assertFalse("Should not be able to open!", factory.canOpen("foo", conf));
  }

  @Test
  @Category(UnitTest.class)
  public void testCanWrite() throws Exception
  {
    // always returns true for now
    Assert.assertTrue("Should be able to write!", factory.canWrite("foo", conf));
  }

  @Test
  @Category(UnitTest.class)
  public void testExists() throws Exception
  {
    provider = factory.createAdHocDataProvider(conf);
    // need to make sure there is something added
    OutputStream stream = provider.add();
    stream.close();

    Assert.assertTrue("Name should exist!", factory.exists(provider.getResourceName(), conf));
  }

  @Test
  @Category(UnitTest.class)
  public void testExistsNothingWritten() throws Exception
  {
    provider = factory.createAdHocDataProvider(conf);

    Assert.assertFalse("Name should not exist!", factory.exists(provider.getResourceName(), conf));
  }

  @Test
  @Category(UnitTest.class)
  public void testExistsMissing() throws Exception
  {
    Assert.assertFalse("Name should not exist!", factory.exists("foo", conf));
  }

  @Test
  @Category(UnitTest.class)
  public void testDeleteNamed() throws Exception
  {
    provider = factory.createAdHocDataProvider(conf);
    Assert.assertFalse("Directory should not exist", HadoopFileUtils.exists(provider.getResourceName()));

    // need to make sure there is something added, so the directories are actually created...
    OutputStream stream = provider.add();
    stream.close();

    Assert.assertTrue("Directory should  exist", HadoopFileUtils.exists(provider.getResourceName()));

    factory.delete(provider.getResourceName(), conf);

    Assert.assertFalse("Directory should not exist", HadoopFileUtils.exists(provider.getResourceName()));
  }

  @Test
  @Category(UnitTest.class)
  public void testDeleteNamedNoData() throws Exception
  {
    provider = factory.createAdHocDataProvider(conf);
    Assert.assertFalse("Directory should not exist", HadoopFileUtils.exists(provider.getResourceName()));

    factory.delete(provider.getResourceName(), conf);

    Assert.assertFalse("Directory should not exist", HadoopFileUtils.exists(provider.getResourceName()));

  }

  @Test
  @Category(UnitTest.class)
  public void testDeleteNamedMissing() throws Exception
  {
    String name = "foo";

    Assert.assertFalse("Directory should not exist", HadoopFileUtils.exists(name));

    factory.delete(name, conf);

    Assert.assertFalse("Directory should not exist", HadoopFileUtils.exists(name));

  }
}