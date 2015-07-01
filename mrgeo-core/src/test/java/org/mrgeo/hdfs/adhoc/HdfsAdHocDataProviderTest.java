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

package org.mrgeo.hdfs.adhoc;

import junit.framework.Assert;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.data.DataProviderException;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.test.LocalRunnerTest;
import org.mrgeo.utils.HadoopUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Properties;

public class HdfsAdHocDataProviderTest extends LocalRunnerTest
{
  private static Properties providerProperties = null;
  HdfsAdHocDataProvider provider = null;

  @Before
  public void setup() throws IOException
  {
    provider = new HdfsAdHocDataProvider(getConfiguration(), providerProperties);
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
  public void testSetupJob() throws Exception
  {
    Job job = new Job(conf, getClass().getSimpleName());
    provider.setupJob(job);

    Assert.assertTrue("Adhoc directory not created!", HdfsAdHocDataProvider.exists(conf,
        provider.getResourceName(), providerProperties));
  }

  @Test(expected = DataProviderException.class)
  @Category(UnitTest.class)
  public void testSetupJobBadName() throws Exception
  {
    Job job = new Job(conf, getClass().getSimpleName());

    HdfsAdHocDataProvider badprovider = new HdfsAdHocDataProvider(conf, "abcd:bad-name",
        providerProperties);
    badprovider.setupJob(job);
  }

  @Test
  @Category(UnitTest.class)
  public void testAdd() throws Exception
  {
    Assert.assertEquals("Adhoc should be empty!", 0, provider.size());
    OutputStream stream = provider.add();

    Assert.assertNotNull("Adhoc stream is null!", stream);

    Assert.assertEquals("Adhoc size should be 1!", 1, provider.size());
    stream.close();
  }

  @Test
  @Category(UnitTest.class)
  public void testAddNamed() throws Exception
  {
    String name = "foo";
    Assert.assertEquals("Adhoc should be empty!", 0, provider.size());
    OutputStream stream = provider.add(name);
    Assert.assertNotNull("Adhoc stream is null!", stream);
    Assert.assertEquals("Adhoc size should be 1!", 1, provider.size());

    Path path = new Path(provider.getName(0));
    Assert.assertEquals("Names are not the same!", name, path.getName());

    stream.close();
  }


  @Test(expected = IllegalArgumentException.class)
  @Category(UnitTest.class)
  public void testAddBadName() throws Exception
  {
    String name = "foo:-)";
    Assert.assertEquals("Adhoc should be empty!", 0, provider.size());
    provider.add(name);
  }

  @Test
  @Category(UnitTest.class)
  public void testDelete() throws Exception
  {
    // need to make sure there is something added, so the directories are actually created...
    OutputStream stream = provider.add();
    stream.close();

    provider.delete();

    Assert.assertFalse("Directory shouold not exist", HadoopFileUtils.exists(provider.getResourcePath()));
  }

  @Test
  @Category(UnitTest.class)
  public void testDeleteThanAdd() throws Exception
  {
    provider.delete();

    // this should work...
    OutputStream stream = provider.add();
    stream.close();

  }

  @Test
  @Category(UnitTest.class)
  public void testMove() throws Exception
  {
    String newname = new Path(HadoopFileUtils.getTempDir(), HadoopUtils.createRandomString(10)).toString();

    // need to make sure there is something added, so the directories are actually created...
    OutputStream stream = provider.add();
    stream.close();

    String oldname = provider.getResourceName();

    provider.move(newname);

    Assert.assertTrue("New name should exist!", HdfsAdHocDataProvider.exists(conf, newname,
        providerProperties));
    Assert.assertFalse("Old name should not exist!", HdfsAdHocDataProvider.exists(conf, oldname,
        providerProperties));
  }

  @Test(expected = IOException.class)
  @Category(UnitTest.class)
  public void testMoveNoFiles() throws Exception
  {
    String newname = new Path(HadoopFileUtils.getTempDir(), HadoopUtils.createRandomString(10)).toString();

    provider.getResourceName();

    provider.move(newname);
  }

  @Test
  @Category(UnitTest.class)
  public void testGet() throws Exception
  {
    String text = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed ut ullamcorper est. Phasellus sed nisl arcu. Nullam non sem quam. Pellentesque vel neque quis erat ultricies dapibus a ac mauris. Morbi sodales vitae nisl ullamcorper facilisis. Quisque pellentesque est in fermentum luctus. Etiam sed nibh id dui faucibus luctus vel sit amet odio. Fusce non sollicitudin ligula, ac fermentum leo. Nulla facilisi.";
    OutputStream outputStream = provider.add();
    PrintStream out = new PrintStream(outputStream);
    out.print(text);

    outputStream.close();

    Assert.assertEquals("Adhoc size should be 1!", 1, provider.size());

    InputStream instream = provider.get(0);
    String intext = IOUtils.toString(instream);
    instream.close();

    Assert.assertEquals("Input string is not equal to output string", text, intext);
  }

  @Test(expected = IOException.class)
  @Category(UnitTest.class)
  public void testGetBadIndex() throws Exception
  {
    OutputStream outputStream = provider.add();
    outputStream.close();

    Assert.assertEquals("Adhoc size should be 1!", 1, provider.size());
    provider.get(10);
  }

  @Test(expected = IOException.class)
  @Category(UnitTest.class)
  public void testGetNegativeIndex() throws Exception
  {
    OutputStream outputStream = provider.add();
    outputStream.close();

    Assert.assertEquals("Adhoc size should be 1!", 1, provider.size());
    provider.get(-1);
  }

  @Test
  @Category(UnitTest.class)
  public void testGetByName() throws Exception
  {
    String name = "foo";
    String text = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed ut ullamcorper est. Phasellus sed nisl arcu. Nullam non sem quam. Pellentesque vel neque quis erat ultricies dapibus a ac mauris. Morbi sodales vitae nisl ullamcorper facilisis. Quisque pellentesque est in fermentum luctus. Etiam sed nibh id dui faucibus luctus vel sit amet odio. Fusce non sollicitudin ligula, ac fermentum leo. Nulla facilisi.";
    OutputStream outputStream = provider.add(name);
    PrintStream out = new PrintStream(outputStream);
    out.print(text);

    outputStream.close();

    Assert.assertEquals("Adhoc size should be 1!", 1, provider.size());

    InputStream instream = provider.get(name);
    String intext = IOUtils.toString(instream);
    instream.close();

    Assert.assertEquals("Input string is not equal to output string", text, intext);
  }

  @Test(expected = IOException.class)
  @Category(UnitTest.class)
  public void testGetByNameBadName() throws Exception
  {
    String name = "foo";

    OutputStream outputStream = provider.add(name);
    outputStream.close();

    Assert.assertEquals("Adhoc size should be 1!", 1, provider.size());

    provider.get("bad-name");
  }

  @Test
  @Category(UnitTest.class)
  public void testGetName() throws Exception
  {
    String name = "foo";

    OutputStream outputStream = provider.add(name);
    outputStream.close();

    Assert.assertEquals("Adhoc size should be 1!", 1, provider.size());

    Path pname = new Path(provider.getName(0));
    Assert.assertEquals("Names are different!", name, pname.getName());

  }

  @Test(expected = IOException.class)
  @Category(UnitTest.class)
  public void testGetNameBadIndex() throws Exception
  {
    String name = "foo";

    OutputStream outputStream = provider.add(name);
    outputStream.close();

    Assert.assertEquals("Adhoc size should be 1!", 1, provider.size());

    provider.getName(10);
  }

  @Test(expected = IOException.class)
  @Category(UnitTest.class)
  public void testGetNameNegativeIndex() throws Exception
  {
    String name = "foo";

    OutputStream outputStream = provider.add(name);
    outputStream.close();

    Assert.assertEquals("Adhoc size should be 1!", 1, provider.size());

    provider.getName(-1);
  }

  @Test
  @Category(UnitTest.class)
  public void testGetResourcePath() throws Exception
  {
    Path pathFromName = new Path(provider.getResourceName());
    Path path = provider.getResourcePath();

    Assert.assertEquals("Paths are different!", pathFromName.toString(), path.toString());
  }

  @Test
  @Category(UnitTest.class)
  public void testSize() throws Exception
  {
    Assert.assertEquals("Adhoc size should be 0!", 0, provider.size());

    for (int i = 1; i < 10; i++)
    {
      OutputStream outputStream = provider.add();
      outputStream.close();
      Assert.assertEquals("Adhoc size should be " + i + "!", i, provider.size());
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testCanOpen() throws Exception
  {
    String name = provider.getResourceName();

    OutputStream outputStream = provider.add();
    outputStream.close();

    Assert.assertTrue("Should be able to open!", HdfsAdHocDataProvider.canOpen(conf, name,
        providerProperties));
  }

  @Test
  @Category(UnitTest.class)
  public void testCanOpenNothingWritten() throws Exception
  {
    String name = provider.getResourceName();

    Assert.assertFalse("Should not be able to open!", HdfsAdHocDataProvider.canOpen(conf, name,
        providerProperties));
  }

  @Test
  @Category(UnitTest.class)
  public void testCanOpenMissing() throws Exception
  {
    Assert.assertFalse("Should not be able to open!", HdfsAdHocDataProvider.canOpen(conf, "foo",
        providerProperties));
  }

  @Test
  @Category(UnitTest.class)
  public void testCanWrite() throws Exception
  {
    // always returns true for now
    Assert.assertTrue("Should be able to write!", HdfsAdHocDataProvider.canWrite("foo", conf,
        providerProperties));
  }

  @Test
  @Category(UnitTest.class)
  public void testExists() throws Exception
  {
    // need to make sure there is something added
    OutputStream stream = provider.add();
    stream.close();

    Assert.assertTrue("Name should exist!", HdfsAdHocDataProvider.exists(conf, provider.getResourceName(),
        providerProperties));
  }

  @Test
  @Category(UnitTest.class)
  public void testExistsNothingWritten() throws Exception
  {
    Assert.assertFalse("Name should not exist!", HdfsAdHocDataProvider.exists(conf, provider.getResourceName(),
        providerProperties));
  }

  @Test
  @Category(UnitTest.class)
  public void testExistsMissing() throws Exception
  {
    Assert.assertFalse("Name should not exist!", HdfsAdHocDataProvider.exists(conf, "foo",
        providerProperties));
  }

  @Test
  @Category(UnitTest.class)
  public void testDeleteNamed() throws Exception
  {
    Assert.assertFalse("Directory should not exist", HadoopFileUtils.exists(provider.getResourcePath()));

    // need to make sure there is something added, so the directories are actually created...
    OutputStream stream = provider.add();
    stream.close();

    Assert.assertTrue("Directory should  exist", HadoopFileUtils.exists(provider.getResourcePath()));

    HdfsAdHocDataProvider.delete(conf, provider.getResourceName(), providerProperties);

    Assert.assertFalse("Directory should not exist", HadoopFileUtils.exists(provider.getResourcePath()));
  }

  @Test
  @Category(UnitTest.class)
  public void testDeleteNamedNoData() throws Exception
  {
    Assert.assertFalse("Directory should not exist", HadoopFileUtils.exists(provider.getResourcePath()));

    HdfsAdHocDataProvider.delete(conf, provider.getResourceName(), providerProperties);

    Assert.assertFalse("Directory should not exist", HadoopFileUtils.exists(provider.getResourcePath()));

  }

  @Test
  @Category(UnitTest.class)
  public void testDeleteNamedMissing() throws Exception
  {
    String name = "foo";

    Assert.assertFalse("Directory should not exist", HadoopFileUtils.exists(name));

    HdfsAdHocDataProvider.delete(conf, name, providerProperties);

    Assert.assertFalse("Directory should not exist", HadoopFileUtils.exists(name));

  }
}