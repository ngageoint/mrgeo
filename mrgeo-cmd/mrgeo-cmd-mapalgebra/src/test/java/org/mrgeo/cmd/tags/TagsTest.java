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

package org.mrgeo.cmd.tags;

import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mrgeo.cmd.MrGeo;
import org.mrgeo.core.Defs;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderNotFound;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.image.MrsImage;
import org.mrgeo.image.MrsPyramid;
import org.mrgeo.image.MrsPyramidMetadata;
import org.mrgeo.junit.IntegrationTest;
import org.mrgeo.test.TestUtils;
import org.mrgeo.utils.HadoopUtils;
import org.mrgeo.utils.LongRectangle;
import org.mrgeo.utils.logging.LoggingUtils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.security.Permission;
import java.util.Properties;

@SuppressWarnings("all") // Test code, not included in production
public class TagsTest
{
private final static String allones = "all-ones";
private static String allonesPath;

private static TestUtils testUtils;
@Rule
public TestName testname = new TestName();
private ProviderProperties providerProperties;


protected static class ExitException extends SecurityException
{
  public final int status;
  public ExitException(int status)
  {
    super("System.exit(" + status + ")");
    this.status = status;
  }
}

private static class NoExitSecurityManager extends SecurityManager
{
  @Override
  public void checkPermission(Permission perm)
  {
    // allow anything.
  }
  @Override
  public void checkPermission(Permission perm, Object context)
  {
    // allow anything.
  }
  @Override
  public void checkExit(int status)
  {
    super.checkExit(status);
    throw new ExitException(status);
  }
}


@BeforeClass
public static void init() throws IOException
{
  LoggingUtils.setDefaultLogLevel(LoggingUtils.ERROR);

  MrGeoProperties.resetProperties();
  testUtils = new TestUtils(TagsTest.class);

}

@AfterClass
public static void finish()
{
  MrGeoProperties.resetProperties();
}

@After
public void teardown()
{
  System.setSecurityManager(null); // or save and restore original
  MrGeoProperties.resetProperties();

  DataProviderFactory.invalidateCache();
}

@Before
public void setUp() throws IOException
{
  // trap System.exit()
  System.setSecurityManager(new NoExitSecurityManager());

  providerProperties = null;

  // need to copy each time...
  allonesPath = "file://" + testUtils.getOutputLocalFor(allones);
  testUtils.copyFileLocal(Defs.INPUT, testUtils.getOutputLocalFor(""), allones);
}

private MrsPyramidMetadata getMetadata() throws IOException
{
  MrsImageDataProvider dp =
      DataProviderFactory.getMrsImageDataProvider(allonesPath, DataProviderFactory.AccessMode.READ,
          providerProperties);

  return dp.getMetadataReader().read();
}

private void setSomeTags(int number) throws IOException
{
  MrsImageDataProvider dp =
      DataProviderFactory.getMrsImageDataProvider(allonesPath, DataProviderFactory.AccessMode.READ,
          providerProperties);

  MrsPyramidMetadata meta = dp.getMetadataReader().read();

  String value;
  if (number > 0)
  {
    meta.setTag("foo", "bar");
  }
  if (number > 1)
  {
    meta.setTag("fa", "wa");
  }
  if (number > 2)
  {
    meta.setTag("golly", "gee");
  }


  dp.getMetadataWriter().write(meta);
}


@Test
@Category(IntegrationTest.class)
public void addOneTag() throws Exception
{
  MrsPyramidMetadata meta = getMetadata();

  String v = meta.getTag("foo", null);
  Assert.assertNull("key already set", v);

  String[] args = {"tags", "-i",  allonesPath, "foo", "bar"};
  try
  {
    MrGeo.main(args);
  }
  catch (ExitException e)
  {
    Assert.assertEquals("Command exited with error", 0, e.status);

    meta = getMetadata();

    v = meta.getTag("foo", null);
    Assert.assertNotNull("key not set", v);
    Assert.assertEquals("bad value", "bar", v);

  }
}

@Test
@Category(IntegrationTest.class)
public void addTwoTags() throws Exception
{
  MrsPyramidMetadata meta = getMetadata();

  String v = meta.getTag("foo", null);
  Assert.assertNull("key already set", v);

  v = meta.getTag("fa", null);
  Assert.assertNull("key already set", v);

  String[] args = {"tags", "-i",  allonesPath, "foo", "bar", "fa", "wa"};
  try
  {
    MrGeo.main(args);
  }
  catch (ExitException e)
  {
    Assert.assertEquals("Command exited with error", 0, e.status);

    meta = getMetadata();

    v = meta.getTag("fa", null);
    Assert.assertNotNull("key not set", v);
    Assert.assertEquals("bad value", "wa", v);

    v = meta.getTag("foo", null);
    Assert.assertNotNull("key not set", v);
    Assert.assertEquals("bad value", "bar", v);

  }
}

@Test
@Category(IntegrationTest.class)
public void overwright() throws Exception
{
  setSomeTags(1);

  MrsPyramidMetadata meta = getMetadata();

  String v = meta.getTag("foo", null);
  Assert.assertNotNull("key already set", v);

  String[] args = {"tags", "-i",  allonesPath, "foo", "choo"};
  try
  {
    MrGeo.main(args);
  }
  catch (ExitException e)
  {
    Assert.assertEquals("Command exited with error", 0, e.status);

    meta = getMetadata();

    v = meta.getTag("foo", null);
    Assert.assertNotNull("key not set", v);
    Assert.assertEquals("bad value", "choo", v);

  }
}

@Test
@Category(IntegrationTest.class)
public void clear() throws Exception
{
  setSomeTags(3);

  MrsPyramidMetadata meta = getMetadata();

  String v = meta.getTag("foo", null);
  Assert.assertNotNull("key not set", v);

  String[] args = {"tags", "-i",  allonesPath, "-c"};
  try
  {
    MrGeo.main(args);
  }
  catch (ExitException e)
  {
    Assert.assertEquals("Command exited with error", 0, e.status);

    meta = getMetadata();

    Assert.assertEquals("tags not cleared", 0, meta.getTags().size());
  }
}

@Test
@Category(IntegrationTest.class)
public void clearAndAdd() throws Exception
{
  setSomeTags(3);

  MrsPyramidMetadata meta = getMetadata();

  String v = meta.getTag("foo", null);
  Assert.assertNotNull("key not set", v);

  String[] args = {"tags", "-i",  allonesPath, "-c", "hello", "world"};
  try
  {
    MrGeo.main(args);
  }
  catch (ExitException e)
  {
    Assert.assertEquals("Command exited with error", 0, e.status);

    meta = getMetadata();

    v = meta.getTag("foo", null);
    Assert.assertNull("key not set", v);

    v = meta.getTag("hello", null);
    Assert.assertNotNull("key not set", v);
    Assert.assertEquals("bad value", "world", v);

  }
}

@Test
@Category(IntegrationTest.class)
public void remove() throws Exception
{
  setSomeTags(2);

  MrsPyramidMetadata meta = getMetadata();

  String v = meta.getTag("fa", null);
  Assert.assertNotNull("key not set", v);

  String[] args = {"tags", "-i",  allonesPath, "-r", "fa"};
  try
  {
    MrGeo.main(args);
  }
  catch (ExitException e)
  {
    Assert.assertEquals("Command exited with error", 0, e.status);

    meta = getMetadata();

    v = meta.getTag("fa", null);
    Assert.assertNull("key not set", v);

    v = meta.getTag("foo", null);
    Assert.assertNotNull("key not set", v);
    Assert.assertEquals("bad value", "bar", v);

  }
}

@Test
@Category(IntegrationTest.class)
public void removeTwo() throws Exception
{
  setSomeTags(3);

  MrsPyramidMetadata meta = getMetadata();

  String v = meta.getTag("foo", null);
  Assert.assertNotNull("key not set", v);

  v = meta.getTag("fa", null);
  Assert.assertNotNull("key not set", v);

  String[] args = {"tags", "-i",  allonesPath, "-r", "fa", "foo"};
  try
  {
    MrGeo.main(args);
  }
  catch (ExitException e)
  {
    Assert.assertEquals("Command exited with error", 0, e.status);

    meta = getMetadata();

    v = meta.getTag("fa", null);
    Assert.assertNull("key set", v);

    v = meta.getTag("foo", null);
    Assert.assertNull("key set", v);
  }
}



}
