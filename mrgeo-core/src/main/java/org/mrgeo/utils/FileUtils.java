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

package org.mrgeo.utils;


import org.mrgeo.core.MrGeoConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;

public class FileUtils
{
private static final Logger log = LoggerFactory.getLogger(FileUtils.class);

public static File createUniqueTmpDir() throws IOException
{
  final File baseDir = new File(System.getProperty("java.io.tmpdir"));

  final String username = "mrgeo-" + System.getProperty("user.name");
  final String baseName = "-" + System.currentTimeMillis();

  final File tempDir = new File(baseDir, username + "/" + baseName);

  return createDisposibleDirectory(tempDir);
}


public static File createTmpUserDir() throws IOException
{
  final File baseDir = new File(System.getProperty("java.io.tmpdir"));
  final String username = "mrgeo-" + System.getProperty("user.name");

  final File tempDir = new File(baseDir, username);

  return createDisposibleDirectory(tempDir);
}

public static File createDisposibleDirectory(File dir) throws IOException
{
  if (!dir.exists())
  {
    if (!dir.mkdir())
    {
      throw new IOException("Error creating directory");
    }

    dir.deleteOnExit();
  }

  return dir;
}
public static File createDir(File dir) throws IOException
{
  if (!dir.getParentFile().exists()) {
    createDir(dir.getParentFile());
  }
  if (!dir.exists())
  {
    if (!dir.mkdir())
    {
      throw new IOException("Error creating directory");
    }
  }

  return dir;
}

public static void deleteDir(final File dir) throws IOException
{
  deleteDir(dir, false);
}

public static void deleteDir(final File dir, final Boolean recursive) throws IOException
{
  if (dir.exists() && dir.isDirectory())
  {
    if (recursive)
    {
      for (File c : dir.listFiles())
      {
        if (c.isDirectory()) {
          deleteDir(c, true);
        }
        else if (!c.delete())
        {
          throw new IOException("Error deleting file");
        }
      }

    }
    if (!dir.delete())
    {
      throw new IOException("Error deleting directory");
    }
  }
}


public static String resolveURI(final String path)
{
  try
  {
    URI uri = new URI(path);
    if (uri.getScheme() == null)
    {
      String fragment = uri.getFragment();
      URI part = new File(uri.getPath()).toURI();

      uri = new URI(part.getScheme(), part.getPath(), fragment);
    }
    return uri.toString();
  }
  catch (URISyntaxException e)
  {
    e.printStackTrace();
  }

  return path;
}

public static String resolveURL(final String path)
{
  try
  {
    URI uri = new URI(path);
    if (uri.getScheme() == null)
    {
      String fragment = uri.getFragment();
      URI part = new File(uri.getPath()).toURI();

      uri = new URI(part.getScheme(), part.getPath(), fragment);
    }
    return uri.toURL().toString();
  }
  catch (URISyntaxException e)
  {
  }
  catch (MalformedURLException e)
  {
  }

  return path;
}

}
