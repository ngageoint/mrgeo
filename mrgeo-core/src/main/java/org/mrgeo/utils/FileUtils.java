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

package org.mrgeo.utils;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;

public class FileUtils
{

  public static File createUniqueTmpDir() throws IOException
  {
    final File baseDir = org.apache.commons.io.FileUtils.getTempDirectory();
    final String username = "mrgeo-" + System.getProperty("user.name");
    final String baseName = "-" + System.currentTimeMillis();

    final File tempDir = new File(baseDir, username + "/" + baseName);
    org.apache.commons.io.FileUtils.forceMkdir(tempDir);
    org.apache.commons.io.FileUtils.forceDeleteOnExit(tempDir);

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
    org.apache.commons.io.FileUtils.forceMkdir(dir);
    org.apache.commons.io.FileUtils.forceDeleteOnExit(dir);

    return dir;
  }

  public static void deleteDir(final File dir) throws IOException
  {
    org.apache.commons.io.FileUtils.deleteDirectory(dir);
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
