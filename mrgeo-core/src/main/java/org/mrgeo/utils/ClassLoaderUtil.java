/*
 * Copyright 2009-2017. DigitalGlobe, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.mrgeo.utils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.io.FilenameUtils;
import org.jboss.vfs.VFS;
import org.jboss.vfs.VirtualFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.JarURLConnection;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.*;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public class ClassLoaderUtil
{
private static final Logger log = LoggerFactory.getLogger(ClassLoaderUtil.class);

public static Collection<String> getMostJars()
{

  ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
  try
  {
    // this seems to populate more jars. Odd.
    getChildResources("META-INF/services");
    getChildResources("");
    getChildResources("/");
  }
  catch (Exception e1)
  {
    log.error("Exception thrown", e1);
  }

  TreeSet<String> result = new TreeSet<>();
  AccessController.doPrivileged(new PrivilegedAction<Object>()
  {
    public Boolean run()
    {
      Thief t = new Thief(classLoader);
      Package[] packages = t.getPackages();

      for (Package p : packages)
      {
        Enumeration<URL> urls;
        try
        {
          String path = p.getName().replace(".", "/");

          urls = classLoader.getResources(path);
          while (urls.hasMoreElements())
          {
            URL resource = urls.nextElement();
            if (resource.getProtocol().equalsIgnoreCase("jar"))
            {
              JarURLConnection conn = (JarURLConnection) resource.openConnection();
              JarFile jarFile = conn.getJarFile();
              result.add(jarFile.getName());
            }
          }
        }
        catch (IOException e)
        {
          log.error("Exception thrown", e);
        }
      }
      return true;
    }
  });

  return result;
}

public static List<URL> getChildResources(String path) throws IOException, ClassNotFoundException
{
  List<URL> result = new LinkedList<>();

  ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
  Enumeration<URL> p = classLoader.getResources(path);
  while (p.hasMoreElements())
  {
    URL resource = p.nextElement();
    //System.out.println("resource: " + resource.toString());
    if (resource.getProtocol().equalsIgnoreCase("FILE"))
    {
      result.addAll(loadDirectory(resource.getFile()));
    }
    else if (resource.getProtocol().equalsIgnoreCase("JAR"))
    {
      result.addAll(loadJar(path, resource));
    }
    else if (resource.getProtocol().equalsIgnoreCase("VFS"))
    {
      result.addAll(loadVfs(resource));
    }
    else
    {
      throw new ClassNotFoundException("Unknown protocol on class resource: "
          + resource.toExternalForm());
    }
  }

  return result;
}

public static List<URL> loadVfs(URL resource) throws IOException
{
  List<URL> result = new LinkedList<>();

  try
  {
    VirtualFile r = VFS.getChild(resource.toURI());
    if (r.exists() && r.isDirectory())
    {
      for (VirtualFile f : r.getChildren())
      {
        result.add(f.asFileURL());
      }
    }
  }
  catch (URISyntaxException e)
  {
    System.out.println("Problem reading resource '" + resource + "':\n " + e.getMessage());
    log.error("Exception thrown", e);
  }

  return result;
}

public static List<URL> loadJar(String path, URL resource) throws IOException
{
  JarURLConnection conn = (JarURLConnection) resource.openConnection();
  JarFile jarFile = conn.getJarFile();
  Enumeration<JarEntry> entries = jarFile.entries();
  List<URL> result = new LinkedList<>();

  String p = path;
  if (!p.endsWith("/"))
  {
    p = p + "/";
  }

  while (entries.hasMoreElements())
  {
    JarEntry entry = entries.nextElement();
    if ((!entry.getName().equals(p)) && (entry.getName().startsWith(p) || entry.getName()
        .startsWith("WEB-INF/classes/" + p)))
    {
      URL url = new URL("jar:"
          + new URL("file", null, jarFile.getName() + "!/" + entry.getName()));
      result.add(url);
    }
  }

  return result;
}

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "filename comes from classloader")
public static List<URL> loadDirectory(String filePath) throws IOException
{
  List<URL> result = new LinkedList<>();
  File directory = new File(filePath);
  if (!directory.isDirectory())
  {
    throw new IOException("Invalid directory " + directory.getAbsolutePath());
  }

  File[] files = directory.listFiles();
  if (files != null)
  {
    for (File file : files)
    {
      if (file.isDirectory())
      {
        loadDirectory(file.getAbsolutePath());
      }
      else
      {
        result.add(new URL("file", null, file.getAbsolutePath()));
      }
    }
  }
  return result;
}

@SuppressWarnings("squid:S1166") // exceptions are caught and returned as false
public static void addLibraryPath(String pathToAdd)
{
  try
  {
    Field usrPathsField = ClassLoader.class.getDeclaredField("usr_paths");

    AccessController.doPrivileged(new PrivilegedAction<Object>()
    {
      public Boolean run()
      {
        try
        {
          usrPathsField.setAccessible(true);

          //get array of paths
          String[] paths = (String[]) usrPathsField.get(null);

          //check if the path to add is already present
          for (String path : paths)
          {
            if (path.equals(pathToAdd))
            {
              return true;
            }
          }

          //add the new path
          String[] newPaths = new String[paths.length + 1];
          System.arraycopy(paths, 0, newPaths, 1, paths.length);
          //final String[] newPaths = Arrays.copyOf(paths, paths.length + 1);
          newPaths[0] = pathToAdd;
          usrPathsField.set(null, newPaths);


          System.setProperty("java.library.path", StringUtils.join(newPaths, ":"));
          Field sysPathsField = ClassLoader.class.getDeclaredField("sys_paths");
          sysPathsField.setAccessible(true);
          sysPathsField.set(null, null);


          return true;
        }
        catch (IllegalAccessException | NoSuchFieldException ignored)
        {
          return false;
        }
      }
    });
  }
  catch (NoSuchFieldException ignored)
  {
  }
}

@SuppressFBWarnings(value = "WEAK_FILENAMEUTILS", justification = "filename comes from classloader")
public static void dumpClasspath(ClassLoader loader, int level)
{
  System.out.println(indent(level) + "Classloader " + loader + ":");

  if (loader instanceof URLClassLoader)
  {
    URLClassLoader ucl = (URLClassLoader) loader;
    //System.out.println("\t" + Arrays.toString(ucl.getURLs()));

    URL[] urls = ucl.getURLs();
    String[] names = new String[urls.length];

    for (int i = 0; i < urls.length; i++)
    {
//        String name = urls[i].toString();
      String name = FilenameUtils.getName(urls[i].toString());

      if (name.length() > 0)
      {
        names[i] = name;
      }
      else
      {
        names[i] = urls[i].toString();
      }

    }
    Arrays.sort(names);

    for (String name : names)
    {
      System.out.println(indent(level + 1) + name);
    }
  }
  else
  {
    System.out.println("\t(cannot display components as not a URLClassLoader)");
  }

  System.out.println("");
  if (loader.getParent() != null)
  {
    dumpClasspath(loader.getParent(), level + 1);
  }

}

private static String indent(int level)
{
  StringBuilder s = new StringBuilder();
  for (int i = 0; i < level * 3; i++)
  {
    s.append(" ");
  }

  return s.toString();
}

private static class Thief extends ClassLoader
{
  Thief(ClassLoader cl)
  {
    super(cl);
  }

  @Override
  public Package[] getPackages()
  {
    return super.getPackages();
  }
}
}
