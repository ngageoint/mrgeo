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

import org.jboss.vfs.VFS;
import org.jboss.vfs.VirtualFile;

import java.io.File;
import java.io.IOException;
import java.net.JarURLConnection;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.*;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public class ClassLoaderUtil
{
  static class Thief extends ClassLoader
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
      e1.printStackTrace();
    }
    Thief t = new Thief(classLoader);
    Package[] packages = t.getPackages();
    TreeSet<String> result = new TreeSet<String>();
    
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
        e.printStackTrace();
      }
    }
    
    return result;
  }
 
  public static List<URL> getChildResources(String path) throws IOException, ClassNotFoundException
  {
    List<URL> result = new LinkedList<URL>();

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
        result.addAll( loadVfs( resource ));
      }
      else
      {
        throw new ClassNotFoundException("Unknown protocol on class resource: "
            + resource.toExternalForm());
      }
    }

    return result;
  }

  public static List<URL> loadVfs( URL resource ) throws IOException
  {
      List<URL> result = new LinkedList<URL>();

      try {
          VirtualFile r = VFS.getChild( resource.toURI() );
          if ( r.exists() && r.isDirectory() ) {
              for ( VirtualFile f : r.getChildren() )
              {
                  result.add( f.asFileURL() );
              }
          }
      } catch (URISyntaxException e) {
          System.out.println( "Problem reading resource '" + resource + "':\n " + e.getMessage() );
          e.printStackTrace();
      }

      return result;
  }

  public static List<URL> loadJar(String path, URL resource) throws IOException
  {
    JarURLConnection conn = (JarURLConnection) resource.openConnection();
    JarFile jarFile = conn.getJarFile();
    Enumeration<JarEntry> entries = jarFile.entries();
    List<URL> result = new LinkedList<URL>();
    
    String p = path;
    if (p.endsWith("/") == false)
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

  public static List<URL> loadDirectory(String filePath) throws IOException
  {
    List<URL> result = new LinkedList<URL>();
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
}
