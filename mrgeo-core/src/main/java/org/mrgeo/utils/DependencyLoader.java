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

import org.apache.commons.lang.ClassUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ClassUtil;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.reflections.Reflections;
import org.reflections.scanners.ResourcesScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.JarURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Pattern;

public class DependencyLoader
{
  private static final Logger log = LoggerFactory.getLogger(DependencyLoader.class);
  private static final String CLASSPATH_FILES = "mapred.job.classpath.files";

  public static Set<String> getDependencies(final Class<?> clazz) throws IOException
  {
    Set<File> files = findDependencies(clazz);

    Set<String> deps = new HashSet<String>();
    for (File f : files)
    {
      deps.add(f.getCanonicalPath());
    }

    return deps;
  }

  public static String getMasterJar(final Class<?> clazz) throws IOException
  {
    Set<Dependency> properties = loadDependenciesByReflection(clazz);
    if (properties != null)
    {
      boolean developmentMode = MrGeoProperties.isDevelopmentMode();
      for (Dependency d: properties)
      {
        if (d.master)
        {
          Set<Dependency> master = new HashSet<Dependency>();
          master.add(d);

          for (File m: getJarsFromProperties(master, !developmentMode))
          {
            return m.getCanonicalPath();
          }
        }
      }
    }

    return null;
  }

  private static Set<File> findDependencies(Class<?> clazz) throws IOException
  {
    Set<File> files;

    Set<Dependency> properties = loadDependenciesByReflection(clazz);
    if (properties != null)
    {
      boolean developmentMode = MrGeoProperties.isDevelopmentMode();
      files = getJarsFromProperties(properties, !developmentMode);
    }
    else
    {
      // properties not found... all we can do is load from the classpath
      files = new HashSet<File>();
      String cpstr = System.getProperty("java.class.path");
      for (String env : cpstr.split(":"))
      {
        files.addAll(findFilesInClasspath(env));
      }
    }
    return files;
  }

  public static Set<String> getUnqualifiedDependencies(final Class<?> clazz) throws IOException
  {
    Set<File> files = findDependencies(clazz);

    Set<String> deps = new HashSet<String>();
    for (File f : files)
    {
      deps.add(f.getName());
    }

    return deps;
  }

  private static Set<File> findFilesInClasspath(String base) throws IOException
  {
    Set<File> files = new HashSet<File>();
    File f = new File(base);
    if (f.exists())
    {
      if (f.isDirectory())
      {
        File[] dir = f.listFiles();
        if (dir != null)
        {
          for (File file : dir)
          {
            files.addAll(findFilesInClasspath(file.getCanonicalPath()));
          }
        }
      }
      else
      {
        if (f.getName().endsWith(".jar"))
        {
          files.add(f);
        }
      }
    }

    return files;
  }

  public static void addDependencies(final Job job, final Class<?> clazz) throws IOException
  {
    if (job.getConfiguration().get("mapred.job.tracker", "local") == "local")
    {
      return;
    }
    FileSystem fs = HadoopFileUtils.getFileSystem(job.getConfiguration());
    Path hdfsBase =
        new Path(MrGeoProperties.getInstance().getProperty(MrGeoConstants.MRGEO_HDFS_DISTRIBUTED_CACHE, "/mrgeo/jars"));

    Set<Dependency> properties = loadDependenciesByReflection(clazz);
    if (properties != null)
    {
      addDependencies(job.getConfiguration(), clazz);
    }
    else
    {
      // properties not found... all we can do is load from the classpath
      addClassPath(job, fs, hdfsBase);
      job.setJarByClass(clazz);
    }
  }

  private static Set<String> getClasspath(Configuration conf)
  {
    String cp = conf.get(CLASSPATH_FILES, "");
    String[] entries = cp.split(System.getProperty("path.separator"));
    Set<String> results = new HashSet<String>(entries.length);
    if (entries.length > 0)
    {
      for (String entry : entries)
      {
        results.add(entry);
      }
    }
    return results;
  }

  public static void addDependencies(final Configuration conf, final Class<?> clazz) throws IOException
  {
    if (conf.get("mapred.job.tracker", "local") == "local")
    {
      return;
    }
    FileSystem fs = HadoopFileUtils.getFileSystem(conf);
    Path hdfsBase =
        new Path(MrGeoProperties.getInstance().getProperty(MrGeoConstants.MRGEO_HDFS_DISTRIBUTED_CACHE, "/mrgeo/jars"));

    Set<String> existing = getClasspath(conf);
    boolean developmentMode = MrGeoProperties.isDevelopmentMode();
    Set<Dependency> properties = loadDependenciesByReflection(clazz);
    if (properties != null)
    {
      for (File p : getJarsFromProperties(properties, !developmentMode))
      {
        addFileToClasspath(conf, existing, fs, hdfsBase, p);
      }
      for (Dependency d: properties)
      {
        if (d.master)
        {
          Set<Dependency> master = new HashSet<Dependency>();
          master.add(d);

          for (File m: getJarsFromProperties(master, !developmentMode))
          {
            log.debug("Setting map.reduce.jar to " + m.getCanonicalPath());
            conf.set("mapreduce.job.jar", m.getCanonicalPath());
          }
        }
      }
    }
    else
    {
      log.warn("Unable to load dependencies by reflection for " + clazz.getName());
    }
  }

  private static Set<File> getJarsFromProperties(Set<Dependency> dependencies,
      boolean recurseDirectories)
      throws IOException
  {
    // now build a list of jars to search for
    Set<String> jars = new HashSet<String>();

    for (Dependency dep : dependencies)
    {
      if (dep.scope != null && dep.type != null &&
          !dep.scope.equals("provided") && !dep.scope.equals("test"))
      {
        jars.add(makeJarFromDependency(dep));
      }
    }

    // find the jars in the classpath
    return findJarsInClasspath(jars, recurseDirectories);
  }

  private static String makeJarFromDependency(Dependency dep)
  {
    return dep.artifact + "-" + dep.version + (dep.classifier == null ? "" : "-" + dep.classifier) + "." + dep.type;
  }

  private static Set<File> findJarsInClasspath(final Set<String> jars, boolean recurseDirectories) throws IOException
  {
    Set<File> paths = new HashSet<File>();

    if (jars.size() == 0)
    {
      return paths;
    }

    Set<String> filesLeft = new HashSet<String>(jars);


    // add the system classpath, including the cwd
    String classpath = System.getProperty("java.class.path", "");

    String depclasspath = MrGeoProperties.getInstance().getProperty("dependency.classpath", null);
    if (depclasspath != null)
    {
      if (classpath == null || classpath.isEmpty())
      {
        classpath = depclasspath;
      }
      else
      {
        classpath = depclasspath + System.getProperty("path.separator") + classpath;
      }
    }

    // prepend MRGEO_HOME
    String mrgeo = System.getenv("MRGEO_HOME");
    if (mrgeo == null)
    {
      log.info("MRGEO_HOME environment variable is undefined. Trying system property");
      mrgeo = System.getProperty("MRGEO_HOME");
    }
    if (mrgeo != null)
    {
      if (classpath == null || classpath.isEmpty())
      {
        classpath = mrgeo;
      }
      else
      {
        classpath = mrgeo + System.getProperty("path.separator") + classpath;
      }
    }
    else
    {
      log.error("MRGEO_HOME is not defined, and may result in inability to find dependent JAR files");
    }

    log.debug("Loading dependent JAR files from claspath: " + classpath);
    String[] classpaths = classpath.split(":");

    for (String cp : classpaths)
    {
      log.debug("Finding dependent JAR files in " + cp);
      if (cp.contains("*"))
      {
        // trim to the last "/" before the "*"
        cp = cp.substring(0, cp.lastIndexOf("/", cp.indexOf("*")));
      }

      File file = new File(cp);
      findJars(file, paths, filesLeft, recurseDirectories);

      // check if we've found all the jars
      if (filesLeft.isEmpty())
      {
        return paths;
      }
    }

    for (String left : filesLeft)
    {
      log.warn("Could not find dependency in classpath: " + left);
    }

    return paths;
  }

  private static void findJars(File file, Set<File> paths, Set<String> filesLeft,
      boolean recurseDirectories) throws IOException
  {
    if (file.exists())
    {
      if (file.isDirectory())
      {
        File[] files = file.listFiles();
        if (files != null)
        {
          for (File f : files)
          {
            if (!f.isDirectory())
            {
              if (filesLeft.contains(f.getName()))
              {
                log.debug("Adding " + f.getName() + " to paths from " + f.getPath());
                paths.add(f);
                filesLeft.remove(f.getName());
                if (filesLeft.isEmpty())
                {
                  return;
                }
              }
            }
            else if (recurseDirectories)
            {
              log.debug("In findJars recursing on dir: " + f.getPath() + " with paths: " + paths.size() + " and filesLeft: " + filesLeft.size());
              findJars(f, paths, filesLeft, recurseDirectories);

              if (filesLeft.isEmpty())
              {
                return;
              }
            }
          }
        }
      }
      else if (filesLeft.contains(file.getName()))
      {
        log.debug("Adding " + file.getName() + " to paths from " + file.getPath());
        paths.add(file);
        filesLeft.remove(file.getName());
      }
    }
  }

//  private static void extractDependencies(Properties properties, Dependency master,
//      Map<String, Dependency> dependencies) throws IOException
//  {
//    for (Map.Entry<Object, Object> property : properties.entrySet())
//    {
//      String keystr = property.getKey().toString();
//      String[] key = keystr.split("/");
//
//      if (key.length == 1)
//      {
//        // this is probably the "master" for the entire jar
//        addToDependency(master, key[0], property.getValue().toString());
//      }
//      else if (key.length != 3)
//      {
//        throw new IOException("Error reading property: " + keystr);
//      }
//      else
//      {
//        String artifact = key[1];
//        Dependency dep;
//
//        if (dependencies.containsKey(artifact))
//        {
//          dep = dependencies.get(artifact);
//        }
//        else
//        {
//          dep = new Dependency();
//          addToDependency(dep, "groupId", key[0]);
//          addToDependency(dep, "artifactId", artifact);
//
//          dependencies.put(artifact, dep);
//        }
//
//        addToDependency(dep, key[2], property.getValue().toString());
//      }
//    }
//  }

  private static void addToDependency(Dependency dependency, String property, String value)
  {
    switch (property)
    {
    case "artifactId":
      dependency.artifact = value;
      break;
    case "classifier":
      dependency.classifier = value;
      break;
    case "groupId":
      dependency.group = value;
      break;
    case "scope":
      dependency.scope = value;
      break;
    case "type":
      dependency.type = value;
      break;
    case "version":
      dependency.version = value;
      break;
    }
  }

  private static void addClassPath(Job job, FileSystem fs, Path hdfsBase) throws IOException
  {
    // make sure the jar path exists
    HadoopFileUtils.create(job.getConfiguration(), hdfsBase);

    Configuration conf = job.getConfiguration();

    Set<String> existing = getClasspath(conf);

    String cpstr = System.getProperty("java.class.path");
    for (String env : cpstr.split(":"))
    {
      addFilesToClassPath(conf, existing, fs, hdfsBase, env);
    }
  }

  private static void addFilesToClassPath(Configuration conf, Set<String> existing, FileSystem fs, Path hdfsBase, String base) throws IOException
  {
    File f = new File(base);
    if (f.exists())
    {
      if (f.isDirectory())
      {
        File[] files = f.listFiles();
        if (files != null)
        {
          for (File file : files)
          {
            addFilesToClassPath(conf, existing, fs, hdfsBase, file.getCanonicalPath());
          }
        }
      }
      else
      {
        if (f.getName().endsWith(".jar"))
        {
          addFileToClasspath(conf, existing, fs, hdfsBase, f);
        }
      }
    }
  }

  private static void addFileToClasspath(Configuration conf, Set<String> existing, FileSystem fs, Path hdfsBase, File file) throws IOException
  {
    Path hdfsPath = new Path(hdfsBase, file.getName());
    if (!existing.contains(hdfsPath.toString()))
    {
      if (fs.exists(hdfsPath))
      {
        // check the timestamp and exit if the one in hdfs is "newer"
        FileStatus status = fs.getFileStatus(hdfsPath);

        if (file.lastModified() <= status.getModificationTime())
        {
          log.debug(file.getPath() + " up to date");
          DistributedCache.addFileToClassPath(hdfsPath, conf, fs);

          existing.add(hdfsPath.toString());
          return;
        }
      }

      // copy the file...
      log.debug("Copying " + file.getPath() + " to HDFS for distribution");

      fs.copyFromLocalFile(new Path(file.getCanonicalFile().toURI()), hdfsPath);
      DistributedCache.addFileToClassPath(hdfsPath, conf, fs);
      existing.add(hdfsPath.toString());
    }
  }

  private static Set<Dependency> loadDependenciesByReflection(Class<?> clazz) throws IOException
  {
    String jar = ClassUtil.findContainingJar(clazz);

    if (jar != null)
    {
      return loadDependenciesFromJar(jar);
    }
    else
    {
      // the properties may have been added on the classpath, lets see if we can find it...
      Set<Dependency> deps = null;

      // set up a resource scanner
      Reflections reflections = new Reflections(new ConfigurationBuilder()
          .setUrls(ClasspathHelper.forPackage(ClassUtils.getPackageName(DependencyLoader.class)))
          .setScanners(new ResourcesScanner()));

      final Set<String> resources = reflections.getResources(Pattern.compile(".*dependencies\\.properties"));
      for (String resource : resources)
      {
        log.debug("Loading dependency properties from: /" + resource);

        InputStream is = DependencyLoader.class.getResourceAsStream("/" + resource);

        Set<Dependency> d = readDependencies(is);
        is.close();

        if (deps == null)
        {
          deps = d;
        }
        else
        {
          deps.addAll(d);
        }
      }

      return deps;
    }
  }

  private static Set<Dependency> readDependencies(InputStream is) throws IOException
  {
    Set<Dependency> deps = new HashSet<Dependency>();

    BufferedReader reader = new BufferedReader(new InputStreamReader(is));

    boolean first = true;
    Dependency master = new Dependency();
    master.type = "jar";
    master.scope = "compile";
    master.master = true;

    Dependency dep = null;

    String line;
    while ((line = reader.readLine()) != null)
    {
      line = line.trim();
      if (!line.startsWith("#") && line.length() > 0)
      {
        String[] vals = line.split("|");

        if (first)
        {
          if (vals.length == 4)
          {
            master.artifact = vals[1];
            master.version = vals[2];

            deps.add(master);

          }

          first = false;
        }
        else if (vals.length == 6)
        {
          dep = new Dependency();

          dep.artifact = vals[1];
          dep.version = vals[2];
          dep.classifier = (vals[3].length() > 0) ? vals[3] : null;
          dep.type = vals[4];
          dep.scope = vals[5];

          deps.add(dep);
        }
      }
    }


    if (log.isDebugEnabled())
    {
      log.debug("Dependencies: ");
      for (Dependency d: deps)
      {
        log.debug("  " + makeJarFromDependency(d));
      }
    }
    return deps;
  }

  private static Set<Dependency> loadDependenciesFromJar(final String jarname) throws IOException
  {
    try
    {
      String jar = jarname;

      URL url;
      JarURLConnection conn;

      try
      {
        url = new URL(jar);
        conn = (JarURLConnection) url.openConnection();
      }
      catch (MalformedURLException e)
      {
        jar = (new File(jar)).toURI().toString();
        if (!jar.startsWith("jar:"))
        {
          jar = "jar:" + jar;
        }

        if (!jar.contains("!/"))
        {
          jar += "!/";
        }

        url = new URL(jar);
        conn = (JarURLConnection) url.openConnection();
      }

      log.debug("Looking in " + jar + " for dependency file");

      JarFile jf = conn.getJarFile();

      for (Enumeration<JarEntry> i = jf.entries(); i.hasMoreElements(); )
      {
        JarEntry je = i.nextElement();
        String name = je.getName();
        if (name.endsWith("dependencies.properties"))
        {
          log.debug("Found dependency for " + jar + " -> " + name);
          URL resource = new URL(jar + name);
          InputStream is = resource.openStream();

          Set<Dependency> deps = readDependencies(is);

          is.close();
          return deps;
        }
      }
    }
    catch (IOException e)
    {
      e.printStackTrace();
      throw new IOException("Error Loading dependency properties file", e);
    }

    throw new IOException("No dependency properties file found in " + jarname);
  }

  private static class Dependency
  {
    public String group;
    public String artifact;
    public String version;
    public String type;
    public String scope;
    public String classifier;
    public boolean master = false;

    @Override
    public String toString()
    {
      return group + ":" + artifact + ":" + version + (classifier == null ? "" : ":" + classifier) + ":" + type + ":" +
          scope;
    }
  }

}
