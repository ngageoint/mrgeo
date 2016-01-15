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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.ClassUtil;
import org.apache.hadoop.util.GenericOptionsParser;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.mapalgebra.vector.PgQueryInputFormat;
import org.mrgeo.pyramid.MrsPyramidMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

/**
 * 
 */
public class HadoopVectorUtils
{
  private static final Logger log = LoggerFactory.getLogger(HadoopUtils.class);
  private static Random random = new Random(System.currentTimeMillis());

  private static Constructor<?> taskAttempt = null;
  private static Constructor<?> jobContext = null;
  static
  {
    Configuration.addDefaultResource("mapred-default.xml");
    Configuration.addDefaultResource("hdfs-default.xml");
    Configuration.addDefaultResource("core-site.xml");
    Configuration.addDefaultResource("mapred-site.xml");
    Configuration.addDefaultResource("hdfs-site.xml");
  }

  /**
   * Add a {@link org.apache.hadoop.fs.Path} to the list of inputs for the map-reduce job.
   *
   * NOTE: This was copied directly from the 1.0.3 source because there is a bug in the 20.2 version
   * of this method. When the Path references a a local file, the 20.2 added in improperly formatted
   * path to the job configuration. It looked like file://localhost:9001/my/path/to/file.tif.
   *
   * @param job
   *          The {@link org.apache.hadoop.mapreduce.Job} to modify
   * @param path
   *          {@link org.apache.hadoop.fs.Path} to be added to the list of inputs for the map-reduce job.
   */
  public static void addInputPath(final Job job, final Path path) throws IOException
  {
    final Configuration conf = job.getConfiguration();
    final Path p = path.getFileSystem(conf).makeQualified(path);
    final String dirStr = org.apache.hadoop.util.StringUtils.escapeString(p.toString());
    final String dirs = conf.get("mapred.input.dir");
    conf.set("mapred.input.dir", dirs == null ? dirStr : dirs + "," + dirStr);
  }

  /**
   * Creates and initializes a new Hadoop configuration. This should never be called by mappers or
   * reducers (remote nodes) or any code that they call.
   */
  @SuppressWarnings("unused")
  public synchronized static Configuration createConfiguration()
  {
    final Configuration config = new Configuration();

    // enables serialization of Serializable objects in Hadoop.
    final String serializations = config.get("io.serializations");
    config.set("io.serializations", serializations + ",org.mrgeo.format.FeatureSerialization" +
      ",org.apache.hadoop.io.serializer.JavaSerialization");
    try
    {
      final Properties p = MrGeoProperties.getInstance();
      final String hadoopParams = p.getProperty("hadoop.params");
      // Usage of GenericOptionsParser was inspired by Hadoop's ToolRunner
      if (hadoopParams != null)
      {
        final String[] hadoopParamsAsArray = hadoopParams.split(" ");
        final GenericOptionsParser parser = new GenericOptionsParser(config, hadoopParamsAsArray);
      }

    }
    catch (final Exception e)
    {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    return config;
  }

  // Between Hadoop 1.0 (chd3) and 2.0 (cdh4), JobContext changed from a concrete class
  // to an interface. This method uses reflection to determine the appropriate class to create and
  // returns a JobContext appropriately constructed
  public static JobContext createJobContext(final Configuration conf, final JobID id)
  {
    if (jobContext == null)
    {
      loadJobContextClass();
    }

    try
    {
      return (JobContext) jobContext.newInstance(new Object[] { conf, id });
    }
    catch (final IllegalArgumentException e)
    {
      e.printStackTrace();
    }
    catch (final InstantiationException e)
    {
      e.printStackTrace();
    }
    catch (final IllegalAccessException e)
    {
      e.printStackTrace();
    }
    catch (final InvocationTargetException e)
    {
      e.printStackTrace();
    }

    return null;
  }

  /**
   * Creates a random string filled with hex values.
   */
  public static synchronized String createRandomString(final int size)
  {
    // create a random string of about 1000 characters. This will force the
    // sequence file to split appropriately. Certainly a hack, but shouldn't
    // cause much of a difference in speed, or storage.
    String randomString = "";
    while (randomString.length() < size)
    {
      randomString += Long.toHexString(random.nextLong());
    }
    return randomString.substring(0, size);
  }

  // Between Hadoop 1.0 (chd3) and 2.0 (cdh4), TaskAttemptContext changed from a concrete class
  // to an interface. This method uses reflection to determine the appropriate class to create and
  // returns a TaskAttemptContext appropriately constructed
  public static TaskAttemptContext createTaskAttemptContext(final Configuration conf,
    final TaskAttemptID id)
  {
    if (taskAttempt == null)
    {
      loadTaskAttemptClass();
    }

    try
    {
      return (TaskAttemptContext) taskAttempt.newInstance(new Object[] { conf, id });
    }
    catch (final IllegalArgumentException e)
    {
      e.printStackTrace();
    }
    catch (final InstantiationException e)
    {
      e.printStackTrace();
    }
    catch (final IllegalAccessException e)
    {
      e.printStackTrace();
    }
    catch (final InvocationTargetException e)
    {
      e.printStackTrace();
    }

    return null;
  }

  public static String createUniqueJobName(final String baseName)
  {
    // create a new unique job name
    final String now = new SimpleDateFormat("yyyy-MM-dd'T'HH-mm-ss").format(new Date());

    final String jobName = baseName + "_" + now + "_" + UUID.randomUUID().toString();

    return jobName;
  }

  public static String[] getDefaultVectorBaseDirectories(final Properties props)
  {
    final String defaultDirs[] = null;

    final String listDirs = props.getProperty(MrGeoConstants.MRGEO_HDFS_VECTOR, null);
    if (listDirs != null)
    {
      final String[] dirs = listDirs.split(",");
      if (dirs != null && dirs.length != 0)
      {
        for (int i = 0; i < dirs.length; i++)
        {
          if (!dirs[i].endsWith("/"))
          {
            dirs[i] += "/";
          }
        }
        return dirs;
      }
    }
    return defaultDirs;
  }

  public static String getDefaultVectorBaseDirectory()
  {
    return getDefaultVectorBaseDirectory(MrGeoProperties.getInstance());
  }

  public static String getDefaultVectorBaseDirectory(final Properties props)
  {
    final String defaultVectorDir = null;
    final String[] dirs = getDefaultVectorBaseDirectories(props);

    if (dirs != null && dirs.length != 0)
    {
      return dirs[0];
    }
    return defaultVectorDir;
  }

  public static double[]
    getDoubleArraySetting(final Configuration config, final String propertyName)
  {
    final String[] strValues = getStringArraySetting(config, propertyName);
    final double[] result = new double[strValues.length];
    for (int ii = 0; ii < strValues.length; ii++)
    {
      // Note: this will throw an exception if parsing is unsuccessful
      result[ii] = Double.parseDouble(strValues[ii]);
    }
    return result;
  }

  /**
   * Formats a string with all of a job's failed task attempts.
   *
   * @param jobId
   *          ID of the job for which to retrieve failed task info
   * @param showStackTrace
   *          if true; the entire stack trace will be shown for each failure exception
   * @param taskLimit
   *          maximum number of tasks to add to the output message
   * @return formatted task failure string if job with jobId exists; empty string otherwise
   * @throws java.io.IOException
   * @todo will rid of the deprecated code, if I can figure out how to...API is confusing
   */
  public static String getFailedTasksString(final String jobId, final boolean showStackTrace,
    final int taskLimit) throws IOException
  {
    final JobClient jobClient = new JobClient(new JobConf(HadoopUtils.createConfiguration()));
    final RunningJob job = jobClient.getJob(jobId);
    final TaskCompletionEvent[] taskEvents = job.getTaskCompletionEvents(0);
    String failedTasksMsg = "";
    int numTasks = taskEvents.length;
    if (taskLimit > 0 && taskLimit < numTasks)
    {
      numTasks = taskLimit;
    }
    int taskCtr = 0;
    for (int i = 0; i < numTasks; i++)
    {
      final TaskCompletionEvent taskEvent = taskEvents[i];
      if (taskEvent.getTaskStatus().equals(TaskCompletionEvent.Status.FAILED))
      {
        final org.apache.hadoop.mapred.TaskAttemptID taskId = taskEvent.getTaskAttemptId();
        final String[] taskDiagnostics = job.getTaskDiagnostics(taskId);
        if (taskDiagnostics != null)
        {
          taskCtr++;
          failedTasksMsg += "\nTask " + String.valueOf(taskCtr) + ": ";
          for (final String taskDiagnostic : taskDiagnostics)
          {
            if (showStackTrace)
            {
              failedTasksMsg += taskDiagnostic;
            }
            else
            {
              failedTasksMsg += taskDiagnostic.split("\\n")[0];
            }
          }
        }
      }
    }
    return failedTasksMsg;
  }

  public static int[] getIntArraySetting(final Configuration config, final String propertyName)
  {
    final String[] strValues = getStringArraySetting(config, propertyName);
    final int[] result = new int[strValues.length];
    for (int ii = 0; ii < strValues.length; ii++)
    {
      // Note: this will throw an exception if parsing is unsuccessful
      result[ii] = Integer.parseInt(strValues[ii]);
    }
    return result;
  }

//  public static Map<String, MrsImagePyramidMetadata> getMetadata(final Configuration config)
//    throws IOException, ClassNotFoundException
//  {
//    final Map<String, MrsImagePyramidMetadata> metadata = new HashMap<String, MrsImagePyramidMetadata>();
//
//    final Iterator<Entry<String, String>> props = config.iterator();
//    while (props.hasNext())
//    {
//      final Entry<String, String> entry = props.next();
//      if (entry.getKey().startsWith("mrspyramid.metadata."))
//      {
//        final MrsImagePyramidMetadata m = (MrsImagePyramidMetadata) Base64Utils
//          .decodeToObject(entry.getValue());
//        final String name = entry.getKey().substring(20); // strip the "mrspyramid.metadata."
//        metadata.put(name, m);
//      }
//    }
//    // Map<String, String> raw = config.getValByRegex("mrspyramid.metadata.*");
//    // for (String basename:raw.keySet())
//    // {
//    // MrsImagePyramidMetadata m = (MrsImagePyramidMetadata)
//    // Base64Utils.decodeToObject(config.get(basename,
//    // null));
//    // String name = basename.substring(20); // strip the "mrspyramid.metadata."
//    // metadata.put(name, m);
//    // }
//
//    return metadata;
//    // return (MrsImagePyramidMetadata) Base64Utils.decodeToObject(config.get("mrspyramid.metadata",
//    // null));
//  }

  public static MrsPyramidMetadata
    getMetadata(final Configuration config, final String pyramid) throws IOException,
      ClassNotFoundException
  {
    final MrsPyramidMetadata metadata = (MrsPyramidMetadata) Base64Utils
      .decodeToObject(config.get("mrspyramid.metadata." + pyramid, null));

    return metadata;
    // return (MrsImagePyramidMetadata) Base64Utils.decodeToObject(config.get("mrspyramid.metadata",
    // null));
  }

  public static String[]
    getStringArraySetting(final Configuration config, final String propertyName)
  {
    final String str = config.get(propertyName);
    if (str == null || str.length() == 0)
    {
      return new String[0];
    }
    final String[] strValues = str.split(",");
    for (int ii = 0; ii < strValues.length; ii++)
    {
      strValues[ii] = strValues[ii].trim();
    }
    return strValues;
  }


  // public static boolean hasHadoop()
  // {
  // // windows requires cygwin to run Hadoop. I don't like that, so we just
  // // default to the raw fs
  // // in windows this won't work for remote calls and such, but we should be
  // // using linux for that.
  // if (System.getProperty("os.name").toLowerCase().contains("windows"))
  // {
  // log.info("Running on an OS w/o Hadoop. Hmmm. This could get ugly.");
  // return false;
  // }
  // return true;
  // }

  public static void setJar(final Job job, Class clazz) throws IOException
  {
    Configuration conf = job.getConfiguration();

    if (HadoopUtils.isLocal(conf))
    {
      String jar = ClassUtil.findContainingJar(clazz);

      if (jar != null)
      {
        conf.set("mapreduce.job.jar", jar);
      }
    }
    else
    {
      DependencyLoader.addDependencies(job, clazz);
    }
  }

  public static void setMetadata(final Configuration conf, final MrsPyramidMetadata metadata)
    throws IOException
  {
    log.debug("Setting hadoop configuration metadata using metadata instance " + metadata);
    conf.set("mrspyramid.metadata." + metadata.getPyramid(), Base64Utils.encodeObject(metadata));
  }

  public static void setMetadata(final Job job, final MrsPyramidMetadata metadata)
    throws IOException
  {
    setMetadata(job.getConfiguration(), metadata);
  }



  public static void setupLocalRunner(final Configuration config) throws IOException
  {
    // hadoop v1 key
    config.set("mapred.job.tracker", "local");
    // hadoop v2 key
    config.set("mapreduce.jobtracker.address", "local");
    config.set("mapreduce.framework.name", "local");

    config.set("mapred.local.dir", FileUtils.createTmpUserDir().getCanonicalPath());
    config.setInt("mapreduce.local.map.tasks.maximum", 1);
    config.setInt("mapreduce.local.reduce.tasks.maximum", 1);
  }


  public static void setupPgQueryInputFormat(final Job job, final String username,
    final String password, final String dbconnection)
  {
    final Configuration conf = job.getConfiguration();
    conf.set(PgQueryInputFormat.USERNAME, username);
    conf.set(PgQueryInputFormat.PASSWORD, password);
    conf.set(PgQueryInputFormat.DBCONNECTION, dbconnection);
  }

  private static void loadJobContextClass()
  {
    try
    {
      final Class<?> jc = Class.forName("org.apache.hadoop.mapreduce.JobContext");
      final Class<?>[] argTypes = { Configuration.class, JobID.class };

      jobContext = jc.getDeclaredConstructor(argTypes);

      return;
    }
    catch (final ClassNotFoundException e)
    {
      // TaskAttemptContext is not a class, could be Hadoop 2.0
    }
    catch (final SecurityException e)
    {
      // Exception, we'll try Hadoop 2.0 just in case...
    }
    catch (final NoSuchMethodException e)
    {
      // Exception, we'll try Hadoop 2.0 just in case...
    }

    try
    {
      final Class<?> jci = Class.forName("org.apache.hadoop.mapreduce.task.JobContextImpl");
      final Class<?>[] argTypes = { Configuration.class, JobID.class };

      jobContext = jci.getDeclaredConstructor(argTypes);

      return;
    }
    catch (final ClassNotFoundException e)
    {
    }
    catch (final SecurityException e)
    {
    }
    catch (final NoSuchMethodException e)
    {
    }

    log.error("ERROR!  Can not find a JobContext implementation class!");
    jobContext = null;
  }

  private static void loadTaskAttemptClass()
  {
    try
    {
      final Class<?> tac = Class.forName("org.apache.hadoop.mapreduce.TaskAttemptContext");
      final Class<?>[] argTypes = { Configuration.class, TaskAttemptID.class };

      taskAttempt = tac.getDeclaredConstructor(argTypes);

      return;
    }
    catch (final ClassNotFoundException e)
    {
      // TaskAttemptContext is not a class, could be Hadoop 2.0
    }
    catch (final SecurityException e)
    {
      // Exception, we'll try Hadoop 2.0 just in case...
    }
    catch (final NoSuchMethodException e)
    {
      // Exception, we'll try Hadoop 2.0 just in case...
    }

    try
    {
      // Class<?> taci = Class.forName("org.apache.hadoop.mapreduce.TaskAttemptContextImpl");
      final Class<?> taci = Class
        .forName("org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl");
      final Class<?>[] argTypes = { Configuration.class, TaskAttemptID.class };

      taskAttempt = taci.getDeclaredConstructor(argTypes);

      return;
    }
    catch (final ClassNotFoundException e)
    {
    }
    catch (final SecurityException e)
    {
    }
    catch (final NoSuchMethodException e)
    {
    }

    log.error("ERROR!  Can not find a TaskAttempt implementation class!");
    taskAttempt = null;
  }
}
