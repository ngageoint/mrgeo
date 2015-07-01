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

//
// copied from Hadoop 1.1.2's "MultipleOutputs" class and delicately
// hacked
//
// taken from: https://github.com/paulhoule/infovore/tree/master/bakemono/src/main/java/com/ontology2/bakemono/mapred
// and renamed to DirectoryMultipleOutputs
//

package org.mrgeo.hadoop.multipleoutputs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.mrgeo.utils.HadoopUtils;

import java.io.IOException;
import java.util.*;

/* If you want Javadoc,  this is different from the "real" MultipleOutputs class
 * but overall similar in character.  That javadoc could be copied over and then
 * pasted up in here 
 * 
 */

public class DirectoryMultipleOutputs<KEYOUT, VALUEOUT>
{

  /**
   * Wraps RecordWriter to increment counters.
   */
  @SuppressWarnings("rawtypes")
  private static class RecordWriterWithCounter extends RecordWriter
  {
    private final RecordWriter writer;
    private final String counterName;
    private final TaskInputOutputContext context;

    public RecordWriterWithCounter(final RecordWriter writer, final String counterName,
      final TaskInputOutputContext context)
    {
      this.writer = writer;
      this.counterName = counterName;
      this.context = context;
    }

    @Override
    @SuppressWarnings("hiding")
    public void close(final TaskAttemptContext context) throws IOException, InterruptedException
    {
      writer.close(context);
    }

    @Override
    public void write(final Object key, final Object value) throws IOException,
      InterruptedException
    {
      context.getCounter(COUNTERS_GROUP, counterName).increment(1);
      writer.write(key, value);
    }
  }

  private static final String base = DirectoryMultipleOutputs.class.getSimpleName();

  private static final String MO_PREFIX = base + ".namedOutput.";
  private static final String FORMAT = ".format";
  private static final String KEY = ".key";
  private static final String VALUE = ".value";
  private static final String HDFS_PATH = ".hdfsPath";

  //
  // copied from FileOutputFormat, used later in inlining of package-visible
  // code from that class
  //

  private static final String COUNTERS_ENABLED = base + ".counters";

  protected static final String BASE_OUTPUT_NAME = "mapreduce.output.basename";

  /**
   * Counters group used by the counters of MultipleOutputs.
   */
  static final String COUNTERS_GROUP = DirectoryMultipleOutputs.class.getName();
  /**
   * Cache for the taskContexts
   */
  private final Map<String, TaskAttemptContext> taskContexts = new HashMap<String, TaskAttemptContext>();

  /**
   * Cached TaskAttemptContext which uses the job's configured settings
   */
  private TaskAttemptContext jobOutputFormatContext;

  private final TaskInputOutputContext<?, ?, KEYOUT, VALUEOUT> context;

  private final Set<String> namedOutputs;

  // Returns list of channel names -- infovore made this package scope so this
  // can be seen when we are initializating the committer

  private final Map<String, RecordWriter<?, ?>> recordWriters;

  private final boolean countersEnabled;

  /**
   * Creates and initializes multiple outputs support, it should be instantiated in the
   * Mapper/Reducer setup method.
   * 
   * @param context
   *          the TaskInputOutputContext object
   */
  public DirectoryMultipleOutputs(final TaskInputOutputContext<?, ?, KEYOUT, VALUEOUT> context)
  {
    this.context = context;
    namedOutputs = Collections.unmodifiableSet(new HashSet<String>(getNamedOutputsList(context)));
    recordWriters = new HashMap<String, RecordWriter<?, ?>>();
    countersEnabled = getCountersEnabled(context);
  }

  /**
   * Adds a named output for the job.
   * <p/>
   * 
   * @param job
   *          job to add the named output
   * @param namedOutput
   *          named output name, it has to be a word, letters and numbers only, cannot be the word
   *          'part' as that is reserved for the default output.
   * 
   *          The named output is a key used internally that references the hdfsPath give as the
   *          next argument
   * 
   * @param Path
   *          to output in HDFS
   * @param outputFormatClass
   *          OutputFormat class.
   * @param keyClass
   *          key class
   * @param valueClass
   *          value class
   */
  @SuppressWarnings("rawtypes")
  public static void addNamedOutput(final Job job, final String namedOutput, final Path hdfsPath,
    final Class<? extends OutputFormat> outputFormatClass, final Class<?> keyClass,
    final Class<?> valueClass)
  {
    checkNamedOutputName(job, namedOutput, true);
    final Configuration conf = job.getConfiguration();
    conf.set(base, conf.get(base, "") + " " + namedOutput);
    conf.setClass(MO_PREFIX + namedOutput + FORMAT, outputFormatClass, OutputFormat.class);
    conf.setClass(MO_PREFIX + namedOutput + KEY, keyClass, Object.class);
    conf.setClass(MO_PREFIX + namedOutput + VALUE, valueClass, Object.class);
    conf.set(MO_PREFIX + namedOutput + HDFS_PATH, hdfsPath.toString());
  }

  /**
   * Returns if the counters for the named outputs are enabled or not. By default these counters are
   * disabled.
   * 
   * @param job
   *          the job
   * @return TRUE if the counters are enabled, FALSE if they are disabled.
   */
  public static boolean getCountersEnabled(final JobContext job)
  {
    return job.getConfiguration().getBoolean(COUNTERS_ENABLED, false);
  }

  /**
   * Enables or disables counters for the named outputs.
   * 
   * The counters group is the {@link MultipleOutputs} class name. The names of the counters are the
   * same as the named outputs. These counters count the number records written to each output name.
   * By default these counters are disabled.
   * 
   * @param job
   *          job to enable counters
   * @param enabled
   *          indicates if the counters will be enabled or not.
   */
  public static void setCountersEnabled(final Job job, final boolean enabled)
  {
    job.getConfiguration().setBoolean(COUNTERS_ENABLED, enabled);
  }

  /**
   * Checks if output name is valid.
   * 
   * name cannot be the name used for the default output
   * 
   * @param outputPath
   *          base output Name
   * @throws IllegalArgumentException
   *           if the output name is not valid.
   */
  private static void checkBaseOutputPath(final String outputPath)
  {
    if (outputPath.equals("part"))
    {
      throw new IllegalArgumentException("output name cannot be 'part'");
    }
  }

  /**
   * Checks if a named output name is valid.
   * 
   * @param namedOutput
   *          named output Name
   * @throws IllegalArgumentException
   *           if the output name is not valid.
   */
  private static void checkNamedOutputName(final JobContext job, final String namedOutput,
    final boolean alreadyDefined)
  {
    checkTokenName(namedOutput);
    checkBaseOutputPath(namedOutput);
    final List<String> definedChannels = getNamedOutputsList(job);
    if (alreadyDefined && definedChannels.contains(namedOutput))
    {
      throw new IllegalArgumentException("Named output '" + namedOutput +
        "' already alreadyDefined");
    }
    else if (!alreadyDefined && !definedChannels.contains(namedOutput))
    {
      throw new IllegalArgumentException("Named output '" + namedOutput + "' not defined");
    }
  }

  /**
   * Checks if a named output name is valid token.
   * 
   * @param namedOutput
   *          named output Name
   * @throws IllegalArgumentException
   *           if the output name is not valid.
   */
  private static void checkTokenName(final String namedOutput)
  {
    if (namedOutput == null || namedOutput.length() == 0)
    {
      throw new IllegalArgumentException("Name cannot be NULL or emtpy");
    }
//    for (final char ch : namedOutput.toCharArray())
//    {
//      if ((ch >= 'A') && (ch <= 'Z'))
//      {
//        continue;
//      }
//      if ((ch >= 'a') && (ch <= 'z'))
//      {
//        continue;
//      }
//      if ((ch >= '0') && (ch <= '9'))
//      {
//        continue;
//      }
//      throw new IllegalArgumentException("Name cannot be have a '" + ch + "' char");
//    }
  }

  // instance code, to be used from Mapper/Reducer code

  // Returns the named output OutputFormat.
  private static Class<? extends OutputFormat<?, ?>> getNamedOutputFormatClass(
    final JobContext job, final String namedOutput)
  {
    return (Class<? extends OutputFormat<?, ?>>) job.getConfiguration().getClass(
      MO_PREFIX + namedOutput + FORMAT, null, OutputFormat.class);
  }

  // Returns the key class for a named output.
  private static Class<?> getNamedOutputKeyClass(final JobContext job, final String namedOutput)
  {
    return job.getConfiguration().getClass(MO_PREFIX + namedOutput + KEY, null, Object.class);
  }

  // Returns the value class for a named output.
  private static Class<?> getNamedOutputValueClass(final JobContext job, final String namedOutput)
  {
    return job.getConfiguration().getClass(MO_PREFIX + namedOutput + VALUE, null, Object.class);
  }

  static TaskAttemptContext _getContext(final TaskAttemptContext context, final String nameOutput)
    throws IOException
  {
    TaskAttemptContext taskContext;
    
    // The following trick leverages the instantiation of a record writer via
    // the job thus supporting arbitrary output formats; it also bypasses
    // the lack of the set method we want on Job here.
    final Configuration clonedConfiguration = new Configuration(context.getConfiguration());
    String path = getHdfsPath(context, nameOutput);
    clonedConfiguration.set("mapred.output.dir", path);

    final Job job = new Job(clonedConfiguration);
    job.setOutputFormatClass(getNamedOutputFormatClass(context, nameOutput));
    job.setOutputKeyClass(getNamedOutputKeyClass(context, nameOutput));
    job.setOutputValueClass(getNamedOutputValueClass(context, nameOutput));

    taskContext = HadoopUtils.createTaskAttemptContext(job.getConfiguration(), context.getTaskAttemptID());
    return taskContext;
  }

  public static String getHdfsPath(final JobContext job, final String namedOutput)
  {
    return job.getConfiguration().get(MO_PREFIX + namedOutput + HDFS_PATH);
  }

  static List<String> getNamedOutputsList(final JobContext job)
  {
    return getNamedOutputsList(job.getConfiguration());
  }

  static public List<String> getNamedOutputsList(final Configuration conf)
  {
    final List<String> names = new ArrayList<String>();
    final StringTokenizer st = new StringTokenizer(
        conf.get(base, ""), " ");
    while (st.hasMoreTokens())
    {
      names.add(st.nextToken());
    }
    return names;
  }

  /**
   * Closes all the opened outputs.
   * 
   * This should be called from cleanup method of map/reduce task. If overridden subclasses must
   * invoke <code>super.close()</code> at the end of their <code>close()</code>
   * 
   */
  @SuppressWarnings("rawtypes")
  public void close() throws IOException, InterruptedException
  {
    for (final RecordWriter writer : recordWriters.values())
    {
      writer.close(context);
    }
  }

  /**
   * Write key value to an output file name.
   * 
   * Gets the record writer from job's output format. Job's output format should be a
   * FileOutputFormat.
   * 
   * @param key
   *          the key
   * @param value
   *          the value
   * @param baseOutputPath
   *          base-output path to write the record to. Note: Framework will generate unique filename
   *          for the baseOutputPath
   */
  @SuppressWarnings("unchecked")
  public void write(final KEYOUT key, final VALUEOUT value, final String baseOutputPath)
    throws IOException, InterruptedException
  {
    checkBaseOutputPath(baseOutputPath);
    if (jobOutputFormatContext == null)
    {
      jobOutputFormatContext = HadoopUtils.createTaskAttemptContext(context.getConfiguration(), context
        .getTaskAttemptID());
    }
    getRecordWriter(jobOutputFormatContext, baseOutputPath).write(key, value);
  }

  /**
   * Write key and value to the namedOutput.
   * 
   * Output path is a unique file generated for the namedOutput. For example,
   * {namedOutput}-(m|r)-{part-number}
   * 
   * @param namedOutput
   *          the named output name
   * @param key
   *          the key
   * @param value
   *          the value
   */
  public <K, V> void write(final String namedOutput, final K key, final V value)
    throws IOException, InterruptedException
  {
    write(namedOutput, key, value, namedOutput);
  }

  /**
   * Write key and value to baseOutputPath using the namedOutput.
   * 
   * @param namedOutput
   *          the named output name
   * @param key
   *          the key
   * @param value
   *          the value
   * @param baseOutputPath
   *          base-output path to write the record to. Note: Framework will generate unique filename
   *          for the baseOutputPath
   */
  @SuppressWarnings("unchecked")
  public <K, V> void write(final String namedOutput, final K key, final V value,
    final String baseOutputPath) throws IOException, InterruptedException
  {
    checkNamedOutputName(context, namedOutput, false);
    checkBaseOutputPath(baseOutputPath);
    if (!namedOutputs.contains(namedOutput))
    {
      throw new IllegalArgumentException("Undefined named output '" + namedOutput + "'");
    }
    final TaskAttemptContext taskContext = getContext(namedOutput);
    getRecordWriter(taskContext, baseOutputPath).write(key, value);
  }

  // Create a taskAttemptContext for the named output with
  // output format and output key/value types put in the context
  private TaskAttemptContext getContext(final String nameOutput) throws IOException
  {

    TaskAttemptContext taskContext = taskContexts.get(nameOutput);

    if (taskContext != null)
    {
      return taskContext;
    }

    taskContext = _getContext(context, nameOutput);

    taskContexts.put(nameOutput, taskContext);

    return taskContext;
  }

  // by being synchronized MultipleOutputTask can be use with a
  // MultithreadedMapper.
  @SuppressWarnings("rawtypes")
  private synchronized RecordWriter getRecordWriter(final TaskAttemptContext taskContext,
    final String baseFileName) throws IOException, InterruptedException
  {

    // look for record-writer in the cache
    RecordWriter writer = recordWriters.get(baseFileName);

    // If not in cache, create a new one
    if (writer == null)
    {
      // in MultipleOutputs, the following commented out line of code was used here
      //
      // FileOutputFormat.setOutputName(taskContext, baseFileName);
      //
      // we can't do that because this method has package visibility but we can do something
      // even worse and inline that code
      //

      // this makes the output file have the same prefix as the directory, instead of the default
      // "part".
      //taskContext.getConfiguration().set(BASE_OUTPUT_NAME, baseFileName);

      try
      {
        Configuration conf = taskContext.getConfiguration();
        
        Class<? extends OutputFormat<?,?>> format = taskContext.getOutputFormatClass();
        OutputFormat of = ReflectionUtils.newInstance(format, conf);
        
        writer = of.getRecordWriter(taskContext);
      }
      catch (final ClassNotFoundException e)
      {
        throw new IOException(e);
      }

      // if counters are enabled, wrap the writer with context
      // to increment counters
      if (countersEnabled)
      {
        writer = new RecordWriterWithCounter(writer, baseFileName, context);
      }

      // add the record-writer to the cache
      recordWriters.put(baseFileName, writer);
    }
    return writer;
  }
}