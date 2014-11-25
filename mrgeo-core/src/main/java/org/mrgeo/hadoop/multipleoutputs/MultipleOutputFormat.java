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

//
// copied from Hadoop 1.1.2's "MultipleOutputs" class and delicately
// hacked
//
// taken from: https://github.com/paulhoule/infovore/tree/master/bakemono/src/main/java/com/ontology2/bakemono/mapred
// and renamed to MultipleOutputFormat
//

package org.mrgeo.hadoop.multipleoutputs;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.List;

public class MultipleOutputFormat<K, V> extends FileOutputFormat<K, V>
{
  private static final String base = MultipleOutputFormat.class.getSimpleName();
  private static final String ROOT_OUTPUT_FORMAT = base + ".rootOutputFormat";

  private FileOutputFormat<K, V> _innerFormat;

  private OutputCommitter _committer;

  @SuppressWarnings("rawtypes")
  public static void setRootOutputFormat(final Job job,
    final Class<? extends FileOutputFormat> theClass)
  {
    job.getConfiguration().setClass(ROOT_OUTPUT_FORMAT, theClass, FileOutputFormat.class);
  }

  @Override
  public synchronized OutputCommitter getOutputCommitter(final TaskAttemptContext context)
    throws IOException
  {
    if (_committer == null)
    {
      // insert list here? we should have enough in the context to construct the object states...
      final List<OutputCommitter> committers = Lists.newArrayList(super.getOutputCommitter(context));
      
      for (final String name : DirectoryMultipleOutputs.getNamedOutputsList(context))
      {
        String p = DirectoryMultipleOutputs.getHdfsPath(context, name);
        committers.add(new FileOutputCommitter(new Path(p), 
          DirectoryMultipleOutputs._getContext(context, name)));
      }

      _committer = new DirectoryMultipleOutputsCommitter(committers);
    }

    return _committer;
  }

  @Override
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public RecordWriter getRecordWriter(final TaskAttemptContext job) throws IOException,
    InterruptedException
  {

    return getRootOutputFormat(job).getRecordWriter(job);
  }

  public FileOutputFormat<K, V> getRootOutputFormat(final TaskAttemptContext job)
  {
    if (_innerFormat == null)
    {
      final Configuration conf = job.getConfiguration();
      final Class<?> c = conf.getClass(ROOT_OUTPUT_FORMAT, FileOutputFormat.class);
      try
      {
        _innerFormat = (FileOutputFormat<K, V>) c.newInstance();
      }
      catch (final InstantiationException e)
      {
        throw new RuntimeException(e);
      }
      catch (final IllegalAccessException e)
      {
        throw new RuntimeException(e);
      }
    }

    return _innerFormat;
  }
}
