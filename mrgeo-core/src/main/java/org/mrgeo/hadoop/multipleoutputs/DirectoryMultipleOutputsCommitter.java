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
// and renamed to DirectoryMultipleOutputsCommitter
//

package org.mrgeo.hadoop.multipleoutputs;

import com.google.common.collect.Lists;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;

public class DirectoryMultipleOutputsCommitter extends OutputCommitter
{

  public List<OutputCommitter> committers;

  //
  // TODO: Make the lists below immutable, widen List<> in constructor
  //

  public DirectoryMultipleOutputsCommitter(final List<OutputCommitter> committers)
  {
    this.committers = committers;
  }

  public DirectoryMultipleOutputsCommitter(final OutputCommitter outputCommitter)
  {
    this.committers = Lists.newArrayList(outputCommitter);
  }

  @Override
  public void abortTask(final TaskAttemptContext taskContext) throws IOException
  {
    for (final OutputCommitter that : committers)
    {
      that.abortTask(taskContext);
    }
  }

  @Override
  public void commitTask(final TaskAttemptContext taskContext) throws IOException
  {
    for (final OutputCommitter that : committers)
    {
      that.commitTask(taskContext);
    }
  }

  @Override
  public boolean needsTaskCommit(final TaskAttemptContext taskContext) throws IOException
  {
    for (final OutputCommitter that : committers)
    {
      if (that.needsTaskCommit(taskContext))
      {
        return true;
      }
    }

    return false;
  }

  @Override
  public void setupJob(final JobContext jobContext) throws IOException
  {
    for (final OutputCommitter that : committers)
    {
      that.setupJob(jobContext);
    }
  }

  @Override
  public void setupTask(final TaskAttemptContext taskContext) throws IOException
  {
    for (final OutputCommitter that : committers)
    {
      that.setupTask(taskContext);
    }
  }

  @Override
  public void commitJob(JobContext jobContext) throws IOException
  {
    for (final OutputCommitter that : committers)
    {
      that.commitJob(jobContext);
    }
    super.commitJob(jobContext);
  }

  @Override
  public void abortJob(JobContext jobContext, State state) throws IOException
  {
    for (final OutputCommitter that : committers)
    {
      that.abortJob(jobContext, state);
    }
    super.abortJob(jobContext, state);
  }

  
}
