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

package org.mrgeo.mapalgebra;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mrgeo.mapalgebra.old.MapOpHadoop;
import org.mrgeo.mapreduce.job.JobCancelledException;
import org.mrgeo.mapreduce.job.JobFailedException;
import org.mrgeo.progress.Progress;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.utils.HadoopUtils;

import java.io.IOException;

public abstract class VectorMapOpHadoop extends MapOpHadoop implements OutputProducer
{
  protected InputFormatDescriptor _output;
  protected String _outputName = null;

  /**
   * Returns the output object w/o blocking. This must be called after determineOutput has
   * completed.
   * 
   * @return
   */
  public InputFormatDescriptor getVectorOutput()
  {
    return _output;
  }

  /**
   * Returns the path to where this output is stored. This is _only_ relevant if the output has
   * already been calculated or it exists on disk for some other reason. If it doesn't exist and
   * needs to be calculated this method should return null. This is for optimization purposes only,
   * do not assume that this will be non-null.
   */
  @Override
  public String getOutputName()
  {
    return _outputName;
  }

  @Override
  public void setOutputName(final String output)
  {
    _outputName = output;
  }

  @Override
  public String resolveOutputName() throws IOException
  {
    if (_outputName == null)
    {
      _outputName = new Path(HadoopFileUtils.getTempDir(),
          HadoopUtils.createRandomString(40) + ".tsv").toString();
      addTempFile(_outputName);
      addTempFile(_outputName + ".columns");
    }
    return _outputName;
  }

  @Override
  public void postBuild(Progress p, boolean buildPyramid) throws IOException, JobFailedException,
      JobCancelledException
  {
//    AdHocDataProvider dp = DataProviderFactory.getAdHocDataProvider(_outputName,
//        AccessMode.WRITE);
//    if (dp != null)
//    {
//      dp.delete();
//    }
//    AdHocDataProvider dpColumns = DataProviderFactory.getAdHocDataProvider(_outputName + ".columns",
//        AccessMode.WRITE);
//    if (dpColumns != null)
//    {
//      dpColumns.delete();
//    }
//    InputFormatDescriptor result = getVectorOutput();
//    _writeVectorOutput(result, getConf(), p);
  }

  /**
   * After a map op chain is executed, moveOutput will be called on the root map op. By default, the
   * map op's output is stored in a location other than where its final resting place will be, and
   * this method is responsible for moving the content to that location (e.g. toPath).
   * 
   * @param toName
   * @throws IOException
   */
  @Override
  public void moveOutput(final String toName) throws IOException
  {
    HadoopFileUtils.delete(toName);
    final Path tmpOut = new Path(getOutputName());
    FileSystem fs = HadoopFileUtils.getFileSystem(tmpOut);
    // just move the file to the output path -- much faster.
    if (fs.rename(tmpOut, new Path(toName)) == false)
    {
      throw new IOException("Error moving temporary file to output path.");
    }
    _outputName = toName;
    final Path columns = new Path(tmpOut.toString() + ".columns");
    if (fs.exists(columns))
    {
      String toColumns = toName + ".columns";
      HadoopFileUtils.delete(toColumns);
      if (fs.rename(columns, new Path(toColumns)) == false)
      {
        throw new IOException("Error moving temporary columns file to output path.");
      }
    }
  }

  /**
   * Returns a clone of this MapOp and all its children. This will not include any intermediate
   * results or temporary variables involved in computation.
   */
  @Override
  public VectorMapOpHadoop clone()
  {
    final VectorMapOpHadoop result = (VectorMapOpHadoop) super.clone();
    result._output = null;
    return result;
  }

  private void _writeVectorOutput(InputFormatDescriptor descriptor, Configuration conf,
      Progress progress) throws IOException, JobFailedException, JobCancelledException
  {
    writeVectorOutput(descriptor, new Path(_outputName), conf, progress);
  }

  public static void writeVectorOutput(InputFormatDescriptor descriptor, Path outputPath,
      Configuration conf, Progress progress) throws IOException, JobFailedException,
      JobCancelledException
  {
//    if (Thread.currentThread().isInterrupted())
//    {
//      throw new JobCancelledException("Job cancelled by user.");
//    }
//
//    Job job = new Job(conf);
//    job.setJobName("Map Algebra Executor writing to " + outputPath.toString());
//
//    HadoopUtils.setJar(job);
//
//    job.setMapperClass(FeatureFilterMapper.class);
//    job.setMapOutputKeyClass(LongWritable.class);
//    job.setMapOutputValueClass(Feature.class);
//
//    job.setNumReduceTasks(0);
//
//    descriptor.populateJobParameters(job);
//
//    job.setOutputFormatClass(CsvOutputFormat.class);
//
//    FileOutputFormat.setOutputPath(job, outputPath);
//
//    MapReduceUtils.runJob(job, progress, null);
  }
}
