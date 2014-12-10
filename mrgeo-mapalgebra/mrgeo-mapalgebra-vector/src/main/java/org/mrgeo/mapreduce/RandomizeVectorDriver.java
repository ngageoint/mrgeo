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

package org.mrgeo.mapreduce;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.mrgeo.format.AutoFeatureInputFormat;
import org.mrgeo.format.CsvOutputFormat;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.mapreduce.job.JobCancelledException;
import org.mrgeo.mapreduce.job.JobFailedException;
import org.mrgeo.mapreduce.job.JobListener;
import org.mrgeo.progress.Progress;
import org.mrgeo.utils.HadoopUtils;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

public class RandomizeVectorDriver
{
  private Class<? extends InputFormat<LongWritable, Geometry>> inputFormat = null;

  public void run(final Job job, final Path outputPath, final Progress progress,
      final JobListener jobListener)
      throws JobFailedException, JobCancelledException, IOException
  {
    // create a new unique job name
    final String now = new SimpleDateFormat("yyyy-MM-dd'T'HH-mm-ss").format(new Date());

    final String jobName = "RandomizeVector_" + now + "_" + UUID.randomUUID().toString();
    job.setJobName(jobName);

    HadoopUtils.setJar(job, this.getClass());

    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(GeometryWritable.class);
    job.setMapperClass(RandomizeVectorMapper.class);

    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Geometry.class);
    job.setReducerClass(GeometryWritableToGeometryReducer.class);
    job.setOutputFormatClass(CsvOutputFormat.class);

    FileOutputFormat.setOutputPath(job, outputPath);

    MapReduceUtils.runJob(job, progress, jobListener);
  }

  public void run(final Path input, final Path output, final Progress progress,
      final JobListener jobListener)
      throws IOException, JobFailedException, JobCancelledException
  {
    final Job job = new Job(HadoopUtils.createConfiguration());

    if (inputFormat == null)
    {
      inputFormat = AutoFeatureInputFormat.class;
    }
    job.setInputFormatClass(inputFormat);
    FileInputFormat.addInputPath(job, input);
    run(job, output, progress, jobListener);
  }
}
