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

package org.mrgeo.mapreduce;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.vector.VectorDataProvider;
import org.mrgeo.data.vector.VectorInputFormat;
import org.mrgeo.data.vector.VectorInputFormatContext;
import org.mrgeo.data.vector.VectorInputFormatProvider;
import org.mrgeo.format.AutoFeatureInputFormat;
import org.mrgeo.format.CsvOutputFormat;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.mapreduce.job.JobCancelledException;
import org.mrgeo.mapreduce.job.JobFailedException;
import org.mrgeo.mapreduce.job.JobListener;
import org.mrgeo.progress.Progress;
import org.mrgeo.utils.HadoopUtils;

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

  public void run(final Configuration conf, final String input, final Path output,
      final Progress progress, final JobListener jobListener,
      Properties providerProperties)
      throws IOException, JobFailedException, JobCancelledException
  {
    final Job job = new Job(conf);

    VectorDataProvider dp = DataProviderFactory.getVectorDataProvider(input,
        AccessMode.READ, providerProperties);
    if (dp != null)
    {
      Set<String> inputs = new HashSet<String>(1);
      inputs.add(input);
      VectorInputFormatContext context = new VectorInputFormatContext(inputs,
          providerProperties);
      VectorInputFormatProvider ifp = dp.getVectorInputFormatProvider(context);
      ifp.setupJob(job, providerProperties);
      job.setInputFormatClass(VectorInputFormat.class);
      run(job, output, progress, jobListener);
    }
    else
    {
      // If we couldn't get a vector data provider for the input, then run the
      // old code for setting up the input format.
      if (inputFormat == null)
      {
        inputFormat = AutoFeatureInputFormat.class;
      }
      job.setInputFormatClass(inputFormat);
      FileInputFormat.addInputPath(job, new Path(input));
      run(job, output, progress, jobListener);
    }
  }
}
