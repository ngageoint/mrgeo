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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.mrgeo.format.CsvOutputFormat;
import org.mrgeo.format.PgQueryInputFormat;
import org.mrgeo.mapreduce.job.JobCancelledException;
import org.mrgeo.mapreduce.job.JobFailedException;
import org.mrgeo.mapreduce.job.JobListener;
import org.mrgeo.progress.Progress;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.utils.HadoopUtils;
import org.mrgeo.utils.HadoopVectorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class PgQueryDriver
{
  @SuppressWarnings("unused")
  private static final Logger _log = LoggerFactory.getLogger(PgQueryDriver.class);

  String _username;
  String _password;
  String _dbconnection;
 
  public void run(Configuration conf, Path sqlPath, Path outputPath, Progress progress,
      JobListener jobListener) throws IOException, JobFailedException, JobCancelledException
  {
    Job job = new Job(conf);
    job.setJobName(String.format("PgQuery %s", sqlPath.toString()));

    HadoopUtils.setJar(job, this.getClass());
  
    // create a corresponding tmp directory for the job
    Path tmp = HadoopFileUtils.createJobTmp();
    FileSystem fs = HadoopFileUtils.getFileSystem(tmp);
    if (fs.exists(tmp) == false)
    {
      fs.mkdirs(tmp);
    }

    try
    {
      HadoopVectorUtils.setupPgQueryInputFormat(job, _username, _password, _dbconnection);
      job.setInputFormatClass(PgQueryInputFormat.class);
      
      job.setMapperClass(FeatureFilterMapper.class);
      job.setMapOutputKeyClass(LongWritable.class);
      job.setMapOutputValueClass(GeometryWritable.class);


      FileInputFormat.setInputPaths(job, sqlPath);

      job.setNumReduceTasks(0);

      job.setReducerClass(GeometryWritableToGeometryReducer.class);
      job.setOutputFormatClass(CsvOutputFormat.class);

      FileOutputFormat.setOutputPath(job, outputPath);

      MapReduceUtils.runJob(job, progress, jobListener);
    }
    finally
    {
      fs.delete(tmp, true);
    }
  }

  public void setUsername(String username)
  {
    _username = username;
  }
  
  public void setPassword(String password)
  {
    _password = password;
  }
  
  public void setDbConnection(String dbconnection)
  {
    _dbconnection = dbconnection;
  }
}
