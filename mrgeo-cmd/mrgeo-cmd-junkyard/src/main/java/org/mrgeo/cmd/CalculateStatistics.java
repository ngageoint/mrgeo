/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.cmd;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.mrgeo.classifier.CalculateStatisticsDriver;
import org.mrgeo.rasterops.OpImageRegistrar;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.utils.HadoopUtils;

/**
 */
public class CalculateStatistics extends Configured implements Tool
{
  public static Options createOptions()
  {
    Options result = new Options();

    Option input = new Option("i", "input", true, "Input TSV directory");
    input.setRequired(true);
    result.addOption(input);
    result.addOption(new Option("q", "quartiles", false, "Compute quartiles"));

    return result;
  }

  public static void main(String[] args) throws Exception
  {
    OpImageRegistrar.registerMrGeoOps();
    int res = ToolRunner.run(HadoopUtils.createConfiguration(), new CalculateStatistics(), args);
    System.exit(res);
  }

  @Override
  public int run(String[] args) throws Exception
  {
    CommandLineParser parser = new PosixParser();
    Options options = CalculateStatistics.createOptions();
    CommandLine line = null;
    try
    {
      line = parser.parse(options, args);
    }
    catch (ParseException e)
    {
      System.out.println(e.getMessage());
      System.out.println();
      new HelpFormatter().printHelp("mrgeo CalculateStatistics", options);
      return 1;
    }

    try
    {
      FileSystem fs = HadoopFileUtils.getFileSystem(getConf());
      Path inputTsv = new Path(line.getOptionValue("input")).makeQualified(fs);
      boolean computeQuartiles = line.hasOption("q");
      CalculateStatisticsDriver.runJob(new Path[] { inputTsv }, computeQuartiles, null);
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }

    return 0;
  }
}
