/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.cmd;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.mrgeo.mapreduce.ColumnNormalizer;
import org.mrgeo.utils.HadoopUtils;

/**
 * 
 */
public class NormalizeColumns extends Configured implements Tool
{
  public static Options createOptions()
  {
    Options result = new Options();

    Option input = new Option("i", "input", true, "Input vector file name");
    input.setRequired(true);
    result.addOption(input);

    Option output = new Option("o", "output", true, "Output image file name");
    output.setRequired(true);
    result.addOption(output);

    return result;
  }

  public static void main(String[] args) throws Exception
  {
    int res = ToolRunner.run(HadoopUtils.createConfiguration(), new NormalizeColumns(), args);
    System.exit(res);
  }

  @Override
  public int run(String[] args) throws Exception
  {
    CommandLineParser parser = new PosixParser();
    Options options = NormalizeColumns.createOptions();
    CommandLine line = null;
    try
    {
      line = parser.parse(options, args);
    }
    catch (ParseException e)
    {
      System.out.println(e.getMessage());
      System.out.println();
      new HelpFormatter().printHelp("mrgeo NormalizeColumns", options);
      return 1;
    }

    String output = line.getOptionValue("o");
    String input = line.getOptionValue("i");

    try
    {
      ColumnNormalizer cn = new ColumnNormalizer();

      cn.run(new Path(input), new Path(output), null, null);
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }

    return 0;
  }
}
