/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */

package org.mrgeo.cmd;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.mrgeo.utils.HadoopUtils;

/**
 * 
 */
public class SumPixels extends Configured implements Tool
{
  public static Options createOptions()
  {
    Options result = new Options();

    Option vector = new Option("v", "vector", true, "Input vector file name");
    vector.setRequired(true);
    result.addOption(vector);

    Option base = new Option("b", "base", true, "Base imagery");
    base.setRequired(true);
    result.addOption(base);

    Option output = new Option("o", "output", true, "Output path");
    output.setRequired(true);
    result.addOption(output);

    return result;
  }

  public static void main(String[] args) throws Exception
  {
    int res = ToolRunner.run(HadoopUtils.createConfiguration(), new SumPixels(), args);
    System.exit(res);
  }

  @Override
  public int run(String[] args) throws Exception
  {
    CommandLineParser parser = new PosixParser();
    Options options = createOptions();
    CommandLine line = null;
    try
    {
      line = parser.parse(options, args);
    }
    catch (ParseException e)
    {
      System.out.println(e.getMessage());
      System.out.println();
      new HelpFormatter().printHelp("mrgeo SumPixels", options);
      return 1;
    }
    
    new HelpFormatter().printHelp("mrgeo SumPixels", options);
    return 1;

//    Path vector = new Path(line.getOptionValue("v"));
//    Path base = new Path(line.getOptionValue("b"));
//    Path output = new Path(line.getOptionValue("o"));
//
//    try
//    {
//      BulkPixelSummer summer = new BulkPixelSummer();
//      summer.run(vector, base, output, null, null);
//    }
//    catch (Exception e)
//    {
//      e.printStackTrace();
//      return 1;
//    }
//
//    return 0;
  }
}
