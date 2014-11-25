/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.cmd;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.mrgeo.format.CsvOutputFormat;
import org.mrgeo.mapreduce.VectorSplitterDriver;
import org.mrgeo.utils.HadoopUtils;

/**
 * Reads vector data from one input splits on a specified column and writes to
 * one file per unique column value.
 */
public class SplitOnColumn extends Configured implements Tool
{
  public static Options createOptions()
  {
    Options result = new Options();

    Option input = new Option("i", "input", true, "Input vector file name");
    input.setRequired(true);
    result.addOption(input);

    Option output = new Option("o", "output", true, "Output vector file name");
    output.setRequired(true);
    result.addOption(output);

    Option column = new Option("c", "column", true, "Column to split on");
    column.setRequired(true);
    result.addOption(column);

    return result;
  }

  public static void main(String[] args) throws Exception
  {
    int res = ToolRunner.run(HadoopUtils.createConfiguration(), new SplitOnColumn(), args);
    System.exit(res);
  }

  @Override
  public int run(String[] args) throws Exception
  {
    CommandLineParser parser = new PosixParser();
    Options options = SplitOnColumn.createOptions();
    CommandLine line = null;
    try
    {
      line = parser.parse(options, args);
    }
    catch (ParseException e)
    {
      System.out.println(e.getMessage());
      System.out.println();
      new HelpFormatter().printHelp("mrgeo SplitOnColumn", options);
      return 1;
    }

    String output = line.getOptionValue("o");
    String input = line.getOptionValue("i");
    String column = line.getOptionValue("c");

    VectorSplitterDriver vs = new VectorSplitterDriver();

    vs.run(new Path(input), new Path(output), CsvOutputFormat.class, column, null, null);

    return 0;
  }
}
