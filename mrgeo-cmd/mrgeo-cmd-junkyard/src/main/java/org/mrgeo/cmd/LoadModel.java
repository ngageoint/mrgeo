/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.cmd;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.mrgeo.classifier.ConditionalProbabilityDataStore;
import org.mrgeo.utils.HadoopUtils;

/**
 * This class is custom made for the Wasabi project. Modifications must be made
 * for the general use case.
 */
public class LoadModel extends Configured implements Tool
{
  public static Options createOptions()
  {
    Options result = new Options();

    Option model = new Option("m", "model", true, "Model Path");
    model.setRequired(true);
    result.addOption(model);

    return result;
  }

  public static void main(String[] args) throws Exception
  {
    int res = ToolRunner.run(HadoopUtils.createConfiguration(), new LoadModel(), args);
    System.exit(res);
  }

  @SuppressWarnings("unused")
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
      new HelpFormatter().printHelp("mrgeo ClassifyNb", options);
      return 1;
    }

    try
    {
      new ConditionalProbabilityDataStore(line.getOptionValue("m"));
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }

    return 0;
  }
}
