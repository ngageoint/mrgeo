/*
 * Copyright 2009-2017. DigitalGlobe, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.mrgeo.cmd;

import org.apache.commons.cli.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.utils.HadoopUtils;
import org.mrgeo.utils.logging.LoggingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * The MrGeo class is the single point of entry for all mrgeo commands.
 */
public class MrGeo extends Configured implements Tool
{
private static Logger log = LoggerFactory.getLogger(MrGeo.class);
private static Map<String, CommandSpi> commands = null;

/**
 * This is the main method for executing mrgeo commands.  All commands come through this method.
 * <p/>
 * Instead of returning an integer denoting return status.  This method uses
 * {@link System#exit(int)} for the return status.
 *
 * @param args String[] Command line arguments
 */
public static void main(String[] args)
{
  Configuration conf = HadoopUtils.createConfiguration();

  int res = 0;
  try
  {
    res = ToolRunner.run(conf, new MrGeo(), args);
  }
  catch (Exception e)
  {
    log.error("Exception thrown", e);
    System.exit(-1);
  }

  System.exit(res);
}

/**
 * Create and return the options available as generic options for all commands.
 *
 * @return Options The generic {@link Options} for all commands.
 */
static Options createOptions()
{
  Options result = new Options();

  result.addOption(new Option("np", "no-persistance", false, "Disable Spark Autopersisting MrGeo RDDs"));

  result.addOption(new Option("l", "local-runner", false, "Use Hadoop & Spark's local runner (used for debugging)"));
  result.addOption(new Option("v", "verbose", false, "Verbose logging"));
  result.addOption(new Option("d", "debug", false, "Debug (very verbose) logging"));
  result.addOption(new Option("h", "help", false, "Display help for this command"));

  return result;
}

/**
 * Print generic usage to std out.
 */
private static void general_usage(Options options)
{
  System.out.println("Usage: mrgeo COMMAND");
  System.out.println("       where command is one of:");

  int maxLen = 0;
  for (String name : commands.keySet())
  {
    maxLen = Math.max(maxLen, name.length());
  }

  for (Map.Entry<String, CommandSpi> cmd : commands.entrySet())
  {
    String name = cmd.getKey();
    System.out.println("          " +
        StringUtils.rightPad(name, maxLen + 2) + cmd.getValue().getDescription());
  }

  System.out.println("Generic options supported are:");
  new HelpFormatter().printHelp("command <options>", options);
}

/**
 * Print generic usage to std out.
 */
private static void specific_usage(Command cmd, Options options)
{
  new HelpFormatter().printHelp("mrgeo " + cmd.getUsage(), options);
}

/**
 * Discover, load, and store all mrgeo commands (using {@link CommandSpi}).
 * Using Java's ServiceLoader, discover all service providers for commands, load them for
 * future use.
 */
private static void loadCommands()
{
  commands = new TreeMap<>();

  ServiceLoader<CommandSpi> loader = ServiceLoader.load(CommandSpi.class);

  for (CommandSpi cmd : loader)
  {
    commands.put(cmd.getCommandName(), cmd);
  }

}

/**
 * {@inheritDoc}
 *
 * @see org.apache.hadoop.util.Tool#run(String[])
 */
@Override
public int run(String[] args) throws IOException
{
  if (commands == null)
  {
    loadCommands();
  }

  Options options = createOptions();
  if (args.length == 0)
  {
    general_usage(options);
    return -1;
  }

  String cmdStr = args[0];

  if (cmdStr.equals("-h") || cmdStr.equals("--help")) {
    // If the user provided the -h switch, then the exit code should be 0
    general_usage(options);
    return 0;
  }

  CommandSpi spi = commands.get(cmdStr);
  Command cmd;
  if (spi == null) {
    System.out.println("Command not found: " + cmdStr);
    System.out.println();
    general_usage(options);
    return -1;
  }
  else {
    try {
      cmd = spi.getCommandClass().newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      log.error("Exception thrown", e);
      return -1;
    }
    cmd.addOptions(options);
  }

  CommandLine line;
  try
  {
    CommandLineParser parser = new ExtendedGnuParser(true);
    // The arguments used for parsing the command line should not include the
    // command name itself.
    line = parser.parse(options, Arrays.copyOfRange(args, 1, args.length));
  }
  catch (ParseException e)
  {
    if (args.length > 1)
    {
      for (String help: args)
      {
        if (help.equals("-h") || help.equals("--help"))
        {
          specific_usage(cmd, options);
          return 0;
        }
      }
    }

    System.out.println(e.getMessage());
    specific_usage(cmd, options);
    return -1;
  }

  // Process the command-line arguments
  if (line.hasOption("h"))
  {
    specific_usage(cmd, options);
    return 0;
  }
  else
  {
    if (line.hasOption("np"))
    {
      MrGeoProperties.getInstance().setProperty(MrGeoConstants.MRGEO_AUTOPERSISTANCE, "false");
    }

    if (line.hasOption("d"))
    {
      LoggingUtils.setDefaultLogLevel(LoggingUtils.DEBUG);
    }
    else if (line.hasOption("v"))
    {
      LoggingUtils.setDefaultLogLevel(LoggingUtils.INFO);
    }
    else
    {
      LoggingUtils.setDefaultLogLevel(LoggingUtils.ERROR);
      HadoopUtils.adjustLogging();
    }
  }

  if (line.hasOption("l"))
  {
    System.out.println("Using local runner");
    HadoopUtils.setupLocalRunner(getConf());
  }

  if (line.hasOption("mm"))
  {
    float mult = Float.parseFloat(line.getOptionValue("mm"));
    MrGeoProperties.getInstance().setProperty(MrGeoConstants.MRGEO_FORCE_MEMORYINTENSIVE, "true");
    MrGeoProperties.getInstance().setProperty(MrGeoConstants.MRGEO_MEMORYINTENSIVE_MULTIPLIER, Float.toString(mult));
  }

  if (line.hasOption("mem"))
  {
    MrGeoProperties.getInstance().setProperty(MrGeoConstants.MRGEO_MAX_PROCESSING_MEM, line.getOptionValue("mem"));
  }

  if (line.hasOption("sf"))
  {
    MrGeoProperties.getInstance().setProperty(MrGeoConstants.MRGEO_SHUFFLE_FRACTION, line.getOptionValue("sf"));
  }

  if (!commands.containsKey(cmdStr))
  {
    int ret = 0;
    if (!line.hasOption("h"))
    {
      System.out.println("Command not found: " + cmdStr);
      System.out.println();
      ret = -1;
    }

    general_usage(options);
    return ret;
  }

  ProviderProperties providerProperties = new ProviderProperties();
  try {
    return cmd.run(line, getConf(), providerProperties);
  }
  catch(ParseException e) {
    System.out.println(e.getMessage());
    new HelpFormatter().printHelp(cmd.getUsage(), options);
    return -1;
  }
}

public static class ExtendedGnuParser extends GnuParser
{

  private boolean ignoreUnrecognizedOption;

  ExtendedGnuParser(final boolean ignoreUnrecognizedOption)
  {
    this.ignoreUnrecognizedOption = ignoreUnrecognizedOption;
  }

  @Override
  protected void processOption(final String arg, final ListIterator iter) throws ParseException
  {
    boolean hasOption = getOptions().hasOption(arg);

    if (hasOption || !ignoreUnrecognizedOption)
    {
      super.processOption(arg, iter);
    }
  }

}

}
