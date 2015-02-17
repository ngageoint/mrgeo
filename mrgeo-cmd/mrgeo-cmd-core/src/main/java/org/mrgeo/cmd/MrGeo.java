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

package org.mrgeo.cmd;

import org.apache.commons.cli.*;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.mrgeo.utils.HadoopUtils;
import org.mrgeo.utils.LoggingUtils;

import java.util.*;

/**
 * The MrGeo class is the single point of entry for all mrgeo commands.
 */
public class MrGeo extends Configured implements Tool
{
  //private static Logger log = LoggerFactory.getLogger(MrGeo.class);
  private static Map<String, CommandSpi> commands = null;

  /**
   * Print generic usage to std out.
   */
  private static void usage()
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
      System.out.println("  " +
          StringUtils.rightPad(name, maxLen + 2) + cmd.getValue().getDescription());
    }

    System.out.println("Generic options supported are:");
    System.out.println("  -v    verbose logging");
    System.out.println("  -d    debug (very verbose) logging");
    System.out.println("Most commands print help when invoked without parameters.");
  }

  /**
   * Discover, load, and store all mrgeo commands (using {@link CommandSpi}).
   * Using Java's ServiceLoader, discover all service providers for commands, load them for
   * future use.
   */
  private static void loadCommands()
  {
    commands = new TreeMap<String, CommandSpi>();

    ServiceLoader<CommandSpi> loader = ServiceLoader.load(CommandSpi.class);

    for (CommandSpi cmd : loader)
    {
      commands.put(cmd.getCommandName(), cmd);
    }

  }

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
      e.printStackTrace();
      System.exit(-1);
    }

    System.exit(res);
  }

  /**
   * Create and return the options available as generic options for all commands.
   *
   * @return Options The generic {@link Options} for all commands.
   */
  private static Options createOptions()
  {
    Options result = new Options();

    result.addOption(new Option("v", "verbose", false, "Verbose logging"));
    result.addOption(new Option("d", "debug", false, "Debug (very verbose) logging"));
    result.addOption(new Option("h", "help", false, "Display help for this command"));

    return result;
  }

  /**
   * {@inheritDoc}
   *
   * @see org.apache.hadoop.util.Tool#run(String[])
   */
  @Override
  public int run(String[] args)
  {
    if (commands == null)
    {
      loadCommands();
    }

    if (args.length == 0)
    {
      usage();
      return -1;
    }

    Options options = createOptions();

    CommandLine line = null;
    try
    {
      CommandLineParser parser = new ExtendedGnuParser(true);
      line = parser.parse(options, args);
    }
    catch (ParseException e)
    {
      usage();
      return -1;
    }

    if (line == null || line.hasOption("-h"))
    {
      usage();
      return 0;
    }
    else
    {
      if (line.hasOption("-v"))
      {
        LoggingUtils.setDefaultLogLevel(LoggingUtils.INFO);
      }
      else if (line.hasOption("-d"))
      {
        LoggingUtils.setDefaultLogLevel(LoggingUtils.DEBUG);
      }
      else
      {
        LoggingUtils.setDefaultLogLevel(LoggingUtils.WARN);
      }
    }


    String cmdStr = args[0];

    if (!commands.containsKey(cmdStr))
    {
      System.out.println("Command not found: " + cmdStr);
      System.out.println();
      usage();
      return -1;
    }

    CommandSpi spi = commands.get(cmdStr);
    try
    {
      Command cmd = spi.getCommandClass().newInstance();

      // strip the 1st argument (the command name) and pass the rest to the command
      Properties providerProperties = new Properties();
      return cmd.run(Arrays.copyOfRange(args, 1, args.length), getConf(), providerProperties);
    }
    catch (Exception e)
    {
      return -1;
    }

  }

  public class ExtendedGnuParser extends GnuParser
  {

    private boolean ignoreUnrecognizedOption;

    public ExtendedGnuParser(final boolean ignoreUnrecognizedOption)
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
