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
import org.mrgeo.utils.LoggingUtils;

import java.io.IOException;
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
  new HelpFormatter().printHelp("ingest <options> <input>", createOptions());
//    System.out.println("  -l    verbose logging");
//    System.out.println("  -mm   memory multiplier (spark only), multiple of the \"yarn.scheduler.minimum-allocation-mb\"");
//    System.out.println("        parameter to allocate east worker in a spark job.  This parameter overrides the");
//    System.out.println("        setting in mrgeo.conf");
//    System.out.println("  -v    verbose logging");
//    System.out.println("  -d    debug (very verbose) logging");
//    System.out.println("Most commands print help when invoked without parameters.");
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

//  LoggingUtils.setDefaultLogLevel(LoggingUtils.WARN);
//  try
//  {
//    Path base = new Path("s3://mrgeo/images/aster-30m/12");
//
//    FileSystem fs = base.getFileSystem(conf);
//
//    TileIdWritable first = new TileIdWritable();
//    TileIdWritable last = new TileIdWritable();
//    RasterWritable raster = new RasterWritable();
//
//    for (int i = 0; i < 1889; i++)
//    {
//      Path file = new Path(base, String.format("part-r-%05d", i));
//
//      System.out.println("Opening reader " + i + " (" + file + ")");
//
//      MapFile.Reader reader = new MapFile.Reader(file, conf);
//      // read the 1st and last keys
//      reader.finalKey(last);
//      reader.reset();
//      reader.next(first, raster);
//
//      if (last.get() < first.get())
//      {
//        System.out.println("Something wen't wrong!  Last (" +
//            last.get() + ") is greater than first (" + first.get() + ")");
//      }
//
//      // don't close the readers on purpose!
//    }
//  }
//  catch (Exception e)
//  {
//    e.printStackTrace();
//  }
//
//  System.exit(1);

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
public static Options createOptions()
{
  Options result = new Options();

  Option mm = new Option("mm", "memory-multiplier", true, "memory multiplier (spark only), " +
      "multiple of the \"yarn.scheduler.minimum-allocation-mb\" parameter to allocate each worker " +
      "in a spark job.  This parameter overrides the setting in mrgeo.conf");
  mm.setRequired(false);
  result.addOption(mm);

  result.addOption(new Option("l", "local-runner", false, "Use Hadoop & Spark's local runner (used for debugging)"));
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
public int run(String[] args) throws IOException
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

  CommandLine line;
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

  if (line == null)
  {
    usage();
    return 0;
  }
  else
  {
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
      LoggingUtils.setDefaultLogLevel(LoggingUtils.WARN);
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

  String cmdStr = args[0];

  if (!commands.containsKey(cmdStr))
  {
    int ret = 0;
    if (!line.hasOption("h"))
    {
      System.out.println("Command not found: " + cmdStr);
      System.out.println();
      ret = -1;
    }

    usage();
    return ret;
  }

  CommandSpi spi = commands.get(cmdStr);
  try
  {
    Command cmd = spi.getCommandClass().newInstance();

    // strip the 1st argument (the command name) and pass the rest to the command
      ProviderProperties providerProperties = new ProviderProperties();
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
