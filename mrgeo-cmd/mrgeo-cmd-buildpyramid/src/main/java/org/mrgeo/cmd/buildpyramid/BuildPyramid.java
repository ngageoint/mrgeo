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
package org.mrgeo.cmd.buildpyramid;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.joda.time.Period;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;
import org.mrgeo.cmd.Command;
import org.mrgeo.aggregators.*;
import org.mrgeo.buildpyramid.BuildPyramidDriver;
import org.mrgeo.utils.HadoopUtils;
import org.mrgeo.utils.LoggingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * 
 */
public class BuildPyramid extends Command
{
  private static Logger log = LoggerFactory.getLogger(BuildPyramid.class);


  public static Options createOptions()
  {
    Options result = new Options();


    Option mean = new Option("m", "mean", false, "Mean (Average) Pixel Resampling Method");
    mean.setRequired(false);
    result.addOption(mean);

    Option sum = new Option("s", "sum", false, "Summing Pixel Resampling Method");
    sum.setRequired(false);
    result.addOption(sum);

    Option cat = new Option("c", "categorical", false, "Category (Mode) Pixel Resampling Method");
    cat.setRequired(false);
    result.addOption(cat);

    Option nearest = new Option("n", "nearest", false, "Nearest Pixel Resampling Method");
    nearest.setRequired(false);
    result.addOption(nearest);

    Option min = new Option("min", "minimum", false, "Minimum Pixel Resampling Method");
    min.setRequired(false);
    result.addOption(min);

    Option max = new Option("max", "maximum", false, "Maximum Pixel Resampling Method");
    max.setRequired(false);
    result.addOption(max);

    Option minavgpair = new Option("minavgpair", "miminumaveragepair", false, "Minimum Average Pair Pixel Resampling Method");
    minavgpair.setRequired(false);
    result.addOption(minavgpair);

    Option local = new Option("l", "local-runner", false, "Use Hadoop's local runner (used for debugging)");
    local.setRequired(false);
    result.addOption(local);

    result.addOption(new Option("v", "verbose", false, "Verbose logging"));
    result.addOption(new Option("d", "debug", false, "Debug (very verbose) logging"));

    Option old = new Option("o", "old-method", false, "Use old Map/Reduce method");
    old.setRequired(false);
    result.addOption(old);


    return result;
  }



  @Override
  public int run(String[] args, final Configuration conf,
      final Properties providerProperties)
  {
    log.info("BuildPyramid");

    try
    {
      long start = System.currentTimeMillis();

      Options options = BuildPyramid.createOptions();
      CommandLine line = null;

      try
      {
        //if no arguments, print help
        if (args.length == 0) throw new ParseException(null);
        CommandLineParser parser = new PosixParser();
        line = parser.parse(options, args);
      }
      catch (ParseException e)
      {
        new HelpFormatter().printHelp("BuildPyramid <input>", options);
        return -1;
      }

      if (line != null)
      {
        if (line.hasOption("v"))
        {
          LoggingUtils.setDefaultLogLevel(LoggingUtils.INFO);
        }
        if (line.hasOption("d"))
        {
          LoggingUtils.setDefaultLogLevel(LoggingUtils.DEBUG);
        }

        if (line.hasOption("l"))
        {
          System.out.println("Using local runner");
          HadoopUtils.setupLocalRunner(conf);
        }

        Aggregator aggregator = new MeanAggregator();
        if (line.hasOption("c"))
        {
          aggregator = new ModeAggregator();
        }
        else if (line.hasOption("s"))
        {
          aggregator = new SumAggregator();
        }
        else if (line.hasOption("n"))
        {
          aggregator = new NearestAggregator();
        }
        else if (line.hasOption("min"))
        {
          aggregator = new MinAggregator();
        }
        else if (line.hasOption("max"))
        {
          aggregator = new MaxAggregator();
        }
        else if (line.hasOption("minavgpair"))
        {
          aggregator = new MinAvgPairAggregator();
        }

        String input = null;
        for (String arg: line.getArgs())
        {
          input = arg;
        }

        log.info("Input image: " + input);

        if (input != null)
        {
          try
          {
            // TODO: Need to obtain provider properties
            if (!BuildPyramidDriver.build(input, aggregator, conf, providerProperties))
            {
              log.error("BuildPyramid exited with error");
              return -1;
            }
          }

          catch (Exception e)
          {
            e.printStackTrace();
            log.error("BuildPyramid exited with error");
            return -1;
          }
        }
      }

      long end = System.currentTimeMillis();
      long duration = end - start;
      PeriodFormatter formatter = new PeriodFormatterBuilder()
      .appendHours()
      .appendSuffix("h:")
      .appendMinutes()
      .appendSuffix("m:")
      .appendSeconds()
      .appendSuffix("s")
      .toFormatter();
      String formatted = formatter.print(new Period(duration));
      log.info("BuildPyramid completed in " + formatted);

      return 0;

    }
    catch (IOException e)
    {
      e.printStackTrace();
      return -1;
    }
  }


}
