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

package org.mrgeo.cmd.buildpyramid;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.joda.time.Period;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;
import org.mrgeo.aggregators.*;
import org.mrgeo.cmd.Command;
import org.mrgeo.cmd.MrGeo;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderNotFound;
import org.mrgeo.data.ProviderProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class BuildPyramid extends Command
{
private static Logger log = LoggerFactory.getLogger(BuildPyramid.class);

@Override
public String getUsage() { return "BuildPyramid <options> <input>"; }

@Override
public void addOptions(Options options)
{
  Option mean = new Option("m", "mean", false, "Mean (Average) Pixel Resampling Method");
  mean.setRequired(false);
  options.addOption(mean);

  Option sum = new Option("s", "sum", false, "Summing Pixel Resampling Method");
  sum.setRequired(false);
  options.addOption(sum);

  Option cat = new Option("c", "categorical", false, "Category (Mode) Pixel Resampling Method");
  cat.setRequired(false);
  options.addOption(cat);

  Option nearest = new Option("n", "nearest", false, "Nearest Pixel Resampling Method");
  nearest.setRequired(false);
  options.addOption(nearest);

  Option min = new Option("min", "minimum", false, "Minimum Pixel Resampling Method");
  min.setRequired(false);
  options.addOption(min);

  Option max = new Option("max", "maximum", false, "Maximum Pixel Resampling Method");
  max.setRequired(false);
  options.addOption(max);

  Option minavgpair =
      new Option("minavgpair", "miminumaveragepair", false, "Minimum Average Pair Pixel Resampling Method");
  minavgpair.setRequired(false);
  options.addOption(minavgpair);
}


@Override
@SuppressWarnings("squid:S1166") // Catching exceptions and logging error
public int run(CommandLine line, final Configuration conf,
    final ProviderProperties providerProperties) throws ParseException
{
  log.info("BuildPyramid");

  long start = System.currentTimeMillis();

  //if no arguments, print help
  if (line.getArgs().length == 0)
  {
    throw new ParseException("Missing input image name");
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
  for (String arg : line.getArgs())
  {
    input = arg;
  }

  log.info("Input image: " + input);

  if (input != null)
  {
    //if (!BuildPyramidDriver.build(input, aggregator, conf, providerProperties))

    // Validate that the user provided an image
    try
    {
      DataProviderFactory.getMrsImageDataProvider(input, DataProviderFactory.AccessMode.READ, providerProperties);
    }
    catch (DataProviderNotFound e)
    {
      System.out.println(input + " is not an image");
      return -1;
    }
    try
    {
      if (!org.mrgeo.buildpyramid.BuildPyramid.build(input, aggregator, conf, providerProperties))
      {
        log.error("BuildPyramid exited with error");
        return -1;
      }
    }
    catch (Exception e)
    {
      log.error("BuildPyramid exited with error", e);
      return -1;
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


}
