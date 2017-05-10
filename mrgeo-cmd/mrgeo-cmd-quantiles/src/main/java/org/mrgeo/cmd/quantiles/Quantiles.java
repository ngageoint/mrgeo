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

package org.mrgeo.cmd.quantiles;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.joda.time.Period;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;
import org.mrgeo.cmd.Command;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderNotFound;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.data.image.MrsPyramidMetadataReader;
import org.mrgeo.image.MrsPyramidMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 *
 */
public class Quantiles extends Command
{
private static Logger log = LoggerFactory.getLogger(Quantiles.class);

@Override
public void addOptions(Options options)
{
  Option num = new Option("n", "numQuantiles", true, "The number of quantiles to compute");
  num.setRequired(false);
  options.addOption(num);

  Option fraction = new Option("f", "fraction", true, "The fraction of pixel values to sample. Must be between 0.0 - 1.0");
  fraction.setRequired(false);
  options.addOption(fraction);
}

@Override
public String getUsage() { return "quantiles <options> <input>"; }

@Override
@SuppressWarnings("squid:S1166") // Exception caught and error message printed
public int run(CommandLine line, final Configuration conf,
               final ProviderProperties providerProperties) throws ParseException
{
  log.info("quantiles");

  long start = System.currentTimeMillis();

  String input = null;
  for (String arg : line.getArgs())
  {
    input = arg;
  }

  log.info("Input image: " + input);

  if (input != null)
  {
    try
    {
      //if (!BuildPyramidDriver.build(input, aggregator, conf, providerProperties))

      // Validate that the user provided an image
      MrsImageDataProvider dp = null;
      try
      {
        dp = DataProviderFactory.getMrsImageDataProvider(input, DataProviderFactory.AccessMode.READ, providerProperties);
      }
      catch (DataProviderNotFound e)
      {
        throw new ParseException(input + " is not an image");
      }
      int numQuantiles = Integer.parseInt(line.getOptionValue("numQuantiles", "0"));
      if (numQuantiles > 0) {
        if (line.hasOption("fraction")) {
          float fraction = Float.parseFloat(line.getOptionValue("fraction"));
          if (!org.mrgeo.quantiles.Quantiles.compute(input, numQuantiles, fraction,
                  conf, providerProperties)) {
            log.error("Quantiles exited with error");
            return -1;
          }
        } else {
          if (!org.mrgeo.quantiles.Quantiles.compute(input, numQuantiles,
                  conf, providerProperties)) {
            log.error("Quantiles exited with error");
            return -1;
          }
        }
      }
      if (dp != null)  {
        MrsPyramidMetadataReader mdReader = dp.getMetadataReader();
        if (mdReader != null) {
          MrsPyramidMetadata metadata = mdReader.read();
          double[][] quantiles = metadata.getQuantiles();
          if (quantiles == null || quantiles.length == 0) {
            System.out.println("No quantiles have been computed for " + input);
            return 0;
          }
          boolean singleBand = (quantiles.length == 1);
          String indent = (singleBand) ? "" : "  ";
          boolean gotQuantiles = false;
          // Print the existing quantiles
          for (int b=0; b < quantiles.length; b++) {
            if (quantiles[b] != null) {
              if (quantiles.length > 1) {
                System.out.println("Band " + (b + 1));
                indent = "  ";
              }
              for (int q = 0; q < quantiles[b].length; q++) {
                System.out.println(indent + quantiles[b][q]);
              }
              gotQuantiles = true;
            }
          }
          if (!gotQuantiles) {
            System.out.println("No quantiles have been computed for " + input);
          }
        }
      }
    }
    catch (IOException e)
    {
      log.error("Quantiles exited with error", e);
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
