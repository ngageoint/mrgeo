/*
 * Copyright 2009-2016 DigitalGlobe, Inc.
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
 *
 */

package org.mrgeo.cmd.quantiles;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.joda.time.Period;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;
import org.mrgeo.aggregators.*;
import org.mrgeo.cmd.Command;
import org.mrgeo.cmd.MrGeo;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderNotFound;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.utils.SparkUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 *
 */
public class Quantiles extends Command
{
private static Logger log = LoggerFactory.getLogger(Quantiles.class);


public static Options createOptions()
{
  Options result = MrGeo.createOptions();

  Option mean = new Option("n", "numQuantiles", true, "The number of quantiles to compute");
  mean.setRequired(true);
  result.addOption(mean);

  Option sum = new Option("f", "fraction", true, "The fraction of pixel values to sample. Must be between 0.0 - 1.0");
  sum.setRequired(false);
  result.addOption(sum);

  return result;
}



@Override
@SuppressWarnings("squid:S1166") // Exception caught and error message printed
public int run(String[] args, final Configuration conf,
    final ProviderProperties providerProperties)
{
  log.info("quantiles");

  long start = System.currentTimeMillis();

  Options options = Quantiles.createOptions();
  CommandLine line;

  try
  {
    //if no arguments, print help
    if (args.length == 0) throw new ParseException(null);
    CommandLineParser parser = new PosixParser();
    line = parser.parse(options, args);
  }
  catch (ParseException e)
  {
    new HelpFormatter().printHelp("quantiles <options> <input>", options);
    return -1;
  }

  if (line == null || line.hasOption("h"))
  {
    new HelpFormatter().printHelp("quantiles <options> <input>", options);
    return -1;
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
      //if (!BuildPyramidDriver.build(input, aggregator, conf, providerProperties))

      // Validate that the user provided an image
      try
      {
        DataProviderFactory.getMrsImageDataProvider(input, DataProviderFactory.AccessMode.READ, providerProperties);
      }
      catch (DataProviderNotFound e) {
        log.error(input + " is not an image");
        return -1;
      }
      Path outputPath = HadoopFileUtils.createUniqueTmpPath(conf);
      try
      {
        int numQuantiles = Integer.parseInt(line.getOptionValue("numQuantiles"));
        if (line.hasOption("fraction")) {
          float fraction = Float.parseFloat(line.getOptionValue("fraction"));
          if (!org.mrgeo.quantiles.Quantiles.compute(input, outputPath.toString(),
              numQuantiles, fraction, conf, providerProperties))
          {
            log.error("Quantiles exited with error");
            return -1;
          }
          printResults(outputPath, conf);
        }
        else {
          if (!org.mrgeo.quantiles.Quantiles.compute(input, outputPath.toString(),
              numQuantiles, conf, providerProperties))
          {
            log.error("Quantiles exited with error");
            return -1;
          }
          printResults(outputPath, conf);
        }
      }
      finally {
        HadoopFileUtils.delete(conf, outputPath);
      }
    }
    catch (IOException e)
    {
      e.printStackTrace();
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

private void printResults(Path outputPath, Configuration conf) throws IOException
{
  InputStream is = HadoopFileUtils.open(conf, outputPath);
  BufferedReader br = new BufferedReader(new InputStreamReader(is));
  try {
    String line = br.readLine();
    while (line != null) {
      System.out.println(line);
      line = br.readLine();
    }
  }
  finally {
    br.close();
  }
}

}
