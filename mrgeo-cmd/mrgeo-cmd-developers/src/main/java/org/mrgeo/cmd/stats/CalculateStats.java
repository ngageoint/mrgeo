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

package org.mrgeo.cmd.stats;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.mrgeo.cmd.Command;
import org.mrgeo.cmd.MrGeo;
import org.mrgeo.data.ProviderProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CalculateStats extends Command
{
private static Logger log = LoggerFactory.getLogger(CalculateStats.class);

private boolean verbose = false;
private boolean debug = false;


public CalculateStats()
{
}

public static Options createOptions()
{
  Options result = MrGeo.createOptions();

  return result;
}


@Override
public int run(final String[] args, final Configuration conf,
    final ProviderProperties providerProperties)
{
  log.info("CalculateStats");

  try
  {

    final Options options = CalculateStats.createOptions();
    CommandLine line;

    final CommandLineParser parser = new PosixParser();
    line = parser.parse(options, args);

    debug = line.hasOption("d");
    verbose = debug || line.hasOption("v");

    String pyramidName = null;
    for (final String arg : line.getArgs())
    {
      pyramidName = arg;
      break;
    }

    if (pyramidName == null)
    {
      new HelpFormatter().printHelp("calcstats <pyramid>", options);
      return 1;
    }

    StatsCalculator.calculate(pyramidName, conf, providerProperties);

    return 0;

  }
  catch (Exception e)
  {
    log.error("Exception thrown", e);
  }

  return -1;
}

}
