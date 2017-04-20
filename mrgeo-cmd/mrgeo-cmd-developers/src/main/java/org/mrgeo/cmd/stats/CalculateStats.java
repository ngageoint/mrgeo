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

public CalculateStats()
{
}

@Override
public String getUsage() { return "calcstats <pyramid>"; }

@Override
public void addOptions(Options options)
{
  // No additional options
}


@Override
public int run(final CommandLine line, final Configuration conf,
    final ProviderProperties providerProperties) throws ParseException
{
  log.info("CalculateStats");

  String pyramidName = null;
  for (final String arg : line.getArgs())
  {
    pyramidName = arg;
    break;
  }

  if (pyramidName == null)
  {
    throw new ParseException("Missing input pyramid");
  }
  StatsCalculator.calculate(pyramidName, conf, providerProperties);
  return 0;
}

}
