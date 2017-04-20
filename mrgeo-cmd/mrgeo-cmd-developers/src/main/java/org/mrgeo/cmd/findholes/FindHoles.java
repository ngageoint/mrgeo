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

package org.mrgeo.cmd.findholes;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.mrgeo.cmd.Command;
import org.mrgeo.cmd.findholes.mapreduce.FindHolesDriver;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.utils.HadoopUtils;
import org.mrgeo.utils.logging.LoggingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * options are output directior and zoom level
 *
 * @author andrew
 */
public class FindHoles extends Command
{

private static Logger log = LoggerFactory.getLogger(FindHoles.class);
private int zoomLevel = -1;
private String out = null;
private String inputImage = null;

public FindHoles()
{
} // end constructor


@Override
public String getUsage() { return "findholes <options> <input>"; }

@Override
public void addOptions(Options options)
{
  Option output = new Option("o", "output", true, "MrsPyramid image name");
  output.setRequired(true);
  options.addOption(output);

  Option zoomLevel = new Option("z", "zoomlevel", true, "Zoom Level to check for the image");
  zoomLevel.setRequired(true);
  options.addOption(zoomLevel);

  Option lcl = new Option("l", "local-runner", false, "Use Hadoop's local runner (used for debugging)");
  lcl.setRequired(false);
  options.addOption(lcl);

  Option roles = new Option("r", "roles", true, "User roles used for access to data.");
  roles.setRequired(false);
  options.addOption(roles);

  options.addOption(new Option("v", "verbose", false, "Verbose logging"));
  options.addOption(new Option("d", "debug", false, "Debug (very verbose) logging"));
}

@Override
public int run(final CommandLine line, final Configuration conf,
    final ProviderProperties providerProperties) throws ParseException
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
    try
    {
      HadoopUtils.setupLocalRunner(conf);
    }
    catch (IOException ioe)
    {
      log.error("Exception thrown", ioe);
      return -1;
    }
  }


  String tmp = line.getOptionValue("z");
  zoomLevel = Integer.parseInt(tmp);
  out = line.getOptionValue("o");

  // DataProviderFactory.PROVIDER_PROPERTY_USER_ROLES
  ProviderProperties props = null;
  if (line.hasOption("r"))
  {
    props = new ProviderProperties("", line.getOptionValue("r"));
  }
  else
  {
    props = new ProviderProperties();
  }


  List<String> al = line.getArgList();
  System.out.print("Input:     ");
  for (String a : al)
  {
    System.out.print(a + " ");
  }
  System.out.println();
  System.out.println("Output:    " + out);
  System.out.println("ZoomLevel: " + zoomLevel);

  System.out.println();

  FindHolesDriver fhd = new FindHolesDriver();
  try
  {
    fhd.runJob(al.get(0), out, zoomLevel, props, conf);
  }
  catch (Exception e)
  {
    log.error("Exception thrown", e);
    return -1;
  }

  return 0;
}

}
