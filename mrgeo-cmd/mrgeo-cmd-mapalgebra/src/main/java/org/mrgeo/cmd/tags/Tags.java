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

package org.mrgeo.cmd.tags;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.mrgeo.cmd.Command;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.image.MrsPyramidMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

public class Tags extends Command
{
private static final Logger log = LoggerFactory.getLogger(Tags.class);

@Override
public String getUsage() { return "tags <options> [tag value tag value...]"; }

@Override
public void addOptions(Options options)
{
  Option input = new Option("i", "input", true, "MrsPyramid image name");
  input.setRequired(true);
  options.addOption(input);

  Option clear = new Option("c", "clear", false, "Clear all tags");
  clear.setRequired(false);
  options.addOption(clear);

  Option remove = new Option("r", "remove", false, "Remove following tags");
  remove.setRequired(false);
  options.addOption(remove);

  Option list = new Option("s", "show", false, "Show all tags");
  list.setRequired(false);
  options.addOption(list);

}

@Override
@SuppressWarnings("squid:S1166") // Exception caught and error message printed
public int run(CommandLine line, Configuration conf,
               ProviderProperties providerProperties) throws ParseException
{
  long starttime = System.currentTimeMillis();
  try
  {
    System.out.println(log.getClass().getName());

    boolean remove = line.hasOption("r");

    String input = line.getOptionValue("i");

    if (input == null)
    {
      throw new ParseException("Input must be specified.");
    }

    try
    {

      MrsImageDataProvider dp =
          DataProviderFactory.getMrsImageDataProvider(input, AccessMode.READ, providerProperties);

      MrsPyramidMetadata meta = dp.getMetadataReader().read();

      if (line.hasOption("s"))
      {
        System.out.println("Tags for " + input);
        for (Entry tags: meta.getTags().entrySet())
        {
          System.out.println("  " + tags.getKey() + ": " + tags.getValue());
        }
        return 0;
      }

      boolean pingpong = true;
      String tag = null;
      if (line.hasOption("c"))
      {
        meta.setTags(new HashMap<>());
      }
      for (String arg : line.getArgs())
      {
        if (remove)
        {
          meta.setTag(arg, null);
        }
        else if (pingpong)
        {
         tag = arg;
        }
        else
        {
          meta.setTag(tag, arg);
        }
        pingpong = !pingpong;
      }

      if (!pingpong && !remove)
      {
        System.out.println("Wrong number of tag/value pairs, not writing metadata");
        return -1;
      }

      System.out.println("Saving metadata for " + input);

      dp.getMetadataWriter().write(meta);

      return 0;

    }
    catch (IOException e)
    {
      System.out.println("Failure updating tags " + e.getMessage());
      return -1;
    }
  }
  finally
  {
    long elapsed = System.currentTimeMillis() - starttime;
    System.out.println("Elapsed time: " + time(elapsed));
  }
}

private String time(long millis)
{
  long second = (millis / 1000) % 60;
  long minute = (millis / (1000 * 60)) % 60;
  long hour = (millis / (1000 * 60 * 60)) % 24;

  return String.format("%02d:%02d:%02d", hour, minute, second) +
      String.format(".%-3d", millis % 1000).replace(' ', '0');

}
}

