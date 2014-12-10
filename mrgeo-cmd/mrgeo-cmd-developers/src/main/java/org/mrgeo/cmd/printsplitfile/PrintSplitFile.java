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

package org.mrgeo.cmd.printsplitfile;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.mrgeo.cmd.Command;
import org.mrgeo.hdfs.tile.SplitFile;
import org.mrgeo.utils.TMSUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * A utility class to print split files on the command line
 * 
 * PrintSplitFile <split filename>
 **/

public class PrintSplitFile extends Command
{

  @Override
  public int run(final String[] args, final Configuration conf,
      final Properties providerProperties)
  {

    try
    {
      final CommandLineParser parser = new PosixParser();
      final CommandLine line = parser.parse(new Options(), args);

      final String splitFile = line.getArgs()[0];

      int index = 0;
      final List<Long> splits = new ArrayList<Long>();
      final List<String> partitions = new ArrayList<String>();
      final SplitFile sf = new SplitFile(conf);
      sf.readSplits(splitFile, splits, partitions);

      int zoomlevel = -1;

      final File f = new File(splitFile);
      final String dir = StringUtils.substringAfterLast(f.getParent(), "/");

      try
      {
        zoomlevel = Integer.parseInt(dir);
      }
      catch (final NumberFormatException e)
      {

      }

      System.out.println("Splits: " + splitFile);
      for (final long split : splits)
      {
        System.out.print("" + index + " " + split);
        if (zoomlevel > 0)
        {
          final TMSUtils.Tile tile = TMSUtils.tileid(split, zoomlevel);
          System.out.print(" tx: " + tile.tx + " ty: " + tile.ty);
        }

        try
        {
          System.out.print(" " + partitions.get(index));
        }
        catch (final IndexOutOfBoundsException e)
        {

        }
        index++;
        System.out.println();
      }

      if (partitions.size() > splits.size())
      {
        for (int i = splits.size(); i < partitions.size(); i++)
        {
          System.out.print("" + index + " (to end) " + partitions.get(index));
        }
        System.out.println();
      }
      return 0;
    }
    catch (final Exception e)
    {
      e.printStackTrace();
    }

    return -1;
  }
}
