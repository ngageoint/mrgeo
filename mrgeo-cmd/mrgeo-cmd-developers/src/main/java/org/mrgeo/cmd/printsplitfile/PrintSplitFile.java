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

package org.mrgeo.cmd.printsplitfile;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.mrgeo.cmd.Command;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderNotFound;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.hdfs.image.HdfsMrsImageDataProvider;
import org.mrgeo.hdfs.tile.FileSplit;
import org.mrgeo.hdfs.tile.SplitInfo;
import org.mrgeo.pyramid.MrsPyramidMetadata;

import java.io.IOException;

/**
 * A utility class to print split files on the command line
 *
 * PrintSplitFile <split filename>
 **/

public class PrintSplitFile extends Command
{

  public static Options createOptions()
  {
    final Options result = new Options();

    final Option zoom = new Option("z", "zoom", true, "Zoom level");
    zoom.setRequired(false);
    result.addOption(zoom);

    return result;
  }

  @Override
  public int run(final String[] args, final Configuration conf,
      final ProviderProperties providerProperties)
  {

    try
    {
      final Options options = PrintSplitFile.createOptions();
      CommandLine line;
      final CommandLineParser parser = new PosixParser();
      line = parser.parse(options, args);

      int zoomlevel = -1;
      if (line.hasOption("z"))
      {
        zoomlevel = Integer.valueOf(line.getOptionValue("z"));
      }

      for (final String name : line.getArgs())
      {
        MrsImageDataProvider dp =
            DataProviderFactory.getMrsImageDataProvider(name, DataProviderFactory.AccessMode.READ,
                conf);

        if (!(dp instanceof HdfsMrsImageDataProvider))
        {
          System.out.println("PrintSplitFile only works on HDFS images");
          return -1;
        }

        MrsPyramidMetadata metadata = dp.getMetadataReader().read();

        if (zoomlevel > 0)
        {
          for (int i = metadata.getMaxZoomLevel(); i > 0; i--)
          {
            System.out.println("Zoom level: " + i);
            printlevel(dp, i);
            System.out.println("---------------");
          }
        }
        else
        {
          printlevel(dp, zoomlevel);
        }
      }
    }
    catch (DataProviderNotFound | ParseException dataProviderNotFound)
    {
      dataProviderNotFound.printStackTrace();
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }

    return 0;
  }

  private void printlevel(MrsImageDataProvider dp, int zoomlevel) throws IOException
  {
    Path parent = new Path(new Path(dp.getResourceName()), "" + zoomlevel);
    FileSplit fs = new FileSplit();

    String splitname = fs.findSplitFile(parent);
    System.out.println("split file: " + splitname);

    fs.readSplits(parent);
    System.out.println("min\tmax\tpartition\tname");

    SplitInfo[] splits = fs.getSplits();
    for (SplitInfo split: splits)
    {
      System.out.println(((FileSplit.FileSplitInfo) split).getStartId());
      System.out.println("\t");
      System.out.println(((FileSplit.FileSplitInfo) split).getEndId());
      System.out.println("\t");
      System.out.println(((FileSplit.FileSplitInfo) split).getName());
      System.out.println("\t");
      System.out.println(split.getPartition());
    }

  }
}
