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

package org.mrgeo.cmd.mrsimageinfo;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.joda.time.format.DateTimeFormat;
import org.mrgeo.cmd.Command;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.DataProviderNotFound;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.data.image.MrsImagePyramidReaderContext;
import org.mrgeo.data.tile.MrsTileReader;
import org.mrgeo.image.ImageStats;
import org.mrgeo.image.MrsImagePyramid;
import org.mrgeo.pyramid.MrsPyramidMetadata;
import org.mrgeo.utils.Bounds;
import org.mrgeo.utils.LongRectangle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.io.IOException;
import java.text.DecimalFormat;

public class MrsImageInfo extends Command
{
  private static Logger log = LoggerFactory.getLogger(MrsImageInfo.class);

  private boolean verbose = false;
  private boolean debug = false;

  private Configuration config;

  public MrsImageInfo()
  {
  }

  public static Options createOptions()
  {
    final Options result = new Options();

    // Option output = new Option("o", "output", true, "Output directory");
    // output.setRequired(true);
    // result.addOption(output);

    result.addOption(new Option("v", "verbose", false, "Verbose"));
    result.addOption(new Option("d", "debug", false, "Debug (very verbose)"));

    return result;
  }

  public static String human(final long bytes)
  {
    final int unit = 1024;
    if (bytes < unit)
    {
      return bytes + "B";
    }
    final int exp = (int) (Math.log(bytes) / Math.log(unit));
    final char pre = new String("KMGTPE").charAt(exp - 1);

    return String.format("%.1f%sB", bytes / Math.pow(unit, exp), pre);
  }

  private static void printNodata(final MrsPyramidMetadata metadata)
  {
    System.out.print("NoData: ");
    for (int band = 0; band < metadata.getBands(); band++)
    {
      if (band > 0)
      {
        System.out.print(", ");
      }
      switch (metadata.getTileType())
      {
      case DataBuffer.TYPE_BYTE:
        System.out.print(metadata.getDefaultValueByte(band));
        break;
      case DataBuffer.TYPE_FLOAT:
        System.out.print(metadata.getDefaultValueFloat(band));
        break;
      case DataBuffer.TYPE_DOUBLE:
        System.out.print(metadata.getDefaultValueDouble(band));
        break;
      case DataBuffer.TYPE_INT:
        System.out.print(metadata.getDefaultValueInt(band));
        break;
      case DataBuffer.TYPE_SHORT:
      case DataBuffer.TYPE_USHORT:
        System.out.print(metadata.getDefaultValueShort(band));
        break;
      default:
        break;
      }
    }
    System.out.println("");
  }

  private static void printTileType(final MrsPyramidMetadata metadata)
  {
    System.out.print("Type: ");
    switch (metadata.getTileType())
    {
    case DataBuffer.TYPE_BYTE:
      System.out.println("byte");
      break;
    case DataBuffer.TYPE_FLOAT:
      System.out.println("float");
      break;
    case DataBuffer.TYPE_DOUBLE:
      System.out.println("double");
      break;
    case DataBuffer.TYPE_INT:
      System.out.println("int");
      break;
    case DataBuffer.TYPE_SHORT:
      System.out.println("short");
      break;
    case DataBuffer.TYPE_USHORT:
      System.out.println("unsigned short");
      break;
    default:
      break;
    }
  }

  @Override
  public int run(final String[] args, final Configuration conf,
      final ProviderProperties providerProperties)
  {
    log.info("MrsImageInfo");

    try
    {
      config = conf;

      final Options options = MrsImageInfo.createOptions();
      CommandLine line = null;

      try
      {
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
          new HelpFormatter().printHelp("MrsImageInfo <pyramid>", options);
          return 1;
        }

        final Path p = new Path(pyramidName);
        final FileSystem fs = p.getFileSystem(config);
        if (!fs.exists(p))
        {
          System.out.println("MrsImagePyramid does not exist: \"" + pyramidName + "\"");
          new HelpFormatter().printHelp("MrsImageInfo <pyramid>", options);
          return 1;
        }

        System.out.println("");
        System.out.println("");
        System.out.println("MrsImagePyramid Information");
        System.out.println("======================");

        final MrsImagePyramid pyramid = MrsImagePyramid.open(pyramidName, providerProperties);
        printMetadata(pyramid.getMetadata(), providerProperties);

        if (line.hasOption("v"))
        {
          printSplitPoints(pyramidName);
        }
      }
      catch (final ParseException e)
      {
        new HelpFormatter().printHelp("MrsImageInfo", options);
        return 1;
      }

      return 0;
    }
    catch (final Exception e)
    {
      e.printStackTrace();
    }

    return -1;
  }

  private void printFileInfo(final Path pfile)
  {
    // TODO: The following is HDFS-sepcific; needs to be re-factored
    try
    {
      final FileSystem fs = pfile.getFileSystem(config);
      final FileStatus stat = fs.getFileStatus(pfile);

      System.out.print("    date: " +
        DateTimeFormat.shortDateTime().print(stat.getModificationTime()));
      System.out.println("  size: " + human(stat.getLen()));

      final FsPermission p = stat.getPermission();

      if (debug)
      {
        System.out.print("    ");
        System.out.print(stat.isDir() ? "d" : "f");
        System.out.print(" u: " + stat.getOwner() + " (" +
          p.getUserAction().toString().toLowerCase() + ")");
        System.out.print(" g: " + stat.getGroup() + " (" +
          p.getGroupAction().toString().toLowerCase() + ")");
        System.out.print(" o: " + "(" + p.getOtherAction().toString().toLowerCase() + ")");

        System.out.print(" blk: " + human(stat.getBlockSize()));
        System.out.println(" repl: " + stat.getReplication());
      }
    }
    catch (final IOException e)
    {
      e.printStackTrace();
    }
  }

  private void printMetadata(final MrsPyramidMetadata metadata,
      final ProviderProperties providerProperties) throws DataProviderNotFound
  {
    System.out.println("name: \"" + metadata.getPyramid() + "\"");
    if (verbose)
    {
      printFileInfo(new Path(metadata.getPyramid()));
    }

    DecimalFormat df = new DecimalFormat(" ##0.00000000;-##0.00000000");

    final Bounds bounds = metadata.getBounds();
    System.out.println("");
    System.out.print("Bounds: (lon/lat)");
    System.out.println("  size (" + df.format(bounds.getWidth()) + ", " +
      df.format(bounds.getHeight()) + ")");
    System.out.print("  UL (" + df.format(bounds.getMinX()) + ", " + df.format(bounds.getMaxY()) +
      ")");
    System.out.println("  UR (" + df.format(bounds.getMaxX()) + ", " + df.format(bounds.getMaxY()) +
      ")");
    System.out.print("  LL (" + df.format(bounds.getMinX()) + ", " + df.format(bounds.getMinY()) +
      ")");
    System.out.println("  LR (" + df.format(bounds.getMaxX()) + ", " + df.format(bounds.getMinY()) +
      ")");
    System.out.println("");
    printTileType(metadata);
    System.out.println("Tile size: " + metadata.getTilesize());
    System.out.println("Bands: " + metadata.getBands());
    printNodata(metadata);
    System.out.println("Classification: " + metadata.getClassification().name());
    System.out.println("Resampling: " + metadata.getResamplingMethod());
    System.out.println("");
    for (int zoom = metadata.getMaxZoomLevel(); zoom >= 1; zoom--)
    {
      System.out.println("level " + zoom);
      System.out.println("  image: \"" + metadata.getPyramid() + " " +
          metadata.getName(zoom) + "\"");
      if (verbose)
      {
        printFileInfo(new Path(metadata.getPyramid(), metadata.getName(zoom)));
      }

      final LongRectangle pb = metadata.getPixelBounds(zoom);

      System.out.print("  width: " + pb.getWidth());
      System.out.print(" height: " + pb.getHeight());

      df = new DecimalFormat("0.00000000");

      System.out.print("  px w: " + df.format(metadata.getPixelWidth(zoom)));
      System.out.println(" px h: " + df.format(metadata.getPixelWidth(zoom)));

      final LongRectangle tb = metadata.getTileBounds(zoom);

      df = new DecimalFormat("#");

      System.out.print("  Tile Bounds: (x, y)");
      System.out.print("  size (" + df.format(tb.getWidth()) + ", " + df.format(tb.getHeight()) +
        ")");
      if (verbose)
      {
        MrsImageDataProvider dp = DataProviderFactory.getMrsImageDataProvider(metadata.getPyramid(),
            AccessMode.READ, providerProperties);
        MrsImagePyramidReaderContext context = new MrsImagePyramidReaderContext();
        context.setZoomlevel(zoom);
        MrsTileReader<Raster> reader;
        try
        {
          reader = dp.getMrsTileReader(context);
          System.out.println(" stored tiles: " + reader.calculateTileCount());
        }
        catch (IOException e)
        {
          System.out.println("Unable to get tile reader for: " + metadata.getPyramid());
        }
      }
      else
      {
        System.out.println();
      }
      if (tb.getWidth() == 1 && tb.getHeight() == 1)
      {
        System.out
          .println("    (" + df.format(tb.getMinX()) + ", " + df.format(tb.getMinY()) + ")");
      }
      else
      {
        System.out.print("    UL (" + df.format(tb.getMinX()) + ", " + df.format(tb.getMaxY()) +
          ")");
        System.out.println("  UR (" + df.format(tb.getMaxX()) + ", " + df.format(tb.getMaxY()) +
          ")");
        System.out.print("    LL (" + df.format(tb.getMinX()) + ", " + df.format(tb.getMinY()) +
          ")");
        System.out.println("  LR (" + df.format(tb.getMaxX()) + ", " + df.format(tb.getMinY()) +
          ")");
      }
      System.out.println("  ImageStats:");
      for (int b = 0; b < metadata.getBands(); b++)
      {
        System.out.print("    ");
        if (metadata.getBands() > 1)
        {
          System.out.print("band " + b + ": ");
        }

        df = new DecimalFormat("0.#");

        final ImageStats stats = metadata.getImageStats(zoom, b);
        System.out.println("min: " + df.format(stats.min) + " max: " + df.format(stats.max) +
          " mean: " + df.format(stats.mean) + " sum: " + df.format(stats.sum) + " count: " +
          df.format(stats.count));
      }

    }
    System.out.println("");
    System.out.println("");
  }

  private void printSplitPoints(final String pyramidName)
  {

  }

}
