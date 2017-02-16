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

package org.mrgeo.cmd.mrsimageinfo;

import org.apache.commons.cli.*;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.joda.time.format.DateTimeFormat;
import org.mrgeo.cmd.Command;
import org.mrgeo.cmd.MrGeo;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.DataProviderNotFound;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.data.image.MrsImageReader;
import org.mrgeo.data.image.MrsPyramidReaderContext;
import org.mrgeo.image.ImageStats;
import org.mrgeo.image.MrsPyramid;
import org.mrgeo.image.MrsPyramidMetadata;
import org.mrgeo.utils.LongRectangle;
import org.mrgeo.utils.tms.Bounds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.image.DataBuffer;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
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
  Options result = MrGeo.createOptions();

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
  final char pre = "KMGTPE".charAt(exp - 1);

  return String.format("%.1f%sB", bytes / Math.pow(unit, exp), pre);
}

private static void printNodata(final MrsPyramidMetadata metadata, PrintStream out)
{
  out.print("NoData: ");
  for (int band = 0; band < metadata.getBands(); band++)
  {
    if (band > 0)
    {
      out.print(", ");
    }
    switch (metadata.getTileType())
    {
    case DataBuffer.TYPE_BYTE:
      out.print(metadata.getDefaultValueByte(band));
      break;
    case DataBuffer.TYPE_FLOAT:
      out.print(metadata.getDefaultValueFloat(band));
      break;
    case DataBuffer.TYPE_DOUBLE:
      out.print(metadata.getDefaultValueDouble(band));
      break;
    case DataBuffer.TYPE_INT:
      out.print(metadata.getDefaultValueInt(band));
      break;
    case DataBuffer.TYPE_SHORT:
    case DataBuffer.TYPE_USHORT:
      out.print(metadata.getDefaultValueShort(band));
      break;
    default:
      break;
    }
  }
  out.println("");
}

private static void printTileType(final MrsPyramidMetadata metadata, PrintStream out)
{
  out.print("Type: ");
  switch (metadata.getTileType())
  {
  case DataBuffer.TYPE_BYTE:
    out.println("byte");
    break;
  case DataBuffer.TYPE_FLOAT:
    out.println("float");
    break;
  case DataBuffer.TYPE_DOUBLE:
    out.println("double");
    break;
  case DataBuffer.TYPE_INT:
    out.println("int");
    break;
  case DataBuffer.TYPE_SHORT:
    out.println("short");
    break;
  case DataBuffer.TYPE_USHORT:
    out.println("unsigned short");
    break;
  default:
    break;
  }
}

@Override
@SuppressWarnings("squid:S1166") // DataProviderNotFound exception caught and message printed
public int run(final String[] args, final Configuration conf,
    final ProviderProperties providerProperties)
{
  log.info("MrsImageInfo");

  try
  {
    config = conf;

    final Options options = MrsImageInfo.createOptions();
    CommandLine line;

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

//      final Path p = new Path(pyramidName);
//      final FileSystem fs = p.getFileSystem(config);
//      if (!fs.exists(p))
//      {
//        out.println("MrsPyramid does not exist: \"" + pyramidName + "\"");
//        new HelpFormatter().printHelp("MrsImageInfo <pyramid>", options);
//        return 1;
//      }

      try (ByteArrayOutputStream baos = new ByteArrayOutputStream())
      {
        try (PrintStream out = new PrintStream(baos))
        {
          try
          {
            final MrsImageDataProvider dp =
                DataProviderFactory.getMrsImageDataProviderNoCache(pyramidName, AccessMode.READ, providerProperties);


            out.println("");
            out.println("");
            out.println("MrsPyramid Information");
            out.println("======================");

            final MrsPyramid pyramid = MrsPyramid.open(dp);

            //final MrsPyramid pyramid = MrsPyramid.open(pyramidName, providerProperties);
            printMetadata(pyramid.getMetadata(), providerProperties, out);

            if (line.hasOption("v"))
            {
              printSplitPoints(pyramidName, out);
            }
          }
          catch (final DataProviderNotFound dpnf)
          {
            out.println("MrsPyramid does not exist: \"" + pyramidName + "\"");
            new HelpFormatter().printHelp("MrsImageInfo <pyramid>", options);
            return 1;
          }
          System.out.println(new String(baos.toByteArray(), StandardCharsets.UTF_8));
        }
      }
    }
    catch (final ParseException e)
    {
      new HelpFormatter().printHelp("MrsImageInfo", options);
      return 1;
    }

    return 0;

  }
  catch (IOException e)
  {
    log.error("Exception thrown", e);
  }

  return -1;
}

private void printFileInfo(final Path pfile, PrintStream out) throws IOException
{
  // TODO: The following is HDFS-sepcific; needs to be re-factored
  final FileSystem fs = pfile.getFileSystem(config);
  final FileStatus stat = fs.getFileStatus(pfile);

  out.print("    date: " +
      DateTimeFormat.shortDateTime().print(stat.getModificationTime()));
  out.println("  size: " + human(stat.getLen()));

  final FsPermission p = stat.getPermission();

  if (debug)
  {
    out.print("    ");
    out.print(stat.isDir() ? "d" : "f");
    out.print(" u: " + stat.getOwner() + " (" +
        p.getUserAction().toString().toLowerCase() + ")");
    out.print(" g: " + stat.getGroup() + " (" +
        p.getGroupAction().toString().toLowerCase() + ")");
    out.print(" o: " + "(" + p.getOtherAction().toString().toLowerCase() + ")");

    out.print(" blk: " + human(stat.getBlockSize()));
    out.println(" repl: " + stat.getReplication());
  }
}

@SuppressWarnings("squid:S1166") // Exception caught and handled
private void printMetadata(final MrsPyramidMetadata metadata,
    final ProviderProperties providerProperties, PrintStream out) throws IOException
{
  out.println("name: \"" + metadata.getPyramid() + "\"");
  if (verbose)
  {
    printFileInfo(new Path(metadata.getPyramid()), out);
  }

  DecimalFormat df = new DecimalFormat(" ##0.00000000;-##0.00000000");

  final Bounds bounds = metadata.getBounds();
  out.println("");
  out.print("Bounds: (lon/lat)");
  out.println("  pixel size (" + df.format(bounds.width()) + ", " +
      df.format(bounds.height()) + ")");
  out.print("  UL (" + df.format(bounds.w) + ", " + df.format(bounds.n) +
      ")");
  out.println("  UR (" + df.format(bounds.e) + ", " + df.format(bounds.n) +
      ")");
  out.print("  LL (" + df.format(bounds.w) + ", " + df.format(bounds.s) +
      ")");
  out.println("  LR (" + df.format(bounds.e) + ", " + df.format(bounds.s) +
      ")");
  out.println("");
  printTileType(metadata, out);
  out.println("Tile size: " + metadata.getTilesize());
  out.println("Bands: " + metadata.getBands());
  printNodata(metadata, out);
  out.println("Classification: " + metadata.getClassification().name());
  out.println("Resampling: " + metadata.getResamplingMethod());
  out.println("");
  for (int zoom = metadata.getMaxZoomLevel(); zoom >= 1; zoom--)
  {
    out.println("level " + zoom);
    out.println("  image: \"" + metadata.getPyramid() + " " +
        metadata.getName(zoom) + "\"");
    if (verbose)
    {
      printFileInfo(new Path(metadata.getPyramid(), metadata.getName(zoom)), out);
    }

    final LongRectangle pb = metadata.getPixelBounds(zoom);

    out.print("  width: " + pb.getWidth());
    out.print(" height: " + pb.getHeight());

    df = new DecimalFormat("0.00000000");

    out.print("  px w: " + df.format(metadata.getPixelWidth(zoom)));
    out.println(" px h: " + df.format(metadata.getPixelWidth(zoom)));

    final LongRectangle tb = metadata.getTileBounds(zoom);

    df = new DecimalFormat("#");

    out.print("  Tile Bounds: (x, y)");
    out.print("  size (" + df.format(tb.getWidth()) + ", " + df.format(tb.getHeight()) +
        ")");
    if (verbose)
    {
      MrsImageDataProvider dp = DataProviderFactory.getMrsImageDataProvider(metadata.getPyramid(),
          AccessMode.READ, providerProperties);
      MrsPyramidReaderContext context = new MrsPyramidReaderContext();
      context.setZoomlevel(zoom);
      MrsImageReader reader;
      try
      {
        reader = dp.getMrsTileReader(context);
        out.println(" stored tiles: " + reader.calculateTileCount());
      }
      catch (IOException e)
      {
        out.println("Unable to get tile reader for: " + metadata.getPyramid());
      }
    }
    else
    {
      out.println();
    }
    if (tb.getWidth() == 1 && tb.getHeight() == 1)
    {
      out
          .println("    (" + df.format(tb.getMinX()) + ", " + df.format(tb.getMinY()) + ")");
    }
    else
    {
      out.print("    UL (" + df.format(tb.getMinX()) + ", " + df.format(tb.getMaxY()) +
          ")");
      out.println("  UR (" + df.format(tb.getMaxX()) + ", " + df.format(tb.getMaxY()) +
          ")");
      out.print("    LL (" + df.format(tb.getMinX()) + ", " + df.format(tb.getMinY()) +
          ")");
      out.println("  LR (" + df.format(tb.getMaxX()) + ", " + df.format(tb.getMinY()) +
          ")");
    }
    out.println("  ImageStats:");
    for (int b = 0; b < metadata.getBands(); b++)
    {
      out.print("    ");
      if (metadata.getBands() > 1)
      {
        out.print("band " + b + ": ");
      }

      df = new DecimalFormat("0.#");

      final ImageStats stats = metadata.getImageStats(zoom, b);
      out.println("min: " + df.format(stats.min) + " max: " + df.format(stats.max) +
          " mean: " + df.format(stats.mean) + " sum: " + df.format(stats.sum) + " count: " +
          df.format(stats.count));
    }

  }
  out.println("");
  out.println("");
}

private void printSplitPoints(final String pyramidName, PrintStream out)
{

}

}
