/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */

package org.mrgeo.cmd;

import java.util.Properties;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.mapalgebra.BasicInputFormatDescriptor;
import org.mrgeo.mapalgebra.InputFormatDescriptor;
import org.mrgeo.mapreduce.RasterizeVectorDriver;
import org.mrgeo.mapreduce.RasterizeVectorPainter;
import org.mrgeo.utils.*;

/**
 * 
 */
public class RasterizeVector extends Configured implements Tool
{
  public static Options createOptions()
  {
    Options result = new Options();

    Option input = new Option("i", "input", true, "Input vector file name or directory of vectors");
    input.setRequired(true);
    result.addOption(input);

    Option output = new Option("o", "output", true, "Output image file name");
    output.setRequired(true);
    result.addOption(output);

    result.addOption("c", "column", true,
        "use the value in the specified column for calculating the value to paint");

    result.addOption("a", "aggregator", true,
        "value type for each intersecting pixel " +
            "(\"last\" - value from last encountered geometry (last is arbitrary), " +
            "\"mask\" - use zero, " + 
            "\"sum\" - sum the values (count if not using a column value), " +
        "\"average\" - average of values");

    result.addOption(new Option("z", "zoom", false,
        "zoom level for rasterization (short cut for cell size)"));

    result.addOption(new Option("s", "cellsize", true,
        "cellsize (pixel size) for rasterization.  may be followed by \"d\" - decimal degrees (default), \"m\" meters or \"z\" zoom level"));

    Option lcl = new Option("l", "local-runner", false, "Use Hadoop's local runner (used for debugging)");
    lcl.setRequired(false);
    result.addOption(lcl);
    
    result.addOption(new Option("v", "verbose", false, "Verbose logging"));
    result.addOption(new Option("d", "debug", false, "Debug (very verbose) logging"));

    return result;
  }

  public static void main(String[] args) throws Exception
  {
    int res = ToolRunner.run(HadoopUtils.createConfiguration(), new RasterizeVector(), args);
    System.exit(res);
  }

  @Override
  public int run(String[] args) throws Exception
  {
    CommandLineParser parser = new PosixParser();
    Options options = RasterizeVector.createOptions();
    CommandLine line = null;
    try
    {
      line = parser.parse(options, args);
    }
    catch (ParseException e)
    {
      System.out.println(e.getMessage());
      System.out.println();
      new HelpFormatter().printHelp("RasterizeVector", options);
      return 1;
    }

    String output = line.getOptionValue("o");
    String input = line.getOptionValue("i");

    RasterizeVectorPainter.AggregationType aggregationType = null;

    if (line.hasOption("a"))
    {
      String value = line.getOptionValue("a").toLowerCase();

      if (value.contentEquals("last"))
      {
        aggregationType = RasterizeVectorPainter.AggregationType.LAST;
      }
      else if (value.contentEquals("mask"))
      {
        aggregationType = RasterizeVectorPainter.AggregationType.MASK;
      }
      else if (value.contentEquals("sum"))
      {
        aggregationType = RasterizeVectorPainter.AggregationType.SUM;
      }
      else if (value.contentEquals("average"))
      {
        aggregationType = RasterizeVectorPainter.AggregationType.AVERAGE;
      }
      else if (value.contentEquals("min"))
      {
        aggregationType = RasterizeVectorPainter.AggregationType.MIN;
      }
      else if (value.contentEquals("max"))
      {
        aggregationType = RasterizeVectorPainter.AggregationType.MAX;
      }
    }
    

    if (aggregationType == null) 
    {
      throw new IllegalArgumentException("Please specify a valid aggregation type.");
    }

    int zoom = -1;
    if (line.hasOption("z"))
    {
      zoom = Integer.parseInt(line.getOptionValue("z"));
    }
    else if (line.hasOption("s"))
    {
      String cs = line.getOptionValue("s").toLowerCase();
      double cellsize = -1;
      if (cs.endsWith("m"))
      {
        cs = cs.replace("m", "");
        cellsize = Double.parseDouble(cs) / LatLng.METERS_PER_DEGREE;
      }
      else if (cs.endsWith("z"))
      {
        cs = cs.replace("z", "");
        zoom = Integer.parseInt(cs);
      }
      else 
      {
        if (cs.endsWith("d"))
        {
          cs = cs.replace("d", "");
        }
        cellsize = Double.parseDouble(cs);
      }
      final int tilesize = Integer.parseInt(MrGeoProperties.getInstance().getProperty("mrsimage.tilesize", "512"));

      if (zoom < 1)
      {
        zoom = TMSUtils.zoomForPixelSize(cellsize, tilesize);
      }
    }
    else
    {
      throw new Exception("Either zoom or cellsize must specified.");

    }
    String valueColumn = line.getOptionValue("c");

    
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
      HadoopUtils.setupLocalRunner(getConf());
    }

    try
    {
      Bounds bounds = calculateBounds(input);

      InputFormatDescriptor ifd = new BasicInputFormatDescriptor(input);

      RasterizeVectorDriver rvd = new RasterizeVectorDriver();

      Job job = new Job(getConf());

      ifd.populateJobParameters(job);

      rvd.setValueColumn(valueColumn);

      rvd.run(job, output, aggregationType, zoom, bounds, null, null, (Properties)null);

      //      if (input.toLowerCase().endsWith(".csv"))
      //      {
      //        if (aggregationType != VectorRasterizer.AggregationType.SUM)
      //        {
      //          System.out.println("Only summing is supported on CSV files.");
      //          return 1;
      //        }
      //        if (line.getOptionValue("n") != null)
      //        {
      //          System.out.println("Normalize is not supported on CSV files.");
      //        }
      //        VectorRasterizerCsv.run(input, image, aggregationType, getConf(), null, null);
      //      }
      //      else
      //      {
      //        VectorRasterizer vr = new VectorRasterizer();
      //
      //        vr.setValueColumn(valueColumn);
      //
      //        vr.run(new Path(input), pyramid, aggregationType, null, null);
      //      }

    }
    catch (Exception e)
    {
      e.printStackTrace();
    }

    return 0;
  }

  private static Bounds calculateBounds(String input)
  {
    return Bounds.world;
  }
}
