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

package org.mrgeo.cmd.upgradepyramid;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridEnvelope2D;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.coverage.grid.io.AbstractGridCoverage2DReader;
import org.geotools.geometry.Envelope2D;
import org.geotools.geometry.GeneralEnvelope;
import org.mrgeo.cmd.Command;
import org.mrgeo.aggregators.*;
import org.mrgeo.buildpyramid.BuildPyramidDriver;
import org.mrgeo.image.geotools.GeotoolsRasterUtils;
import org.mrgeo.ingest.IngestImageDriver;
import org.mrgeo.ingest.IngestImageDriver.IngestImageException;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.utils.Bounds;
import org.mrgeo.utils.HadoopUtils;
import org.mrgeo.utils.LoggingUtils;
import org.mrgeo.utils.TMSUtils;
import org.opengis.referencing.operation.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

public class UpgradePyramid extends Command
{
  private static Logger log = LoggerFactory.getLogger(UpgradePyramid.class);

  private int zoomlevel;
  private int tilesize;
  private Number nodata = null;
  private int bands;

  private Bounds bounds = null;

  public static Options createOptions()
  {
    final Options result = new Options();

    final Option output = new Option("o", "output", true, "MrsImagePyramid image name");
    output.setRequired(true);
    result.addOption(output);

    final Option cat = new Option("c", "categorical", false,
        "Input [pixels] are categorical values");
    cat.setRequired(false);
    result.addOption(cat);

    final Option pyramid = new Option("sp", "skippyramid", false, "Skip building pyramids");
    pyramid.setRequired(false);
    result.addOption(pyramid);

    final Option recurse = new Option("nr", "norecursion", false,
        "Do not recurse through sub-directories");
    recurse.setRequired(false);
    result.addOption(recurse);

    Option mean = new Option("m", "mean", false, "Mean (Average) Pyramid Pixel Resampling Method");
    mean.setRequired(false);
    result.addOption(mean);

    Option sum = new Option("s", "sum", false, "Summing Pyramid Pixel Resampling Method");
    sum.setRequired(false);
    result.addOption(sum);

    Option nearest = new Option("n", "nearest", false, "Nearest Pyramid Pixel Resampling Method");
    nearest.setRequired(false);
    result.addOption(nearest);

    Option min = new Option("min", "minimum", false, "Minimum Pyramid Pixel Resampling Method");
    min.setRequired(false);
    result.addOption(min);

    Option max = new Option("max", "maximum", false, "Maximum Pyramid Pixel Resampling Method");
    max.setRequired(false);
    result.addOption(max);

    Option minavgpair = new Option("minavgpair", "miminumaveragepair", false, "Minimum Average Pair Pyramid Pixel Resampling Method");
    minavgpair.setRequired(false);
    result.addOption(minavgpair);

    final Option local = new Option("l", "local-runner", false,
        "Use Hadoop's local runner (used for debugging)");
    local.setRequired(false);
    result.addOption(local);

    result.addOption(new Option("v", "verbose", false, "Verbose logging"));
    result.addOption(new Option("d", "debug", false, "Debug (very verbose) logging"));

    return result;
  }

  @Override
  public int run(final String[] args, Configuration conf, Properties providerProperties)
  {
    log.info("UpgradePyramid");

    try
    {
      final Options options = UpgradePyramid.createOptions();
      CommandLine line = null;
      try
      {
        final CommandLineParser parser = new PosixParser();
        line = parser.parse(options, args);
      }
      catch (final ParseException e)
      {
        new HelpFormatter().printHelp("UpgradePyramid <input>", options);
        return -1;
      }

      if (line != null)
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
          HadoopUtils.setupLocalRunner(conf);
        }

        final boolean categorical = line.hasOption("c");
        final boolean skipPyramids = line.hasOption("sp");
        final boolean recurse = !line.hasOption("nr");
        final String output = line.getOptionValue("o");

        log.debug("categorical: " + categorical);
        log.debug("skip pyramids: " + skipPyramids);
        log.debug("output: " + output);

        final List<String> inputs = new LinkedList<String>();

        for (final String arg : line.getArgs())
        {
          final String pyname = arg + "/Level000";
          inputs.addAll(getInputs(pyname, recurse, conf));
        }

        log.info("Ingest inputs (" + inputs.size() + ")");
        for (final String input : inputs)
        {
          log.info("   " + input);
        }

        if (inputs.size() > 0)
        {
          try
          {
            IngestImageDriver.localIngest(inputs.toArray(new String[inputs.size()]), 
              output, categorical, conf, bounds, zoomlevel, tilesize, nodata.doubleValue(),
              bands, new HashMap<String, String>(), "", providerProperties);

            if (!skipPyramids)
            {
              Aggregator aggregator = new MeanAggregator();
              if (line.hasOption("c"))
              {
                aggregator = new ModeAggregator();
              }
              else if (line.hasOption("s"))
              {
                aggregator = new SumAggregator();
              }
              else if (line.hasOption("n"))
              {
                aggregator = new NearestAggregator();
              }
              else if (line.hasOption("min"))
              {
                aggregator = new MinAggregator();
              }
              else if (line.hasOption("max"))
              {
                aggregator = new MaxAggregator();
              }
              else if (line.hasOption("minavgpair"))
              {
                aggregator = new MinAvgPairAggregator();
              }

              BuildPyramidDriver.build(output, aggregator, conf, providerProperties);
            }
          }
          catch (final Exception e)
          {
            e.printStackTrace();
            log.error("UpgradePyramid exited with error");
            return -1;
          }
        }
      }
      log.info("UpgradePyramid complete");

      return 0;

    }
    catch (Exception e)
    {
      e.printStackTrace();
    }

    return -1;
  }

  private void calculateParams(AbstractGridCoverage2DReader reader)
  {
    try
    {
      GridCoverage2D image = GeotoolsRasterUtils.getImageFromReader(reader, "EPSG:4326");

      // calculate zoom level for the image
      final GridGeometry2D geometry = image.getGridGeometry();
      Envelope2D pixelEnvelop;
      pixelEnvelop = geometry.gridToWorld(new GridEnvelope2D(0, 0, 1, 1));

      final double pixelsizeLon = Math.abs(pixelEnvelop.width);
      final double pixelsizeLat = Math.abs(pixelEnvelop.height);

      final int zx = TMSUtils.zoomForPixelSize(pixelsizeLon, tilesize);
      final int zy = TMSUtils.zoomForPixelSize(pixelsizeLat, tilesize);

      if (zoomlevel < zx)
      {
        zoomlevel = zx;
      }
      if (zoomlevel < zy)
      {
        zoomlevel = zy;
      }

      bands = image.getNumSampleDimensions();
      try
      {
        Method m = reader.getClass().getMethod("getMetadata", new Class[] {});
        final Object meta = m.invoke(reader, new Object[] {});

        m = meta.getClass().getMethod("getNoData", new Class[] {});

        if (nodata == null)
        {
          nodata = 0; // default...
          nodata = (Number) m.invoke(meta, new Object[] {});

          log.debug("nodata: b: " + nodata.byteValue() + " d: " + nodata.doubleValue() +
            " f: " + nodata.floatValue() + " i: " + nodata.intValue() +
            " s: " + nodata.shortValue() + " l: " + nodata.longValue());
        }
        else
        {
          final double n = (Double) m.invoke(meta, new Object[] {});

          if ((Double.isNaN(nodata.doubleValue()) != Double.isNaN(n)) ||
              (!Double.isNaN(nodata.doubleValue()) && (nodata.doubleValue() != n)))
          {
            throw new IngestImageException(
              "All images within the set need to have the same nodata value: "
                  + " one has " + m.invoke(meta, new Object[] {}) + ", others have "
                  + nodata.toString());
          }
        }

        final GeneralEnvelope envelope = (GeneralEnvelope) image.getEnvelope();

        log.debug("    image bounds: (lon/lat) " +
            envelope.getMinimum(GeotoolsRasterUtils.LON_DIMENSION) + ", " +
            envelope.getMinimum(GeotoolsRasterUtils.LAT_DIMENSION) + " to " +
            envelope.getMaximum(GeotoolsRasterUtils.LON_DIMENSION) + ", " +
            envelope.getMaximum(GeotoolsRasterUtils.LAT_DIMENSION));

        if (bounds == null)
        {
          bounds = new Bounds(envelope
            .getMinimum(GeotoolsRasterUtils.LON_DIMENSION), envelope
            .getMinimum(GeotoolsRasterUtils.LAT_DIMENSION), envelope
            .getMaximum(GeotoolsRasterUtils.LON_DIMENSION), envelope
            .getMaximum(GeotoolsRasterUtils.LAT_DIMENSION));
        }
        else
        {
          bounds.expand(envelope
            .getMinimum(GeotoolsRasterUtils.LON_DIMENSION), envelope
            .getMinimum(GeotoolsRasterUtils.LAT_DIMENSION), envelope
            .getMaximum(GeotoolsRasterUtils.LON_DIMENSION), envelope
            .getMaximum(GeotoolsRasterUtils.LAT_DIMENSION));
        }


      }
      catch (final NoSuchMethodException e)
      {

      }
      catch (IllegalArgumentException e)
      {
      }
      catch (IllegalAccessException e)
      {
      }
      catch (InvocationTargetException e)
      {
      }

    }
    catch (TransformException e)
    {
    }
    catch (IOException e)
    {
    }
  }

  List<String> getInputs(String arg, boolean recurse, final Configuration conf) throws IOException 
  {
    AbstractGridCoverage2DReader reader = null;

    List<String> inputs = new LinkedList<String>();

    File f = new File(arg);

    // recurse through directories
    if (f.isDirectory())
    {
      File[] dir = f.listFiles();

      for (File s: dir)
      {
        try
        {
          if (s.isFile() || (s.isDirectory() && recurse))
          {
            inputs.addAll(getInputs(s.getCanonicalPath(), recurse, conf));
          }
        }
        catch (IOException e)
        {
        }
      }
    }
    else if (f.isFile())
    {
      try
      {
        // is this an geospatial image file?
        System.out.print("*** checking " + f.getCanonicalPath());
        try
        {
          reader = GeotoolsRasterUtils.openImage("file://" + f.getCanonicalPath());

          if (reader != null)
          {
            System.out.println(" accepted ***");
            calculateParams(reader);
            inputs.add(f.getCanonicalPath());
          }
          else
          {
            System.out.println(" can't load ***");
          }
        }
        catch (IOException e)
        {
          System.out.println(" can't load ***");
        }

      }
      finally
      {
        try
        {
          GeotoolsRasterUtils.closeStreamFromReader(reader);
        }
        catch (Exception e)
        {
        }
      }

      //      // is this an geospatial image file?
      //      
      //      System.out.print("*** checking " + f.getCanonicalPath());
      //      if (GeotoolsRasterUtils.fastAccepts(f))
      //      {
      //        try
      //        {
      //          System.out.println(" accepted ***");
      //          inputs.add(f.getCanonicalPath());
      //        }
      //        catch (IOException e)
      //        {
      //          e.printStackTrace();
      //        }
      //      }
      //      else
      //      {
      //        System.out.println(" can't load ***");
      //      }
    }
    else
    {
      Path p = new Path(arg);
      FileSystem fs = HadoopFileUtils.getFileSystem(conf, p);

      if (fs.exists(p))
      {              
        FileStatus status = fs.getFileStatus(p);

        if (status.isDir() && recurse)
        {
          FileStatus[] files = fs.listStatus(p);
          for (FileStatus file: files)
          {
            inputs.addAll(getInputs(file.getPath().toString(), recurse, conf));
          }
        }
        else
        {
          try
          {
            // is this an geospatial image file?
            System.out.print("*** checking " + p.toString());
            try
            {
              reader = GeotoolsRasterUtils.openImage(p.toString());
              if (reader != null)
              {
                System.out.println(" accepted ***");
                calculateParams(reader);
                inputs.add(p.toString());
              }
              else
              {
                System.out.println(" can't load ***");
              }
            }
            catch (IOException e)
            {
              System.out.println(" can't load ***");
            }
          }
          finally
          {
            try
            {
              GeotoolsRasterUtils.closeStreamFromReader(reader);
            }
            catch (Exception e)
            {
            }
          }
          //          InputStream stream = null;
          //          try
          //          {
          //            stream = HadoopFileUtils.open(getConf(), p);
          //            AbstractGridCoverage2DReader reader = GeotoolsRasterUtils.openHdfsImage(getConf(), p);
          //            // is this an geospatial image file?
          //            System.out.print("*** checking " + p.toString());
          //            if (GeotoolsRasterUtils.fastAccepts(stream))
          //            {
          //              System.out.println(" accepted ***");
          //              inputs.add(p.toString());
          //            }
          //            else
          //            {
          //              System.out.println(" can't load ***");
          //            }
          //          }
          //          finally
          //          {
          //            if (stream != null)
          //            {
          //              stream.close();
          //            }
          //          }
        }
      }
    }

    return inputs;
  }

}
