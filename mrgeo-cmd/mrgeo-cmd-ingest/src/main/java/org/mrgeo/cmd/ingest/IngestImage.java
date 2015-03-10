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

package org.mrgeo.cmd.ingest;

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
import org.joda.time.Period;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;
import org.mrgeo.cmd.Command;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.aggregators.*;
import org.mrgeo.buildpyramid.BuildPyramidDriver;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.adhoc.AdHocDataProvider;
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
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

public class IngestImage extends Command
{

  private Options options;

  private static Logger log = LoggerFactory.getLogger(IngestImage.class);

  private int zoomlevel;
  private int tilesize;
  private Number nodata = null;
  private int bands;
  private boolean overrideNodata = false;
  private boolean local = false;
  private boolean quick = false;
  private boolean ignoretags = false;
  private Map<String, String> tags = new HashMap<String, String>();
  private Bounds bounds = null;

  private AdHocDataProvider adhoc = null;
  private PrintStream adhocStream = null;

  public IngestImage()
  {
    options = createOptions();
  }


  public static Options createOptions()
  {
    Options result = new Options();

    Option output = new Option("o", "output", true, "MrsImagePyramid image name");
    output.setRequired(true);
    result.addOption(output);

    Option cat = new Option("c", "categorical", false, "Input [pixels] are categorical values");
    cat.setRequired(false);
    result.addOption(cat);

    Option pyramid = new Option("sp", "skippyramid", false, "Skip building pyramids");
    pyramid.setRequired(false);
    result.addOption(pyramid);

    Option recurse = new Option("nr", "norecursion", false, "Do not recurse through sub-directories");
    recurse.setRequired(false);
    result.addOption(recurse);

    Option nodata = new Option("nd", "nodata", true, "override nodata value");
    //nodata.setArgPattern(argPattern, limit);
    nodata.setRequired(false);
    result.addOption(nodata);

    Option tags = new Option("t", "tags", true, "tags (fmt: \"k1,v:k2,v:...\"");
    tags.setRequired(false);
    result.addOption(tags);

    Option notags = new Option("nt", "notags", false, "Do not automatically load tags from source images");
    notags.setRequired(false);
    result.addOption(notags);

    OptionGroup aggregators = new OptionGroup();

    Option mean = new Option("m", "mean", false, "Mean (Average) Pyramid Pixel Resampling Method");
    mean.setRequired(false);
    aggregators.addOption(mean);

    Option sum = new Option("s", "sum", false, "Summing Pyramid Pixel Resampling Method");
    sum.setRequired(false);
    aggregators.addOption(sum);

    Option nearest = new Option("n", "nearest", false, "Nearest Pyramid Pixel Resampling Method");
    nearest.setRequired(false);
    aggregators.addOption(nearest);

    Option min = new Option("min", "minimum", false, "Minimum Pyramid Pixel Resampling Method");
    min.setRequired(false);
    aggregators.addOption(min);

    Option max = new Option("max", "maximum", false, "Maximum Pyramid Pixel Resampling Method");
    max.setRequired(false);
    aggregators.addOption(max);

    Option minavgpair = new Option("minavgpair", "miminumaveragepair", false, "Minimum Average Pair Pyramid Pixel Resampling Method");
    minavgpair.setRequired(false);
    aggregators.addOption(minavgpair);

    result.addOptionGroup(aggregators);

    Option local = new Option("lc", "local", false, "Use local files for ingest, good for small ingests");
    local.setRequired(false);
    result.addOption(local);

    Option lcl = new Option("l", "local-runner", false, "Use Hadoop's local runner (used for debugging)");
    lcl.setRequired(false);
    result.addOption(lcl);

    Option quick = new Option("q", "quick", false, "Quick ingest (for small files only)");
    quick.setRequired(false);
    result.addOption(quick);

    Option protectionLevelOption = new Option("pl", "protectionLevel", true, "Protection level");
    // If mrgeo.conf security.classification.required is true and there is no
    // security.classification.default, then the security classification
    // argument is required, otherwise it is not.
    Properties props = MrGeoProperties.getInstance();
    String protectionLevelRequired = props.getProperty(
        MrGeoConstants.MRGEO_PROTECTION_LEVEL_REQUIRED, "false").trim();
    String protectionLevelDefault = props.getProperty(
        MrGeoConstants.MRGEO_PROTECTION_LEVEL_DEFAULT, "");
    if (protectionLevelRequired.equalsIgnoreCase("true") &&
        protectionLevelDefault.isEmpty())
    {
      protectionLevelOption.setRequired(true);
    }
    else
    {
      protectionLevelOption.setRequired(false);
    }
    result.addOption(protectionLevelOption);


    result.addOption(new Option("v", "verbose", false, "Verbose logging"));
    result.addOption(new Option("d", "debug", false, "Debug (very verbose) logging"));

    return result;
  }

  private void calculateParams(AbstractGridCoverage2DReader reader, final Configuration conf)
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

      if (adhoc == null)
      {
        adhoc = DataProviderFactory.createAdHocDataProvider(conf);
        adhocStream = new PrintStream(adhoc.add(IngestImageDriver.INGEST_BOUNDS_FILE));
      }

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
        else if (!overrideNodata)
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


//    if (reader instanceof GeoTiffReader)
//    {
//      GeoTiffIIOMetadataDecoder metadata = ((GeoTiffReader)reader).getMetadata();
//
//
//      for (GeoKeyEntry key: metadata.getGeoKeys())
//      {
//        System.out.println("  " + key.getKeyID() + ":" + metadata.getGeoKey(key.getKeyID()) + ":");
//      }
//    }

    String[] names = reader.getMetadataNames();
    if (names != null)
    {
      System.out.println("Metadata:");
      for (String name : names)
      {
        System.out.println("  " + name + ":" + reader.getMetadataValue(name));
        tags.put(name, reader.getMetadataValue(name));
      }
      System.out.println("***");
    }
  }

  List<String> getInputs(String arg, boolean recurse, final Configuration conf) throws IOException
  {
    AbstractGridCoverage2DReader reader = null;

    List<String> inputs = new LinkedList<String>();

    try
    {
      File f = new File(new URI(arg));

      // recurse through directories
      if (f.isDirectory())
      {
        File[] dir = f.listFiles();

        for (File s : dir)
        {
          try
          {
            if (s.isFile() || (s.isDirectory() && recurse))
            {
              inputs.addAll(getInputs(s.getCanonicalFile().toURI().toString(), recurse, conf));
            }
          }
          catch (IOException e)
          {
          }
        }
      }
      else if (f.isFile())
      {
        // is this an geospatial image file?
        System.out.print("*** checking (local file) " + f.getCanonicalPath());
        try
        {
          //reader = GeotoolsRasterUtils.openImage("file://" + f.getCanonicalPath());
          reader = GeotoolsRasterUtils.openImage(f.getCanonicalFile().toURI().toString());

          if (reader != null)
          {
            System.out.println(" accepted ***");
            calculateParams(reader, conf);
            inputs.add(f.getCanonicalFile().toURI().toString());
          }
          else
          {
            reader = GeotoolsRasterUtils.openImageFromFile(f);

            if (reader != null)
            {
              System.out.println(" accepted ***");
              calculateParams(reader, conf);
              inputs.add(f.getCanonicalFile().toURI().toString());

              // force local mode
              local = true;
            }
            else
            {
              System.out.println(" can't load ***");
            }
          }
        }
        catch (IOException e)
        {
          try
          {
            reader = GeotoolsRasterUtils.openImageFromFile(f);

            if (reader != null)
            {
              System.out.println(" accepted ***");
              calculateParams(reader, conf);
              inputs.add(f.getCanonicalFile().toURI().toString());

              // force local mode
              local = true;
            }
            else
            {
              System.out.println(" can't load ***");
            }
          }
          catch (IOException e2)
          {
            System.out.println(" can't load ***");
          }
          catch (UnsupportedOperationException e2)
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
      return inputs;
    }
    catch (URISyntaxException e)
    {
    } catch(IllegalArgumentException e){
      
    }


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
          inputs.addAll(getInputs(file.getPath().toUri().toString(), recurse, conf));
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
            reader = GeotoolsRasterUtils.openImage(p.toUri().toString());
            if (reader != null)
            {
              System.out.println(" accepted ***");
              calculateParams(reader, conf);
              inputs.add(p.toUri().toString());
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
      }
    }

    return inputs;

  }


  @Override
  public int run(String[] args, Configuration conf, Properties providerProperties)
  {
    try
    {
      long start = System.currentTimeMillis();

      CommandLine line = null;
      try
      {
        CommandLineParser parser = new GnuParser();
        line = parser.parse(options, args);
      }
      catch (ParseException e)
      {
        System.out.println(e.getMessage());
        new HelpFormatter().printHelp("ingest <options> <input>", options);
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

        overrideNodata = line.hasOption("nd");


        boolean categorical = line.hasOption("c");
        boolean skipPyramids = line.hasOption("sp");
        boolean recurse = !line.hasOption("nr");
        String output = line.getOptionValue("o");

        log.debug("categorical: " + categorical);
        log.debug("skip pyramids: " + skipPyramids);
        log.debug("output: " + output);

        List<String> inputs = new LinkedList<String>();

        zoomlevel = 0;
        tilesize = Integer.parseInt(MrGeoProperties.getInstance().getProperty("mrsimage.tilesize", "512"));

        for (String arg: line.getArgs())
        {
          inputs.addAll(getInputs(arg, recurse, conf));
        }

        log.info("Ingest inputs (" + inputs.size() + ")");
        for (String input:inputs)
        {
          log.info("   " + input);
        }

        if (line.hasOption("t"))
        {
          String rawTags = line.getOptionValue("t");

          String splittags[] = rawTags.split(",");
          for (String t: splittags)
          {
            String[] s = t.split(":");
            if (s.length != 2)
            {
              log.error("Bad tag format.  Should be: k1:v1,k2:v2,...  is: " + rawTags);
              return -1;
            }

            tags.put(s[0], s[1]);
          }
        }

        if (overrideNodata)
        {
          String str = line.getOptionValue("nd");
          if (str.compareToIgnoreCase("nan") != 0)
          {
            nodata = Double.parseDouble(line.getOptionValue("nd"));
            log.info("overriding nodata with: " + nodata);
          }
        }
        else if (nodata == null)
        {
          nodata = Double.NaN;
        }

        quick = quick | line.hasOption("q");
        local = local | line.hasOption("lc");
        String protectionLevel = line.getOptionValue("pl");
        if (inputs.size() > 0)
        {
          try
          {
            final boolean success;
            if (quick)
            {
              success = IngestImageDriver.quickIngest(inputs.get(0), output, categorical,
                  conf, overrideNodata, nodata, tags, protectionLevel, providerProperties);
            }
            else if (local)
            {
              success = IngestImageDriver.localIngest(inputs.toArray(new String[inputs.size()]),
                  output, categorical, conf, bounds, zoomlevel, tilesize, nodata, bands,
                  tags, protectionLevel, providerProperties);
            }
            else
            {
              success = IngestImageDriver.ingest(inputs.toArray(new String[inputs.size()]),
                  output, categorical, conf, bounds, zoomlevel, tilesize, nodata, bands,
                  tags, protectionLevel, providerProperties);
            }
            if (!success)
            {
              log.error("IngestImage exited with error");
              return 1;
            }

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
          catch (Exception e)
          {
            e.printStackTrace();
            log.error("IngestImage exited with error", e);
            return 1;
          }
        }
      }

      long end = System.currentTimeMillis();
      long duration = end - start;
      PeriodFormatter formatter = new PeriodFormatterBuilder()
          .appendHours()
          .appendSuffix("h:")
          .appendMinutes()
          .appendSuffix("m:")
          .appendSeconds()
          .appendSuffix("s")
          .toFormatter();
      String formatted = formatter.print(new Period(duration));
      log.info("IngestImage complete in " + formatted);

      return 0;
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }

    return -1;
  }
}
