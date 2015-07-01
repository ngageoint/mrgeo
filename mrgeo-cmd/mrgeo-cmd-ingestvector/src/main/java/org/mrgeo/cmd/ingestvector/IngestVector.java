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

package org.mrgeo.cmd.ingestvector;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.joda.time.Period;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;
import org.mrgeo.cmd.Command;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.image.geotools.GeotoolsRasterUtils;
import org.mrgeo.mapreduce.ingestvector.IngestVectorDriver;
import org.mrgeo.vector.mrsvector.OSMTileIngester;
import org.mrgeo.utils.geotools.GeotoolsVectorReader;
import org.mrgeo.utils.geotools.GeotoolsVectorUtils;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.utils.HadoopUtils;
import org.mrgeo.utils.LoggingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

/**
 * 
 */
public class IngestVector extends Command
{
  private static Logger log = LoggerFactory.getLogger(IngestVector.class);

  public Configuration config;
  public static Options createOptions()
  {
    Options result = new Options();

    Option output = new Option("o", "output", true, "VectorTile name");
    output.setRequired(true);
    result.addOption(output);

    Option zoom = new Option("z", "zoom", true, "Maximum zoom level for the ingest");
    zoom.setRequired(false);
    result.addOption(zoom);

    Option pyramid = new Option("sp", "skippyramid", false, "Skip building pyramids");
    pyramid.setRequired(false);
    result.addOption(pyramid);

    Option recurse = new Option("nr", "norecursion", false, "Do not recurse through sub-directories");
    recurse.setRequired(false);
    result.addOption(recurse);

    Option local = new Option("lc", "local", false, "Use local files for ingest, good for small ingests");
    local.setRequired(false);
    result.addOption(local);

    Option lcl = new Option("l", "local-runner", false, "Use Hadoop's local runner (used for debugging)");
    lcl.setRequired(false);
    result.addOption(lcl);

    Option osm = new Option("osm", false, "Input is an OSM pbf or xml file");
    osm.setRequired(false);
    result.addOption(osm);

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


  List<String> getInputs(String arg, boolean recurse) throws IOException 
  {
    GeotoolsVectorReader reader = null;

    List<String> inputs = new LinkedList<String>();

    File f = new File(arg);
    URI uri = f.toURI();
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
            inputs.addAll(getInputs(s.getCanonicalPath(), recurse));
          }
        }
        catch (IOException e)
        {
        }
      }
    }
    else if (f.isFile())
    {
      // is this a valid file?
      System.out.print("*** checking " + f.getCanonicalPath());
      try
      {
        reader = GeotoolsVectorUtils.open(uri);

        if (reader != null)
        {
          System.out.println(" accepted ***");
          inputs.add(uri.toString());
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
    else
    {
      Path p = new Path(arg);
      FileSystem fs = HadoopFileUtils.getFileSystem(config, p);
      if (fs.exists(p))
      {              
        FileStatus status = fs.getFileStatus(p);

        if (status.isDir() && recurse)
        {
          FileStatus[] files = fs.listStatus(p);
          for (FileStatus file: files)
          {
            inputs.addAll(getInputs(file.getPath().toString(), recurse));
          }
        }
        else
        {
          // is this a valid file?
          System.out.print("*** checking " + p.toString());
          try
          {
            reader = GeotoolsVectorUtils.open(p.makeQualified(fs).toUri());
            if (reader != null)
            {
              System.out.println(" accepted ***");
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
      }
    }

    return inputs;
  }


  List<String> oldGetInputs(String arg, boolean recurse) throws IOException 
  {
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
            inputs.addAll(getInputs(s.getCanonicalPath(), recurse));
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
      System.out.print("*** checking " + f.getCanonicalPath());
      if (GeotoolsRasterUtils.fastAccepts(f))
      {
        try
        {
          System.out.println(" accepted ***");
          inputs.add(f.getCanonicalPath());
        }
        catch (IOException e)
        {
          e.printStackTrace();
        }
      }
      else
      {
        System.out.println(" can't load ***");
      }
    }
    else
    {
      Path p = new Path(arg);
      FileSystem fs = HadoopFileUtils.getFileSystem(config, p);
      if (fs.exists(p))
      {              
        FileStatus status = fs.getFileStatus(p);

        if (status.isDir() && recurse)
        {
          FileStatus[] files = fs.listStatus(p);
          for (FileStatus file: files)
          {
            inputs.addAll(getInputs(file.getPath().toString(), recurse));
          }
        }
        else
        {
          InputStream stream = null;
          try
          {
            stream = HadoopFileUtils.open(config, p);
            // is this an geospatial image file?
            System.out.print("*** checking " + p.toString());
            if (GeotoolsRasterUtils.fastAccepts(stream))
            {
              System.out.println(" accepted ***");
              inputs.add(p.toString());
            }
            else
            {
              System.out.println(" can't load ***");
            }
          }
          finally
          {
            if (stream != null)
            {
              stream.close();
            }
          }
        }
      }
    }

    return inputs;
  }

  @Override
  public int run(String[] args, Configuration conf, Properties providerProperties)
  {
    log.info("IngestVector");

    try
    {
      config = conf;
      long start = System.currentTimeMillis();

      Options options = IngestVector.createOptions();
      CommandLine line = null;
      try
      {
        CommandLineParser parser = new GnuParser();
        line = parser.parse(options, args);
      }
      catch (ParseException e)
      {
        new HelpFormatter().printHelp("IngestVector <input>", options);
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
          HadoopUtils.setupLocalRunner(config);
        }

        int zoomlevel = -1;
        if (line.hasOption("z"))
        {
          zoomlevel = Integer.valueOf(line.getOptionValue("z"));
        }

        boolean skipPyramids = line.hasOption("sp");
        boolean recurse = !line.hasOption("nr");
        String output = line.getOptionValue("o");

        log.debug("skip pyramids: " + skipPyramids);
        log.debug("output: " + output);

        // need to initialize before looking for the files...
        GeotoolsVectorUtils.initialize();

        String protectionLevel = line.getOptionValue("pl");
        if (line.hasOption("osm"))
        {
          String[] inputs = line.getArgs();
          if (!line.hasOption("z"))
          {
            System.out.println("You must specify a zomm level with -z when ingesting OSM data");
            return -1;
          }
          if (line.hasOption("l"))
          {
            System.out.println("Using local runner");
            HadoopUtils.setupLocalRunner(config);
          }
          OSMTileIngester.ingestOsm(inputs, output, config, zoomlevel, protectionLevel,
              providerProperties);
        }
        else
        {
          List<String> inputs = new LinkedList<String>();

          for (String arg: line.getArgs())
          {
            inputs.addAll(getInputs(arg, recurse));
          }

          log.info("Ingest inputs (" + inputs.size() + ")");
          for (String input:inputs)
          {
            log.info("   " + input);
          }

          if (inputs.size() > 0)
          {
            // Ingest non-OSM data
            try
            {
              final boolean success;
              if (line.hasOption("lc"))
              {
                success = IngestVectorDriver.localIngest(inputs.toArray(new String[inputs.size()]), 
                  output, config, zoomlevel, protectionLevel);
              }
              else
              {
                success = IngestVectorDriver.ingest(inputs.toArray(new String[inputs.size()]), 
                  output, config, zoomlevel, protectionLevel, providerProperties);
              }
              if (!success)
              {
                log.error("IngestVector exited with error");
                return 1;
              }

              //            if (!skipPyramids)
              //            {
              //              BuildPyramidDriver.build(output, aggregator, getConf());
              //            }
            }
            catch (Exception e)
            {
              e.printStackTrace();
              log.error("IngestVector exited with error");
              return 1;
            }
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
      log.info("IngestVector complete in " + formatted);

      return 0;
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }

    return -1;

  }


}
