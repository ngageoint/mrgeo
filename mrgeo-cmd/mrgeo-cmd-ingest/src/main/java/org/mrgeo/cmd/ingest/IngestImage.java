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

package org.mrgeo.cmd.ingest;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.joda.time.Period;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;
import org.mrgeo.aggregators.*;
import org.mrgeo.buildpyramid.BuildPyramid;
import org.mrgeo.cmd.Command;
import org.mrgeo.cmd.MrGeo;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.ingest.IngestInputProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class IngestImage extends Command
{

private static Logger log = LoggerFactory.getLogger(IngestImage.class);
private Options options;
private double[] nodata;
private int bands = -1;
private int tiletype = -1;
private boolean skippreprocessing = false;
private boolean local = false;
private boolean quick = false;
private Map<String, String> tags = new HashMap<>();
private boolean firstInput = true;

public IngestImage()
{
  options = createOptions();
}


public static Options createOptions()
{
  Options result = MrGeo.createOptions();

  Option output = new Option("o", "output", true, "MrsPyramid image name");
  output.setRequired(true);
  result.addOption(output);

  Option cat = new Option("c", "categorical", false, "Input [pixels] are categorical values");
  cat.setRequired(false);
  result.addOption(cat);

  Option skipCat = new Option("sc", "skip-cat-load", false, "Do not load categories from source");
  skipCat.setRequired(false);
  result.addOption(skipCat);

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

  Option minavgpair =
      new Option("minavgpair", "miminumaveragepair", false, "Minimum Average Pair Pyramid Pixel Resampling Method");
  minavgpair.setRequired(false);
  aggregators.addOption(minavgpair);

  result.addOptionGroup(aggregators);

  Option local = new Option("lc", "local", false, "Use local files for ingest, good for small ingests");
  local.setRequired(false);
  result.addOption(local);

  Option quick = new Option("q", "quick", false, "Quick ingest (for small files only)");
  quick.setRequired(false);
  result.addOption(quick);

  Option zoom = new Option("z", "zoom", true, "force zoom level");
  zoom.setRequired(false);
  result.addOption(zoom);

  Option skippre = new Option("sk", "skippreprocessing", false, "Skip the preprocessing step (must specify zoom)");
  skippre.setRequired(false);
  result.addOption(skippre);

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

  return result;
}

@Override
@SuppressWarnings("squid:S1166") // Exceptions caught and error message printed
public int run(String[] args, Configuration conf, ProviderProperties providerProperties)
{
  try
  {
    long start = System.currentTimeMillis();

    CommandLine line;
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


    if (line == null || line.hasOption("h"))
    {
      new HelpFormatter().printHelp("ingest <options> <input>", options);
      return -1;
    }


    boolean overrideNodata = line.hasOption("nd");
    double[] nodataOverride = null;
    if (overrideNodata)
    {
      String str = line.getOptionValue("nd");
      String[] strElements = str.split(",");
      nodataOverride = new double[strElements.length];
      for (int i = 0; i < nodataOverride.length; i++)
      {
        try
        {
          nodataOverride[i] = parseNoData(strElements[i]);
        }
        catch (NumberFormatException nfe)
        {
          System.out.println("Invalid nodata value: " + strElements[i]);
          return -1;
        }
      }
    }


    boolean categorical = line.hasOption("c");
    boolean skipCatLoad = line.hasOption("sc");
    boolean skipPyramids = line.hasOption("sp");
    boolean recurse = !line.hasOption("nr");

    skippreprocessing = line.hasOption("sk");
    String output = line.getOptionValue("o");

    log.debug("categorical: " + categorical);
    log.debug("skip category loading: " + skipCatLoad);
    log.debug("skip pyramids: " + skipPyramids);
    log.debug("output: " + output);

    ArrayList<String> inputs = new ArrayList<String>();


    int zoomlevel = -1;
    if (line.hasOption("z"))
    {
      zoomlevel = Integer.parseInt(line.getOptionValue("z"));
    }

    if (skippreprocessing && zoomlevel < 1)
    {
      log.error("Need to specify zoomlevel to skip preprocessing");
      return -1;
    }
    IngestInputProcessor iip = new IngestInputProcessor(conf, nodataOverride, zoomlevel, skippreprocessing);

    try
    {
      for (String arg : line.getArgs())
      {
        iip.processInput(arg, recurse);
      }
      inputs.addAll(JavaConversions.asJavaCollection(iip.getInputs()));
    }
    catch (IllegalArgumentException e)
    {
      System.out.println(e.getMessage());
      return -1;
    }

    log.info("Ingest inputs (" + inputs.size() + ")");
    for (String input : inputs)
    {
      log.info("   " + input);
    }

    if (line.hasOption("t"))
    {
      String rawTags = line.getOptionValue("t");

      String splittags[] = rawTags.split(",");
      for (String t : splittags)
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

    quick = quick || line.hasOption("q");
    local = local || line.hasOption("lc");

    String protectionLevel = line.getOptionValue("pl");

    if (inputs.size() > 0)
    {
      try
      {
        final boolean success;
        if (quick)
        {
//            success = IngestImage.quickIngest(inputs.get(0), output, categorical,
//                conf, overrideNodata, nodata, tags, protectionLevel, providerProperties);
          log.error("Quick Ingest is not yet implemented");
          return -1;
        }
        else if (local)
        {
          success = org.mrgeo.ingest.IngestImage.localIngest(inputs.toArray(new String[inputs.size()]),
              output, categorical, skipCatLoad, conf, iip.getBounds(), iip.getZoomlevel(), iip.tilesize(),
              iip.getNodata(), iip.getBands(), iip.getTiletype(),
              tags, protectionLevel, providerProperties);
        }
        else
        {
          success = org.mrgeo.ingest.IngestImage.ingest(inputs.toArray(new String[inputs.size()]),
              output, categorical, skipCatLoad, conf, iip.getBounds(), iip.getZoomlevel(), iip.tilesize(),
              iip.getNodata(), iip.getBands(),
              iip.getTiletype(), tags, protectionLevel, providerProperties);
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

          BuildPyramid.build(output, aggregator, conf, providerProperties);
        }
      }
      catch (Exception e)
      {
        log.error("IngestImage exited with error", e);
        return 1;
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
    log.error("IngestImage exited with error", e);
  }

  return -1;
}

private double parseNoData(String fromArg) throws NumberFormatException
{
  String arg = fromArg.trim();
  if (arg.compareToIgnoreCase("nan") != 0)
  {
    return Double.parseDouble(arg);
  }
  else
  {
    return Double.NaN;
  }
}
}
