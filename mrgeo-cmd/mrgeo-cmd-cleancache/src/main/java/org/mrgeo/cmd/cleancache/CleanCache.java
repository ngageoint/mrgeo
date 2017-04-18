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

package org.mrgeo.cmd.cleancache;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.mrgeo.cmd.Command;
import org.mrgeo.cmd.MrGeo;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.utils.S3Utils;
import org.mrgeo.utils.SparkUtils;
import org.mrgeo.utils.logging.LoggingUtils;
import org.mrgeo.utils.tms.Bounds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.GregorianCalendar;

public class CleanCache extends Command
{

private static Logger log = LoggerFactory.getLogger(CleanCache.class);
private Options options;

public CleanCache()
{
  options = createOptions();
}


public static Options createOptions()
{
  Options result = MrGeo.createOptions();

  Option size = new Option("s", "size", true, "Clean out the oldest cached files until the cache is no bigger than the specified size");
  result.addOption(size);
  Option bbox = new Option("b", "bbox", true, "Clean out cached files that intersect the specified bounding box");
  result.addOption(bbox);
  Option age = new Option("a", "age", true, "Clean out cached files that were last accessed before the specified age in days (e.g. 90 days ago)");
  result.addOption(age);
  Option zoom = new Option("z", "zoom", true, "Clean out cached files for the specified zoom level");
  result.addOption(zoom);
  Option dryrun = new Option("dr", "dry-run", false, "Dry run the clean out of cached files without actually deleting them");
  result.addOption(dryrun);

  return result;
}

@Override
public int run(String[] args, Configuration conf, ProviderProperties providerProperties)
{
  try
  {
    CommandLine line = null;
    try
    {
      CommandLineParser parser = new GnuParser();
      line = parser.parse(options, args);
    }
    catch (ParseException e)
    {
      System.out.println(e.getMessage());
      new HelpFormatter().printHelp("cleancache <options> <operation>", options);
      return -1;
    }

    if (line == null || line.getOptions().length == 0 || line.hasOption("h"))
    {
      new HelpFormatter().printHelp("cleancache <options> <operation>", options);
      return -1;
    }

    int zoomMin = -1;
    int zoomMax = -1;
    if (line.hasOption("z"))
    {
      try
      {
        String zoomSpec = line.getOptionValue("z", "");
        if (zoomSpec.isEmpty()) {
          System.err.println("The zoom argument must either be a single numeric zoom level or a comma-separated min zoom value and max zoom value");
          return -1;
        }
        else {
          String[] zoomValues = zoomSpec.split(",");
          if (zoomValues.length == 1 || zoomValues.length == 2) {
            try {
              zoomMin = Integer.parseInt(zoomValues[0]);
              if (zoomValues.length == 2) {
                zoomMax = Integer.parseInt(zoomValues[1]);
              }
              else {
                zoomMax = zoomMin;
              }
            }
            catch(NumberFormatException nfe) {
              System.err.println("The zoom argument must either be a single numeric zoom level or a comma-separated min zoom value and max zoom value");
              return -1;
            }
          }
          else {
            System.err.println("The zoom argument must either be a single zoom level or a comma-separated min zoom value and max zoom value");
            return -1;
          }
        }
      }
      catch (NumberFormatException nfe)
      {
        System.err.println("Invalid zoom level specified: " + line.getOptionValue("z"));
        return -1;
      }
      if (zoomMin < 1 || zoomMax < 1) {
        System.err.println("Zoom level must be at least 1");
        return -1;
      }
      if (zoomMax < zoomMin) {
        System.err.println("The maximum zoom level must be >= minimum zoom level");
        return -1;
      }
    }

    long maxCacheSize = -1;
    if (line.hasOption("s")) {
      String sizeSpec = line.getOptionValue("s", "");
      maxCacheSize = (SparkUtils.humantokb(sizeSpec) * 1024L);
      if (maxCacheSize <= 0) {
        System.err.println("The maximum cache size cannot be <= 0");
        return -1;
      }
    }
    if (zoomMin > 0 && maxCacheSize > 0) {
      System.err.println("You cannot combine zoom options and size options");
      return -1;
    }

    int age = -1;
    int ageField = -1;
    if (line.hasOption("a")) {
      String ageOption = line.getOptionValue("a").trim();
      char ageType = ageOption.charAt(ageOption.length()-1);
      switch(ageType) {
        case 'd':
        case 'D':
          ageField = GregorianCalendar.DAY_OF_YEAR;
          break;
        case 'M':
          ageField = GregorianCalendar.MONTH;
          break;
        case 'y':
          ageField = GregorianCalendar.YEAR;
          break;
        case 'w':
        case 'W':
          ageField = GregorianCalendar.WEEK_OF_YEAR;
          break;
        case 'h':
        case 'H':
          ageField = GregorianCalendar.HOUR;
          break;
        case 'm':
          ageField = GregorianCalendar.MINUTE;
          break;
        case 'S':
          ageField = GregorianCalendar.SECOND;
          break;
        case 's':
          ageField = GregorianCalendar.MILLISECOND;
          break;
        default:
          System.err.println("Invalid age type '" + ageType + "'. Must be 'D' (days), 'd' (days), 'M' (months), 'y' (years), 'H' (hours), 'h' (hours), 'm' (minutes), 'S' (seconds), 's' (milliseconds)");
          return -1;
      }
      try {
        age = Integer.parseInt(ageOption.substring(0, ageOption.length() - 1));
      }
      catch(NumberFormatException e) {
        System.err.println("Invalid age " + ageOption.substring(0, ageOption.length() - 1) + ". Age must be a positive integer");
        return -1;
      }
      if (age <= 0) {
        System.err.println("The age must be > 0");
        return -1;
      }
    }
    if (age > 0 && (zoomMin > 0 || maxCacheSize > 0)) {
      System.err.println("You cannot combine age option with either zoom or size option");
      return -1;
    }

    Bounds bbox = null;
    if (line.hasOption("b")) {
      String bboxSpec = line.getOptionValue("b", "");
      String[] bboxComponents = bboxSpec.split(",");
      if (bboxComponents.length == 4) {
        bbox = Bounds.fromCommaString(bboxSpec);
      }
      else {
        System.err.println("You must provide a bounding box with -a like <minx, miny, maxx, maxy>");
        return -1;
      }
    }
    if (bbox != null && (age > 0 || zoomMin > 0 || maxCacheSize > 0)) {
      System.err.println("You cannot combine bbox option with zoom, age or size options");
      return -1;
    }

    boolean dryrun = line.hasOption("dr");
    if (line.hasOption("v"))
    {
      LoggingUtils.setDefaultLogLevel(LoggingUtils.INFO);
    }
    if (line.hasOption("d"))
    {
      LoggingUtils.setDefaultLogLevel(LoggingUtils.DEBUG);
    }

    // Clean out the cache now.
    int defaultTilesize = -1;
    try {
      defaultTilesize = Integer.parseInt(MrGeoProperties.getInstance().getProperty(
              MrGeoConstants.MRGEO_MRS_TILESIZE,
              MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT));
    }
    catch(NumberFormatException e) {
      System.err.println("Unable to get the default tile size from mrgeo.conf");
      return -1;
    }
    S3Utils.cleanCache(maxCacheSize, age, ageField, bbox, zoomMin, zoomMax, dryrun,
            defaultTilesize, conf, providerProperties);
    return 0;
  }
  catch (Exception e)
  {
    log.error("Exception thrown", e);
  }

  return -1;
}

}
