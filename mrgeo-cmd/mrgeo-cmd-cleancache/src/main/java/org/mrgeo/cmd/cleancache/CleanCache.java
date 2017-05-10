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

import java.io.IOException;
import java.util.GregorianCalendar;

public class CleanCache extends Command
{

private static Logger log = LoggerFactory.getLogger(CleanCache.class);
private Options options;

public CleanCache()
{
}

@Override
public String getUsage() { return "cleancache <options> <operation>"; }

@Override
public void addOptions(Options result)
{
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
}

@Override
public int run(CommandLine line, Configuration conf,
               ProviderProperties providerProperties) throws ParseException
{
  try
  {
    int zoomMin = -1;
    int zoomMax = -1;
    if (line.hasOption("z"))
    {
      try
      {
        String zoomSpec = line.getOptionValue("z", "");
        if (zoomSpec.isEmpty()) {
          throw new ParseException("The zoom argument must either be a single numeric zoom level or a comma-separated min zoom value and max zoom value");
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
              throw new ParseException("The zoom argument must either be a single numeric zoom level or a comma-separated min zoom value and max zoom value");
            }
          }
          else {
            throw new ParseException("The zoom argument must either be a single zoom level or a comma-separated min zoom value and max zoom value");
          }
        }
      }
      catch (NumberFormatException nfe)
      {
        throw new ParseException("Invalid zoom level specified: " + line.getOptionValue("z"));
      }
      if (zoomMin < 1 || zoomMax < 1) {
        throw new ParseException("Zoom level must be at least 1");
      }
      if (zoomMax < zoomMin) {
        throw new ParseException("The maximum zoom level must be >= minimum zoom level");
      }
    }

    long maxCacheSize = -1;
    if (line.hasOption("s")) {
      String sizeSpec = line.getOptionValue("s", "");
      maxCacheSize = (SparkUtils.humantokb(sizeSpec) * 1024L);
      if (maxCacheSize <= 0) {
        throw new ParseException("The maximum cache size cannot be <= 0");
      }
    }
    if (zoomMin > 0 && maxCacheSize > 0) {
      throw new ParseException("You cannot combine zoom options and size options");
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
          throw new ParseException("Invalid age type '" + ageType + "'. Must be 'D' (days), 'd' (days), 'M' (months), 'y' (years), 'H' (hours), 'h' (hours), 'm' (minutes), 'S' (seconds), 's' (milliseconds)");
      }
      try {
        age = Integer.parseInt(ageOption.substring(0, ageOption.length() - 1));
      }
      catch(NumberFormatException e) {
        throw new ParseException("Invalid age " + ageOption.substring(0, ageOption.length() - 1) + ". Age must be a positive integer");
      }
      if (age <= 0) {
        throw new ParseException("The age must be > 0");
      }
    }
    if (age > 0 && (zoomMin > 0 || maxCacheSize > 0)) {
      throw new ParseException("You cannot combine age option with either zoom or size option");
    }

    Bounds bbox = null;
    if (line.hasOption("b")) {
      String bboxSpec = line.getOptionValue("b", "");
      String[] bboxComponents = bboxSpec.split(",");
      if (bboxComponents.length == 4) {
        bbox = Bounds.fromCommaString(bboxSpec);
      }
      else {
        throw new ParseException("You must provide a bounding box with -a like <minx, miny, maxx, maxy>");
      }
    }
    if (bbox != null && (age > 0 || zoomMin > 0 || maxCacheSize > 0)) {
      throw new ParseException("You cannot combine bbox option with zoom, age or size options");
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
      throw new ParseException("Unable to get the default tile size from mrgeo.conf");
    }
    S3Utils.cleanCache(maxCacheSize, age, ageField, bbox, zoomMin, zoomMax, dryrun,
            defaultTilesize, conf, providerProperties);
    return 0;
  }
  catch (IOException e)
  {
    log.error("Exception thrown", e);
  }

  return -1;
}

}
