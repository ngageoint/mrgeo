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

package org.mrgeo.geometry.splitter;

import org.joda.time.DateTime;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.mrgeo.geometry.Geometry;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class TimeSpanGeometrySplitter implements GeometrySplitter
{
  public static final String START_TIME_PROPERTY = "startTime";
  public static final String END_TIME_PROPERTY = "endTime";
  public static final String TIME_FORMAT_PROPERTY = "timeFormat";
  public static final String INTERVAL_PROPERTY = "interval";
  public static final String START_FIELD_PROPERTY = "startFieldName";
  public static final String END_FIELD_PROPERTY = "endFieldName";
  public static final String COMPARE_TYPE_PROPERTY = "compareType";

  public enum CompareType { START_TIME, END_TIME, EITHER_TIME };

  private DateTimeFormatter dtf;
  private String startField;
  private String endField;
  private String[] outputs;
  private DateTime[] breakpoints;
  private CompareType compareType = CompareType.END_TIME;

  public TimeSpanGeometrySplitter()
  {
  }

  @Override
  public void initialize(Map<String, String> splitterProperties, final boolean uuidOutputNames,
      final String[] outputNames)
  {
    String timeFormat = splitterProperties.get(TIME_FORMAT_PROPERTY);
    if (timeFormat == null || timeFormat.isEmpty())
    {
      dtf = ISODateTimeFormat.dateTime();
    }
    else
    {
      dtf = DateTimeFormat.forPattern(timeFormat);
    }
    DateTime startTime = getTimeProperty(splitterProperties, START_TIME_PROPERTY, dtf);
    DateTime endTime = getTimeProperty(splitterProperties, END_TIME_PROPERTY, dtf);
    String strInterval = splitterProperties.get(INTERVAL_PROPERTY);
    if (strInterval == null || strInterval.isEmpty())
    {
      throw new IllegalArgumentException("Missing interval property for time span geometry splitter");
    }
    int seconds = -1;
    try
    {
      seconds = Integer.parseInt(strInterval);
    }
    catch(NumberFormatException nfe)
    {
      throw new IllegalArgumentException("Invalid value for interval property for time span geometry splitter");
    }
    Period interval = Period.seconds(seconds);
    startField = splitterProperties.get(START_FIELD_PROPERTY);
    if (startField == null || startField.isEmpty())
    {
      throw new IllegalArgumentException("Missing startField property for time span geometry splitter");
    }
    endField = splitterProperties.get(END_FIELD_PROPERTY);
    if (endField == null || endField.isEmpty())
    {
      throw new IllegalArgumentException("Missing endField property for time span geometry splitter");
    }
    compareType = TimeSpanGeometrySplitter.CompareType.END_TIME;
    String strCompareType = splitterProperties.get(COMPARE_TYPE_PROPERTY);
    if (strCompareType != null && !strCompareType.isEmpty())
    {
      compareType = compareTypeFromString(strCompareType);
    }

    List<DateTime> breakpointsList = new ArrayList<DateTime>();
    DateTime currBreakpoint = startTime;
    while (currBreakpoint.compareTo(endTime) <= 0)
    {
      breakpointsList.add(currBreakpoint);
      currBreakpoint = currBreakpoint.plus(interval);
    }
    // If the endTime is greater than the last breakpoint, then
    // include one more breakpoint.
    if (endTime.compareTo(currBreakpoint.minus(interval)) > 0)
    {
      breakpointsList.add(currBreakpoint);
    }
    if ((outputNames != null) && (breakpointsList.size() != outputNames.length))
    {
      throw new IllegalArgumentException("Invalid set of output names specified for the time span" +
          " geometry splitter. There are " + breakpointsList.size() + " breakpoints, and " + outputNames.length +
          " output names");
    }
    breakpoints = new DateTime[breakpointsList.size()];
    breakpointsList.toArray(breakpoints);
    outputs = new String[breakpoints.length];
    for (int i=0; i < breakpoints.length; i++)
    {
      String output;
      if (outputNames != null)
      {
        output = outputNames[i];
      }
      else
      {
        if (uuidOutputNames)
        {
          output = UUID.randomUUID().toString();
        }
        else
        {
          output = breakpoints[i].toString(dtf);
          // DirectoryMultipleOutputs only allows alphanumeric characters
          output = output.replaceAll("[^\\p{Alnum}]", "");
        }
      }
      outputs[i] = output;
    }
  }

  @Override
  public String[] getAllSplits()
  {
    return outputs.clone();
  }

  @Override
  public List<String> splitGeometry(Geometry geometry) throws IllegalArgumentException
  {
    String startValue = geometry.getAttribute(startField);
    if (startValue == null || startValue.isEmpty())
    {
      throw new IllegalArgumentException("Missing value for " + startField);
    }
    DateTime start = dtf.parseDateTime(startValue);

    String endValue = geometry.getAttribute(endField);
    if (endValue == null || endValue.isEmpty())
    {
      throw new IllegalArgumentException("Missing value for " + endField);
    }
    DateTime end = dtf.parseDateTime(endValue);

    // Which splits does this geometry fall within
    int i = 1;
    List<String> results = new ArrayList<String>();
    while (i < breakpoints.length)
    {
      // Determine the split this geometry belongs to according to the
      // compareType.
      if (compareType == CompareType.START_TIME)
      {
        if (start.compareTo(breakpoints[i-1]) >= 0 && start.compareTo(breakpoints[i]) < 0)
        {
          results.add(outputs[i]);
          // Only one breakpoint applies for this comparisoon type
          break;
        }
      }
      else if (compareType == CompareType.END_TIME)
      {
        if (end.compareTo(breakpoints[i-1]) > 0 && end.compareTo(breakpoints[i]) <= 0)
        {
          results.add(outputs[i]);
          // Only one breakpoint applies for this comparisoon type
          break;
        }
      }
      else
      {
        // The geometry falls within an interval if its start is less than
        // the ending breakpoint for that interval and the end is greater equal
        // than the breakpoint for the previous interval.
        if ((start.compareTo(breakpoints[i]) < 0) && (end.compareTo(breakpoints[i-1]) > 0))
        {
          results.add(outputs[i]);
        }
      }
      i++;
    }
    return results;
  }

  private static DateTime getTimeProperty(final Map<String, String> splitterProperties,
      final String propName, final DateTimeFormatter dtf)
  {
    String strValue = splitterProperties.get(propName);
    DateTime result = null;
    if (strValue == null)
    {
      throw new IllegalArgumentException("Missing " + propName + " property for time span geometry splitter");
    }
    try
    {
      result = dtf.parseDateTime(strValue);
    }
    catch(IllegalArgumentException e)
    {
      throw new IllegalArgumentException("Invalid " + propName + " property for time span geometry splitter");
    }
    return result;
  }

  public static CompareType compareTypeFromString(final String strCompareType)
  {
    CompareType compareType = CompareType.END_TIME;
    if (strCompareType.equalsIgnoreCase("start"))
    {
      compareType = TimeSpanGeometrySplitter.CompareType.START_TIME;
    }
    else if (strCompareType.equalsIgnoreCase("end"))
    {
      compareType = TimeSpanGeometrySplitter.CompareType.END_TIME;
    }
    else if (strCompareType.equalsIgnoreCase("either"))
    {
      compareType = TimeSpanGeometrySplitter.CompareType.EITHER_TIME;
    }
    else
    {
      throw new IllegalArgumentException("Invalid compareType parameter for geometry splitter '" + strCompareType + "'. It must be start, end or either");
    }
    return compareType;
  }

  public static String compareTypeToString(CompareType compareType)
  {
    String strCompareType = "end";
    if (compareType == TimeSpanGeometrySplitter.CompareType.START_TIME)
    {
      strCompareType = "start";
    }
    else if (compareType == TimeSpanGeometrySplitter.CompareType.EITHER_TIME)
    {
      strCompareType = "either";
    }
    return strCompareType;
  }
}
