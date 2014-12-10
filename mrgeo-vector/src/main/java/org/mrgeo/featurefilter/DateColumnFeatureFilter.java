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

/**
 * 
 */
package org.mrgeo.featurefilter;

import org.apache.commons.lang.StringUtils;
import org.joda.time.*;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.mrgeo.geometry.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Allows for filtering vector features by advanced date comparison functions
 */
public class DateColumnFeatureFilter extends ColumnFeatureFilter
{
  private static final long serialVersionUID = 1L;

  private static final Logger log = LoggerFactory.getLogger(DateColumnFeatureFilter.class);
  
  private String dateFormat = "MM-dd-yyyy";
  public String getDateFormat() { return dateFormat; }
  public void setDateFormat(String dateFormat) 
  { 
    DateTimeFormat.forPattern(dateFormat);  //throws an exception if the pattern is invalid
    this.dateFormat = dateFormat; 
  } 
  
  public enum DateGranularity 
  {
    AFTER,  //any time after the filter
    BEFORE, //any time before the filter
    SAME_DAY_OF_ANY_MONTH, //matching day of month in any month
    SAME_DAY_OF_ANY_WEEK, //matching day of week in any week
    SAME_DAY_OF_ANY_YEAR, //matching date of year in any year
    SAME_HOUR_OF_ANY_DAY,  //matching hour of day in any day
    SAME_INSTANT,  //matches the exact instant in time
    SAME_MINUTE_OF_ANY_HOUR, //matching minute in any hour
    SAME_MONTH_OF_ANY_YEAR, //matching month in year in any year
    WITHIN_A_DAY,  //within a day of each other
    WITHIN_AN_HOUR, //within an hour of each other
    WITHIN_A_MINUTE,   //within a minute of each other
    WITHIN_A_MONTH, //within a month of each other
    WITHIN_A_YEAR //within a year of each other
  }
  private DateGranularity dateFilterGranularity = DateGranularity.SAME_INSTANT;
  public DateGranularity getDateFilterGranularity() { return dateFilterGranularity; }
  public void setDateFilterGranularity(DateGranularity dateFilterGranularity) 
  { this.dateFilterGranularity = dateFilterGranularity; }
  
  /* (non-Javadoc)
   * @see org.mrgeo.featurefilter.BaseFeatureFilter#filterInPlace(com.vividsolutions.jump.feature.Feature)
   */
  @Override
  public Geometry filterInPlace(Geometry feature)
  {
    //should have already set a date format by this point for date filtering
    assert(!StringUtils.isEmpty(dateFormat));
    assert(dateFilterGranularity != null);
    
    feature = super.filterInPlace(feature);
    if (feature == null)
    {
      return null;
    }

    try
    {
      DateTimeFormatter dateFormatter = DateTimeFormat.forPattern(dateFormat);
      DateTime filterDate = dateFormatter.parseDateTime(filterValue);
      DateTime featureAttributeDate = 
        dateFormatter.parseDateTime(featureAttributeValue);
      if (datePassesFilter(featureAttributeDate, filterDate))
      {
        return feature;
      }
    }
    catch (IllegalArgumentException e)
    {
      //couldn't parse the feature's date
      log.debug("Invalid date: " + featureAttributeValue + " for column: " + filterColumn);
      return null;
    }
    
    return null;
  }
  
  private boolean datePassesFilter(DateTime date, DateTime filter)
  {
    return 
      ((dateFilterGranularity.equals(DateGranularity.AFTER) && date.isAfter(filter)) ||
       (dateFilterGranularity.equals(DateGranularity.BEFORE) && date.isBefore(filter)) ||
       (dateFilterGranularity.equals(DateGranularity.SAME_INSTANT) && filter.isEqual(date)) ||
       (dateFilterGranularity.equals(DateGranularity.WITHIN_A_MINUTE) &&
         (Minutes.minutesBetween(filter, date).getMinutes() == 0)) ||
       (dateFilterGranularity.equals(DateGranularity.WITHIN_AN_HOUR) &&
         (Hours.hoursBetween(filter, date).getHours() == 0)) ||
       (dateFilterGranularity.equals(DateGranularity.WITHIN_A_DAY) &&
         (Days.daysBetween(filter, date).getDays() == 0)) ||
       (dateFilterGranularity.equals(DateGranularity.WITHIN_A_MONTH) &&
         (Months.monthsBetween(filter, date).getMonths() == 0)) ||
       (dateFilterGranularity.equals(DateGranularity.WITHIN_A_YEAR) &&
         (Years.yearsBetween(filter, date).getYears() == 0)) ||
       (dateFilterGranularity.equals(DateGranularity.SAME_MINUTE_OF_ANY_HOUR) &&
         (date.getMinuteOfHour() == filter.getMinuteOfHour())) ||
       (dateFilterGranularity.equals(DateGranularity.SAME_HOUR_OF_ANY_DAY) &&
         (date.getHourOfDay() == filter.getHourOfDay())) ||
       (dateFilterGranularity.equals(DateGranularity.SAME_DAY_OF_ANY_WEEK) &&
         (date.getDayOfWeek() == filter.getDayOfWeek())) ||
       (dateFilterGranularity.equals(DateGranularity.SAME_DAY_OF_ANY_MONTH) &&
         (date.getDayOfMonth() == filter.getDayOfMonth())) ||
       //date.getDayOfYear isn't sufficient here, b/c leap years have a different number of days
       (dateFilterGranularity.equals(DateGranularity.SAME_DAY_OF_ANY_YEAR) &&
         ((date.getDayOfMonth() == filter.getDayOfMonth()) && 
          (date.getMonthOfYear() == filter.getMonthOfYear()))) ||
       (dateFilterGranularity.equals(DateGranularity.SAME_MONTH_OF_ANY_YEAR) &&
         (date.getMonthOfYear() == filter.getMonthOfYear())));
  }
}
