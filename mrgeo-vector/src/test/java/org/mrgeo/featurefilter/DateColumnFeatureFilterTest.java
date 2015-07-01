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

package org.mrgeo.featurefilter;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.junit.UnitTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("static-method")
public class DateColumnFeatureFilterTest extends ColumnFeatureFilterTest
{
  private static List<Geometry> unfilteredFeatures;
  private static List<Geometry> unfilteredFeatures2;
  private static List<Geometry> unfilteredFeaturesIso;
  
  @BeforeClass
  public static void init() throws IOException, InterruptedException
  {
    unfilteredFeatures = readFeatures("dateTest1.tsv");
    unfilteredFeatures2 = readFeatures("dateTest2.tsv");
    unfilteredFeaturesIso = readFeatures("dateTest3.tsv");
  }
  
  @Test 
  @Category(UnitTest.class)
  public void testDateFilteringWithinADay() throws Exception
  {
    DateColumnFeatureFilter featureFilter = new DateColumnFeatureFilter();
    featureFilter.setFilterColumn("date");
    featureFilter.setFilterValue("12-11-2012");
    featureFilter.setFilterType(ColumnFeatureFilter.FilterType.valueOf("DATE"));
    featureFilter.setDateFormat("MM-dd-yyyy");
    featureFilter.setDateFilterGranularity(
      DateColumnFeatureFilter.DateGranularity.WITHIN_A_DAY);
    List<Geometry> filteredFeatures = new ArrayList<Geometry>();
    for (Geometry unfilteredFeature : unfilteredFeatures)
    {
      Geometry filteredFeature = featureFilter.filter(unfilteredFeature);
      if (filteredFeature != null)
      {
        filteredFeatures.add(filteredFeature);
      }
    }
    Assert.assertTrue(filteredFeatures.size() == 2);
    Assert.assertTrue(filteredFeatures.get(0).getAttribute("date").equals("12-11-2012"));
    
    //same set of tests as previous but with a different date format
    featureFilter = new DateColumnFeatureFilter();
    featureFilter.setFilterColumn("date");
    featureFilter.setFilterValue("2012-12-11");
    featureFilter.setFilterType(ColumnFeatureFilter.FilterType.valueOf("DATE"));
    featureFilter.setDateFormat("yyyy-MM-dd");
    featureFilter.setDateFilterGranularity(
      DateColumnFeatureFilter.DateGranularity.WITHIN_A_DAY);
    filteredFeatures = new ArrayList<Geometry>();
    for (Geometry unfilteredFeature : unfilteredFeatures2)
    {
      Geometry filteredFeature = featureFilter.filter(unfilteredFeature);
      if (filteredFeature != null)
      {
        filteredFeatures.add(filteredFeature);
      }
    }
    Assert.assertTrue(filteredFeatures.size() == 2);
    Assert.assertTrue(filteredFeatures.get(0).getAttribute("date").equals("2012-12-11"));
    
    //more granular ISO-8601 dates
    featureFilter = new DateColumnFeatureFilter();
    featureFilter.setFilterColumn("date");
    featureFilter.setFilterValue("2012-12-11T08:00:00");
    featureFilter.setFilterType(ColumnFeatureFilter.FilterType.valueOf("DATE"));
    featureFilter.setDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    featureFilter.setDateFilterGranularity(
      DateColumnFeatureFilter.DateGranularity.WITHIN_A_DAY);
    filteredFeatures = new ArrayList<Geometry>();
    for (Geometry unfilteredFeature : unfilteredFeaturesIso)
    {
      Geometry filteredFeature = featureFilter.filter(unfilteredFeature);
      if (filteredFeature != null)
      {
        filteredFeatures.add(filteredFeature);
      }
    }
    Assert.assertTrue(filteredFeatures.size() == 2);
    Assert.assertTrue(
      ((String)filteredFeatures.get(0).getAttribute("date")).startsWith("2012-12-11"));
  }
  
  @Test 
  @Category(UnitTest.class)
  public void testDateFilteringWithinAMonth() throws Exception
  {
    DateColumnFeatureFilter featureFilter = new DateColumnFeatureFilter();
    featureFilter.setFilterColumn("date");
    featureFilter.setFilterValue("12-11-2012");
    featureFilter.setFilterType(ColumnFeatureFilter.FilterType.valueOf("DATE"));
    featureFilter.setDateFormat("MM-dd-yyyy");
    featureFilter.setDateFilterGranularity(
      DateColumnFeatureFilter.DateGranularity.WITHIN_A_MONTH);
    List<Geometry> filteredFeatures = new ArrayList<Geometry>();
    for (Geometry unfilteredFeature : unfilteredFeatures)
    {
      Geometry filteredFeature = featureFilter.filter(unfilteredFeature);
      if (filteredFeature != null)
      {
        filteredFeatures.add(filteredFeature);
      }
    }
    Assert.assertTrue(filteredFeatures.size() == 6);
    for (Geometry filteredFeature : filteredFeatures)
    {
      String dateAttribute = (String)filteredFeature.getAttribute("date");
      Assert.assertTrue(dateAttribute.startsWith("12-") || dateAttribute.startsWith("11-"));
    }
    
    //same set of tests as previous but with a different date format
    featureFilter = new DateColumnFeatureFilter();
    featureFilter.setFilterColumn("date");
    featureFilter.setFilterValue("2012-12-11");
    featureFilter.setFilterType(ColumnFeatureFilter.FilterType.valueOf("DATE"));
    featureFilter.setDateFormat("yyyy-MM-dd");
    featureFilter.setDateFilterGranularity(
      DateColumnFeatureFilter.DateGranularity.WITHIN_A_MONTH);
    filteredFeatures = new ArrayList<Geometry>();
    for (Geometry unfilteredFeature : unfilteredFeatures2)
    {
      Geometry filteredFeature = featureFilter.filter(unfilteredFeature);
      if (filteredFeature != null)
      {
        filteredFeatures.add(filteredFeature);
      }
    }
    Assert.assertTrue(filteredFeatures.size() == 6);
    for (Geometry filteredFeature : filteredFeatures)
    {
      String dateAttribute = (String)filteredFeature.getAttribute("date");
      Assert.assertTrue(dateAttribute.contains("-12-") || dateAttribute.contains("-11-"));
    }
    
    //more granular ISO-8601 dates
    featureFilter = new DateColumnFeatureFilter();
    featureFilter.setFilterColumn("date");
    featureFilter.setFilterValue("2012-12-11T08:00:00");
    featureFilter.setFilterType(ColumnFeatureFilter.FilterType.valueOf("DATE"));
    featureFilter.setDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    featureFilter.setDateFilterGranularity(
      DateColumnFeatureFilter.DateGranularity.WITHIN_A_MONTH);
    filteredFeatures = new ArrayList<Geometry>();
    for (Geometry unfilteredFeature : unfilteredFeaturesIso)
    {
      Geometry filteredFeature = featureFilter.filter(unfilteredFeature);
      if (filteredFeature != null)
      {
        filteredFeatures.add(filteredFeature);
      }
    }
    Assert.assertTrue(filteredFeatures.size() == 7);
    for (Geometry filteredFeature : filteredFeatures)
    {
      String dateAttribute = (String)filteredFeature.getAttribute("date");
      Assert.assertTrue(dateAttribute.contains("-12-") || dateAttribute.contains("-11-"));
    }
  }
  
  @Test 
  @Category(UnitTest.class)
  public void testDateFilteringWithinAYear() throws Exception
  {
    DateColumnFeatureFilter featureFilter = new DateColumnFeatureFilter();
    featureFilter.setFilterColumn("date");
    featureFilter.setFilterValue("12-11-2012");
    featureFilter.setFilterType(ColumnFeatureFilter.FilterType.valueOf("DATE"));
    featureFilter.setDateFormat("MM-dd-yyyy");
    featureFilter.setDateFilterGranularity(
      DateColumnFeatureFilter.DateGranularity.WITHIN_A_YEAR);
    List<Geometry> filteredFeatures = new ArrayList<Geometry>();
    for (Geometry unfilteredFeature : unfilteredFeatures)
    {
      Geometry filteredFeature = featureFilter.filter(unfilteredFeature);
      if (filteredFeature != null)
      {
        filteredFeatures.add(filteredFeature);
      }
    }
    Assert.assertTrue(filteredFeatures.size() == 6);
    for (Geometry filteredFeature : filteredFeatures)
    {
      Assert.assertTrue(((String)filteredFeature.getAttribute("date")).endsWith("-2012"));
    }
    
    //same set of tests as previous but with a different date format
    featureFilter = new DateColumnFeatureFilter();
    featureFilter.setFilterColumn("date");
    featureFilter.setFilterValue("2012-12-11");
    featureFilter.setFilterType(ColumnFeatureFilter.FilterType.valueOf("DATE"));
    featureFilter.setDateFormat("yyyy-MM-dd");
    featureFilter.setDateFilterGranularity(
      DateColumnFeatureFilter.DateGranularity.WITHIN_A_YEAR);
    filteredFeatures = new ArrayList<Geometry>();
    for (Geometry unfilteredFeature : unfilteredFeatures2)
    {
      Geometry filteredFeature = featureFilter.filter(unfilteredFeature);
      if (filteredFeature != null)
      {
        filteredFeatures.add(filteredFeature);
      }
    }
    Assert.assertTrue(filteredFeatures.size() == 6);
    for (Geometry filteredFeature : filteredFeatures)
    {
      Assert.assertTrue(((String)filteredFeature.getAttribute("date")).startsWith("2012-"));
    }
    
    //more granular ISO-8601 dates
    featureFilter = new DateColumnFeatureFilter();
    featureFilter.setFilterColumn("date");
    featureFilter.setFilterValue("2012-12-11T08:00:00");
    featureFilter.setFilterType(ColumnFeatureFilter.FilterType.valueOf("DATE"));
    featureFilter.setDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    featureFilter.setDateFilterGranularity(
      DateColumnFeatureFilter.DateGranularity.WITHIN_A_YEAR);
    filteredFeatures = new ArrayList<Geometry>();
    for (Geometry unfilteredFeature : unfilteredFeaturesIso)
    {
      Geometry filteredFeature = featureFilter.filter(unfilteredFeature);
      if (filteredFeature != null)
      {
        filteredFeatures.add(filteredFeature);
      }
    }
    Assert.assertTrue(filteredFeatures.size() == 7);
    for (Geometry filteredFeature : filteredFeatures)
    {
      Assert.assertTrue(((String)filteredFeature.getAttribute("date")).startsWith("2012-"));
    }
  }
  
  @Test 
  @Category(UnitTest.class)
  public void testDateFilteringSameDayOfAnyWeek() throws Exception
  {
    DateColumnFeatureFilter featureFilter = new DateColumnFeatureFilter();
    featureFilter.setFilterColumn("date");
    featureFilter.setFilterValue("12-13-2012");
    featureFilter.setFilterType(ColumnFeatureFilter.FilterType.valueOf("DATE"));
    featureFilter.setDateFormat("MM-dd-yyyy");
    featureFilter.setDateFilterGranularity(
      DateColumnFeatureFilter.DateGranularity.SAME_DAY_OF_ANY_WEEK);
    List<Geometry> filteredFeatures = new ArrayList<Geometry>();
    for (Geometry unfilteredFeature : unfilteredFeatures)
    {
      Geometry filteredFeature = featureFilter.filter(unfilteredFeature);
      if (filteredFeature != null)
      {
        filteredFeatures.add(filteredFeature);
      }
    }
    Assert.assertTrue(filteredFeatures.size() == 2);
    
    //same set of tests as previous but with a different date format
    featureFilter = new DateColumnFeatureFilter();
    featureFilter.setFilterColumn("date");
    featureFilter.setFilterValue("2012-12-13");
    featureFilter.setFilterType(ColumnFeatureFilter.FilterType.valueOf("DATE"));
    featureFilter.setDateFormat("yyyy-MM-dd");
    featureFilter.setDateFilterGranularity(
      DateColumnFeatureFilter.DateGranularity.SAME_DAY_OF_ANY_WEEK);
    filteredFeatures = new ArrayList<Geometry>();
    for (Geometry unfilteredFeature : unfilteredFeatures2)
    {
      Geometry filteredFeature = featureFilter.filter(unfilteredFeature);
      if (filteredFeature != null)
      {
        filteredFeatures.add(filteredFeature);
      }
    }
    Assert.assertTrue(filteredFeatures.size() == 2);
  }
  
  @Test 
  @Category(UnitTest.class)
  public void testDateFilteringSameDayOfAnyMonth() throws Exception
  {
    DateColumnFeatureFilter featureFilter = new DateColumnFeatureFilter();
    featureFilter.setFilterColumn("date");
    featureFilter.setFilterValue("12-13-2012");
    featureFilter.setFilterType(ColumnFeatureFilter.FilterType.valueOf("DATE"));
    featureFilter.setDateFormat("MM-dd-yyyy");
    featureFilter.setDateFilterGranularity(
      DateColumnFeatureFilter.DateGranularity.SAME_DAY_OF_ANY_MONTH);
    List<Geometry> filteredFeatures = new ArrayList<Geometry>();
    for (Geometry unfilteredFeature : unfilteredFeatures)
    {
      Geometry filteredFeature = featureFilter.filter(unfilteredFeature);
      if (filteredFeature != null)
      {
        filteredFeatures.add(filteredFeature);
      }
    }
    Assert.assertTrue(filteredFeatures.size() == 3);
    for (Geometry filteredFeature : filteredFeatures)
    {
      Assert.assertTrue(((String)filteredFeature.getAttribute("date")).contains("-13-"));
    }
    
    //same set of tests as previous but with a different date format
    featureFilter = new DateColumnFeatureFilter();
    featureFilter.setFilterColumn("date");
    featureFilter.setFilterValue("2012-12-13");
    featureFilter.setFilterType(ColumnFeatureFilter.FilterType.valueOf("DATE"));
    featureFilter.setDateFormat("yyyy-MM-dd");
    featureFilter.setDateFilterGranularity(
      DateColumnFeatureFilter.DateGranularity.SAME_DAY_OF_ANY_MONTH);
    filteredFeatures = new ArrayList<Geometry>();
    for (Geometry unfilteredFeature : unfilteredFeatures2)
    {
      Geometry filteredFeature = featureFilter.filter(unfilteredFeature);
      if (filteredFeature != null)
      {
        filteredFeatures.add(filteredFeature);
      }
    }
    Assert.assertTrue(filteredFeatures.size() == 3);
    for (Geometry filteredFeature : filteredFeatures)
    {
      Assert.assertTrue(((String)filteredFeature.getAttribute("date")).endsWith("-13"));
    }
    
    //more granular ISO-8601 dates
    featureFilter = new DateColumnFeatureFilter();
    featureFilter.setFilterColumn("date");
    featureFilter.setFilterValue("2012-12-13T08:00:00");
    featureFilter.setFilterType(ColumnFeatureFilter.FilterType.valueOf("DATE"));
    featureFilter.setDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    featureFilter.setDateFilterGranularity(
      DateColumnFeatureFilter.DateGranularity.SAME_DAY_OF_ANY_MONTH);
    filteredFeatures = new ArrayList<Geometry>();
    for (Geometry unfilteredFeature : unfilteredFeaturesIso)
    {
      Geometry filteredFeature = featureFilter.filter(unfilteredFeature);
      if (filteredFeature != null)
      {
        filteredFeatures.add(filteredFeature);
      }
    }
    Assert.assertTrue(filteredFeatures.size() == 3);
    for (Geometry filteredFeature : filteredFeatures)
    {
      Assert.assertTrue(((String)filteredFeature.getAttribute("date")).contains("-13T"));
    }
  }
  
  @Test 
  @Category(UnitTest.class)
  public void testDateFilteringSameMonthOfAnyYear() throws Exception
  {
    DateColumnFeatureFilter featureFilter = new DateColumnFeatureFilter();
    featureFilter.setFilterColumn("date");
    featureFilter.setFilterValue("11-13-2012");
    featureFilter.setFilterType(ColumnFeatureFilter.FilterType.valueOf("DATE"));
    featureFilter.setDateFormat("MM-dd-yyyy");
    featureFilter.setDateFilterGranularity(
      DateColumnFeatureFilter.DateGranularity.SAME_MONTH_OF_ANY_YEAR);
    List<Geometry> filteredFeatures = new ArrayList<Geometry>();
    for (Geometry unfilteredFeature : unfilteredFeatures)
    {
      Geometry filteredFeature = featureFilter.filter(unfilteredFeature);
      if (filteredFeature != null)
      {
        filteredFeatures.add(filteredFeature);
      }
    }
    Assert.assertTrue(filteredFeatures.size() == 2);
    for (Geometry filteredFeature : filteredFeatures)
    {
      Assert.assertTrue(((String)filteredFeature.getAttribute("date")).startsWith("11-"));
    }
    
    //same set of tests as previous but with a different date format
    featureFilter = new DateColumnFeatureFilter();
    featureFilter.setFilterColumn("date");
    featureFilter.setFilterValue("2012-11-13");
    featureFilter.setFilterType(ColumnFeatureFilter.FilterType.valueOf("DATE"));
    featureFilter.setDateFormat("yyyy-MM-dd");
    featureFilter.setDateFilterGranularity(
      DateColumnFeatureFilter.DateGranularity.SAME_MONTH_OF_ANY_YEAR);
    filteredFeatures = new ArrayList<Geometry>();
    for (Geometry unfilteredFeature : unfilteredFeatures2)
    {
      Geometry filteredFeature = featureFilter.filter(unfilteredFeature);
      if (filteredFeature != null)
      {
        filteredFeatures.add(filteredFeature);
      }
    }
    Assert.assertTrue(filteredFeatures.size() == 2);
    for (Geometry filteredFeature : filteredFeatures)
    {
      Assert.assertTrue(((String)filteredFeature.getAttribute("date")).contains("-11-"));
    }
    
    //more granular ISO-8601 dates
    featureFilter = new DateColumnFeatureFilter();
    featureFilter.setFilterColumn("date");
    featureFilter.setFilterValue("2012-11-13T08:00:00");
    featureFilter.setFilterType(ColumnFeatureFilter.FilterType.valueOf("DATE"));
    featureFilter.setDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    featureFilter.setDateFilterGranularity(
      DateColumnFeatureFilter.DateGranularity.SAME_MONTH_OF_ANY_YEAR);
    filteredFeatures = new ArrayList<Geometry>();
    for (Geometry unfilteredFeature : unfilteredFeaturesIso)
    {
      Geometry filteredFeature = featureFilter.filter(unfilteredFeature);
      if (filteredFeature != null)
      {
        filteredFeatures.add(filteredFeature);
      }
    }
    Assert.assertTrue(filteredFeatures.size() == 3);
    for (Geometry filteredFeature : filteredFeatures)
    {
      Assert.assertTrue(((String)filteredFeature.getAttribute("date")).contains("-11-"));
    }
  }
  
  @Test 
  @Category(UnitTest.class)
  public void testDateFilteringSameDayOfAnyYear() throws Exception
  {
    DateColumnFeatureFilter featureFilter = new DateColumnFeatureFilter();
    featureFilter.setFilterColumn("date");
    featureFilter.setFilterValue("11-13-2012");
    featureFilter.setFilterType(ColumnFeatureFilter.FilterType.valueOf("DATE"));
    featureFilter.setDateFormat("MM-dd-yyyy");
    featureFilter.setDateFilterGranularity(
      DateColumnFeatureFilter.DateGranularity.SAME_DAY_OF_ANY_YEAR);
    List<Geometry> filteredFeatures = new ArrayList<Geometry>();
    for (Geometry unfilteredFeature : unfilteredFeatures)
    {
      Geometry filteredFeature = featureFilter.filter(unfilteredFeature);
      if (filteredFeature != null)
      {
        filteredFeatures.add(filteredFeature);
      }
    }
    Assert.assertTrue(filteredFeatures.size() == 2);
    for (Geometry filteredFeature : filteredFeatures)
    {
      Assert.assertTrue(((String)filteredFeature.getAttribute("date")).contains("-13-"));
    }
    
    //same set of tests as previous but with a different date format
    featureFilter = new DateColumnFeatureFilter();
    featureFilter.setFilterColumn("date");
    featureFilter.setFilterValue("2012-11-13");
    featureFilter.setFilterType(ColumnFeatureFilter.FilterType.valueOf("DATE"));
    featureFilter.setDateFormat("yyyy-MM-dd");
    featureFilter.setDateFilterGranularity(
      DateColumnFeatureFilter.DateGranularity.SAME_DAY_OF_ANY_YEAR);
    filteredFeatures = new ArrayList<Geometry>();
    for (Geometry unfilteredFeature : unfilteredFeatures2)
    {
      Geometry filteredFeature = featureFilter.filter(unfilteredFeature);
      if (filteredFeature != null)
      {
        filteredFeatures.add(filteredFeature);
      }
    }
    Assert.assertTrue(filteredFeatures.size() == 2);
    for (Geometry filteredFeature : filteredFeatures)
    {
      Assert.assertTrue(((String)filteredFeature.getAttribute("date")).endsWith("-13"));
    }
    
    //more granular ISO-8601 dates
    featureFilter = new DateColumnFeatureFilter();
    featureFilter.setFilterColumn("date");
    featureFilter.setFilterValue("2012-11-13T08:00:00");
    featureFilter.setFilterType(ColumnFeatureFilter.FilterType.valueOf("DATE"));
    featureFilter.setDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    featureFilter.setDateFilterGranularity(
      DateColumnFeatureFilter.DateGranularity.SAME_DAY_OF_ANY_YEAR);
    filteredFeatures = new ArrayList<Geometry>();
    for (Geometry unfilteredFeature : unfilteredFeaturesIso)
    {
      Geometry filteredFeature = featureFilter.filter(unfilteredFeature);
      if (filteredFeature != null)
      {
        filteredFeatures.add(filteredFeature);
      }
    }
    Assert.assertTrue(filteredFeatures.size() == 2);
    for (Geometry filteredFeature : filteredFeatures)
    {
      Assert.assertTrue(((String)filteredFeature.getAttribute("date")).contains("-13T"));
    }
  }
  
  @Test 
  @Category(UnitTest.class)
  public void testDateFilteringBefore() throws Exception
  {
    DateColumnFeatureFilter featureFilter = new DateColumnFeatureFilter();
    featureFilter.setFilterColumn("date");
    featureFilter.setFilterValue("11-13-2012");
    featureFilter.setFilterType(ColumnFeatureFilter.FilterType.valueOf("DATE"));
    featureFilter.setDateFormat("MM-dd-yyyy");
    featureFilter.setDateFilterGranularity(
      DateColumnFeatureFilter.DateGranularity.BEFORE);
    List<Geometry> filteredFeatures = new ArrayList<Geometry>();
    for (Geometry unfilteredFeature : unfilteredFeatures)
    {
      Geometry filteredFeature = featureFilter.filter(unfilteredFeature);
      if (filteredFeature != null)
      {
        filteredFeatures.add(filteredFeature);
      }
    }
    Assert.assertTrue(filteredFeatures.size() == 1);
    
    //same set of tests as previous but with a different date format
    featureFilter = new DateColumnFeatureFilter();
    featureFilter.setFilterColumn("date");
    featureFilter.setFilterValue("2012-11-13");
    featureFilter.setFilterType(ColumnFeatureFilter.FilterType.valueOf("DATE"));
    featureFilter.setDateFormat("yyyy-MM-dd");
    featureFilter.setDateFilterGranularity(
      DateColumnFeatureFilter.DateGranularity.BEFORE);
    filteredFeatures = new ArrayList<Geometry>();
    for (Geometry unfilteredFeature : unfilteredFeatures2)
    {
      Geometry filteredFeature = featureFilter.filter(unfilteredFeature);
      if (filteredFeature != null)
      {
        filteredFeatures.add(filteredFeature);
      }
    }
    Assert.assertTrue(filteredFeatures.size() == 1);
    
    //more granular ISO-8601 dates
    featureFilter = new DateColumnFeatureFilter();
    featureFilter.setFilterColumn("date");
    featureFilter.setFilterValue("2012-11-13T08:00:00");
    featureFilter.setFilterType(ColumnFeatureFilter.FilterType.valueOf("DATE"));
    featureFilter.setDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    featureFilter.setDateFilterGranularity(
      DateColumnFeatureFilter.DateGranularity.BEFORE);
    filteredFeatures = new ArrayList<Geometry>();
    for (Geometry unfilteredFeature : unfilteredFeaturesIso)
    {
      Geometry filteredFeature = featureFilter.filter(unfilteredFeature);
      if (filteredFeature != null)
      {
        filteredFeatures.add(filteredFeature);
      }
    }
    Assert.assertTrue(filteredFeatures.size() == 3);
  }
  
  @Test 
  @Category(UnitTest.class)
  public void testDateFilteringAfter() throws Exception
  {
    DateColumnFeatureFilter featureFilter = new DateColumnFeatureFilter();
    featureFilter.setFilterColumn("date");
    featureFilter.setFilterValue("11-13-2012");
    featureFilter.setFilterType(ColumnFeatureFilter.FilterType.valueOf("DATE"));
    featureFilter.setDateFormat("MM-dd-yyyy");
    featureFilter.setDateFilterGranularity(
      DateColumnFeatureFilter.DateGranularity.AFTER);
    List<Geometry> filteredFeatures = new ArrayList<Geometry>();
    for (Geometry unfilteredFeature : unfilteredFeatures)
    {
      Geometry filteredFeature = featureFilter.filter(unfilteredFeature);
      if (filteredFeature != null)
      {
        filteredFeatures.add(filteredFeature);
      }
    }
    Assert.assertTrue(filteredFeatures.size() == 5);
    
    //same set of tests as previous but with a different date format
    featureFilter = new DateColumnFeatureFilter();
    featureFilter.setFilterColumn("date");
    featureFilter.setFilterValue("2012-11-13");
    featureFilter.setFilterType(ColumnFeatureFilter.FilterType.valueOf("DATE"));
    featureFilter.setDateFormat("yyyy-MM-dd");
    featureFilter.setDateFilterGranularity(
      DateColumnFeatureFilter.DateGranularity.AFTER);
    filteredFeatures = new ArrayList<Geometry>();
    for (Geometry unfilteredFeature : unfilteredFeatures2)
    {
      Geometry filteredFeature = featureFilter.filter(unfilteredFeature);
      if (filteredFeature != null)
      {
        filteredFeatures.add(filteredFeature);
      }
    }
    Assert.assertTrue(filteredFeatures.size() == 5);
    
    //more granular ISO-8601 dates
    featureFilter = new DateColumnFeatureFilter();
    featureFilter.setFilterColumn("date");
    featureFilter.setFilterValue("2012-11-13T08:00:00");
    featureFilter.setFilterType(ColumnFeatureFilter.FilterType.valueOf("DATE"));
    featureFilter.setDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    featureFilter.setDateFilterGranularity(
      DateColumnFeatureFilter.DateGranularity.AFTER);
    filteredFeatures = new ArrayList<Geometry>();
    for (Geometry unfilteredFeature : unfilteredFeaturesIso)
    {
      Geometry filteredFeature = featureFilter.filter(unfilteredFeature);
      if (filteredFeature != null)
      {
        filteredFeatures.add(filteredFeature);
      }
    }
    Assert.assertTrue(filteredFeatures.size() == 5);
  }
  
  @Test 
  @Category(UnitTest.class)
  public void testDateFilteringSameInstant() throws Exception
  {
    //more granular ISO-8601 dates
    DateColumnFeatureFilter featureFilter = new DateColumnFeatureFilter();
    featureFilter.setFilterColumn("date");
    featureFilter.setFilterValue("2012-12-11T08:00:00");
    featureFilter.setFilterType(ColumnFeatureFilter.FilterType.valueOf("DATE"));
    featureFilter.setDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    featureFilter.setDateFilterGranularity(
      DateColumnFeatureFilter.DateGranularity.SAME_INSTANT);
    List<Geometry> filteredFeatures = new ArrayList<Geometry>();
    for (Geometry unfilteredFeature : unfilteredFeaturesIso)
    {
      Geometry filteredFeature = featureFilter.filter(unfilteredFeature);
      if (filteredFeature != null)
      {
        filteredFeatures.add(filteredFeature);
      }
    }
    Assert.assertTrue(filteredFeatures.size() == 1);
    Assert.assertTrue(filteredFeatures.get(0).getAttribute("date").equals("2012-12-11T08:00:00"));
  }
  
  @Test 
  @Category(UnitTest.class)
  public void testDateFilteringWithinAMinute() throws Exception
  {
    //more granular ISO-8601 dates
    DateColumnFeatureFilter featureFilter = new DateColumnFeatureFilter();
    featureFilter.setFilterColumn("date");
    featureFilter.setFilterValue("2012-12-11T08:00:00");
    featureFilter.setFilterType(ColumnFeatureFilter.FilterType.valueOf("DATE"));
    featureFilter.setDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    featureFilter.setDateFilterGranularity(
      DateColumnFeatureFilter.DateGranularity.WITHIN_A_MINUTE);
    List<Geometry> filteredFeatures = new ArrayList<Geometry>();
    for (Geometry unfilteredFeature : unfilteredFeaturesIso)
    {
      Geometry filteredFeature = featureFilter.filter(unfilteredFeature);
      if (filteredFeature != null)
      {
        filteredFeatures.add(filteredFeature);
      }
    }
    Assert.assertTrue(filteredFeatures.size() == 2);
    for (Geometry filteredFeature : filteredFeatures)
    {
      Assert.assertTrue(((String)filteredFeature.getAttribute("date")).contains("T08:00:"));
    }
  }
  
  @Test 
  @Category(UnitTest.class)
  public void testDateFilteringWithinAnHour() throws Exception
  {
    //more granular ISO-8601 dates
    DateColumnFeatureFilter featureFilter = new DateColumnFeatureFilter();
    featureFilter.setFilterColumn("date");
    featureFilter.setFilterValue("2012-12-11T08:00:00");
    featureFilter.setFilterType(ColumnFeatureFilter.FilterType.valueOf("DATE"));
    featureFilter.setDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    featureFilter.setDateFilterGranularity(
      DateColumnFeatureFilter.DateGranularity.WITHIN_AN_HOUR);
    List<Geometry> filteredFeatures = new ArrayList<Geometry>();
    for (Geometry unfilteredFeature : unfilteredFeaturesIso)
    {
      Geometry filteredFeature = featureFilter.filter(unfilteredFeature);
      if (filteredFeature != null)
      {
        filteredFeatures.add(filteredFeature);
      }
    }
    Assert.assertTrue(filteredFeatures.size() == 2);
    for (Geometry filteredFeature : filteredFeatures)
    {
      Assert.assertTrue(((String)filteredFeature.getAttribute("date")).contains("T08:00:0"));
    }
  }
  
  @Test 
  @Category(UnitTest.class)
  public void testDateFilteringSameMinuteOfAnyHour() throws Exception
  {
    //more granular ISO-8601 dates
    DateColumnFeatureFilter featureFilter = new DateColumnFeatureFilter();
    featureFilter.setFilterColumn("date");
    featureFilter.setFilterValue("2012-12-11T08:00:00");
    featureFilter.setFilterType(ColumnFeatureFilter.FilterType.valueOf("DATE"));
    featureFilter.setDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    featureFilter.setDateFilterGranularity(
      DateColumnFeatureFilter.DateGranularity.SAME_MINUTE_OF_ANY_HOUR);
    List<Geometry> filteredFeatures = new ArrayList<Geometry>();
    for (Geometry unfilteredFeature : unfilteredFeaturesIso)
    {
      Geometry filteredFeature = featureFilter.filter(unfilteredFeature);
      if (filteredFeature != null)
      {
        filteredFeatures.add(filteredFeature);
      }
    }
    Assert.assertTrue(filteredFeatures.size() == 7);
    for (Geometry filteredFeature : filteredFeatures)
    {
      Assert.assertTrue(((String)filteredFeature.getAttribute("date")).contains(":00:"));
    }
  }
  
  @Test 
  @Category(UnitTest.class)
  public void testDateFilteringInvalidDateFormat() throws Exception
  {
    boolean caughtException = false;
    try
    {
      //invalid date format
      DateColumnFeatureFilter featureFilter = new DateColumnFeatureFilter();
      featureFilter.setDateFormat("foo");
    }
    catch (IllegalArgumentException e)
    {
      caughtException = true;
    }
    Assert.assertTrue(caughtException);
  } 
}
