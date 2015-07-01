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
public class TextColumnFeatureFilterTest extends ColumnFeatureFilterTest
{
  private static List<Geometry> unfilteredFeatures;
  
  @BeforeClass
  public static void init() throws IOException, InterruptedException
  {
    unfilteredFeatures = readFeatures("idTest1.tsv");
  }
  
  @Test 
  @Category(UnitTest.class)
  public void testStringFilteringExact() throws Exception
  {
    TextColumnFeatureFilter featureFilter = new TextColumnFeatureFilter();
    featureFilter.setFilterColumn("id");
    featureFilter.setFilterValue("1");
    featureFilter.setFilterType(ColumnFeatureFilter.FilterType.valueOf("TEXT"));
    featureFilter.setParsingMethod(TextColumnFeatureFilter.ParsingMethod.valueOf("EXACT"));
    List<Geometry> filteredFeatures = new ArrayList<Geometry>();
    for (Geometry unfilteredFeature : unfilteredFeatures)
    {
      Geometry filteredFeature = featureFilter.filter(unfilteredFeature);
      if (filteredFeature != null)
      {
        filteredFeatures.add(filteredFeature);
      }
    }
    Assert.assertEquals(filteredFeatures.size(), 1);
    Assert.assertNotNull(filteredFeatures.get(0));
    Assert.assertEquals(filteredFeatures.get(0).getAttribute("id"), "1");
    
    featureFilter = new TextColumnFeatureFilter();
    featureFilter.setFilterColumn("id");
    featureFilter.setFilterValue("test");
    featureFilter.setFilterType(ColumnFeatureFilter.FilterType.valueOf("TEXT"));
    featureFilter.setParsingMethod(TextColumnFeatureFilter.ParsingMethod.valueOf("EXACT"));
    filteredFeatures = new ArrayList<Geometry>();
    for (Geometry unfilteredFeature : unfilteredFeatures)
    {
      Geometry filteredFeature = featureFilter.filter(unfilteredFeature);
      if (filteredFeature != null)
      {
        filteredFeatures.add(filteredFeature);
      }
    }
    Assert.assertEquals(filteredFeatures.size(), 1);
    
    featureFilter = new TextColumnFeatureFilter();
    featureFilter.setFilterColumn("id");
    featureFilter.setFilterValue("*test*");
    featureFilter.setFilterType(ColumnFeatureFilter.FilterType.valueOf("TEXT"));
    featureFilter.setParsingMethod(TextColumnFeatureFilter.ParsingMethod.valueOf("EXACT"));
    filteredFeatures = new ArrayList<Geometry>();
    for (Geometry unfilteredFeature : unfilteredFeatures)
    {
      Geometry filteredFeature = featureFilter.filter(unfilteredFeature);
      if (filteredFeature != null)
      {
        filteredFeatures.add(filteredFeature);
      }
    }
    Assert.assertEquals(filteredFeatures.size(), 0);
    
    //this filter should not return any records - record value doesn't exist
    featureFilter = new TextColumnFeatureFilter();
    featureFilter.setFilterColumn("id");
    featureFilter.setFilterValue("4");
    featureFilter.setFilterType(ColumnFeatureFilter.FilterType.valueOf("TEXT"));
    featureFilter.setParsingMethod(TextColumnFeatureFilter.ParsingMethod.valueOf("EXACT"));
    filteredFeatures = new ArrayList<Geometry>();
    for (Geometry unfilteredFeature : unfilteredFeatures)
    {
      Geometry filteredFeature = featureFilter.filter(unfilteredFeature);
      if (filteredFeature != null)
      {
        filteredFeatures.add(filteredFeature);
      }
    }
    Assert.assertEquals(filteredFeatures.size(), 0);
    
    //this filter should not return any records - invalid column name
    featureFilter = new TextColumnFeatureFilter();
    featureFilter.setFilterColumn("id2");
    featureFilter.setFilterValue("1");
    featureFilter.setFilterType(ColumnFeatureFilter.FilterType.valueOf("TEXT"));
    featureFilter.setParsingMethod(TextColumnFeatureFilter.ParsingMethod.valueOf("EXACT"));
    filteredFeatures = new ArrayList<Geometry>();
    for (Geometry unfilteredFeature : unfilteredFeatures)
    {
      Geometry filteredFeature = featureFilter.filter(unfilteredFeature);
      if (filteredFeature != null)
      {
        filteredFeatures.add(filteredFeature);
      }
    }
    Assert.assertEquals(filteredFeatures.size(), 0);
  }
  
  @Test 
  @Category(UnitTest.class)
  public void testStringFilteringWildcard() throws Exception
  {
    TextColumnFeatureFilter featureFilter = new TextColumnFeatureFilter();
    featureFilter.setFilterColumn("id");
    featureFilter.setFilterValue("1");
    featureFilter.setFilterType(ColumnFeatureFilter.FilterType.valueOf("TEXT"));
    featureFilter.setParsingMethod(TextColumnFeatureFilter.ParsingMethod.valueOf("WILDCARD"));
    List<Geometry> filteredFeatures = new ArrayList<Geometry>();
    for (Geometry unfilteredFeature : unfilteredFeatures)
    {
      Geometry filteredFeature = featureFilter.filter(unfilteredFeature);
      if (filteredFeature != null)
      {
        filteredFeatures.add(filteredFeature);
      }
    }
    Assert.assertEquals(filteredFeatures.size(), 1);
    Assert.assertNotNull(filteredFeatures.get(0));
    Assert.assertEquals(filteredFeatures.get(0).getAttribute("id"), "1");
    
    featureFilter = new TextColumnFeatureFilter();
    featureFilter.setFilterColumn("id");
    featureFilter.setFilterValue("test*");
    featureFilter.setFilterType(ColumnFeatureFilter.FilterType.valueOf("TEXT"));
    featureFilter.setParsingMethod(TextColumnFeatureFilter.ParsingMethod.valueOf("WILDCARD"));
    filteredFeatures = new ArrayList<Geometry>();
    for (Geometry unfilteredFeature : unfilteredFeatures)
    {
      Geometry filteredFeature = featureFilter.filter(unfilteredFeature);
      if (filteredFeature != null)
      {
        filteredFeatures.add(filteredFeature);
      }
    }
    Assert.assertEquals(filteredFeatures.size(), 2);
    Assert.assertNotNull(filteredFeatures.get(0));
    Assert.assertEquals(filteredFeatures.get(0).getAttribute("id"), "test1");
    Assert.assertNotNull(filteredFeatures.get(1));
    Assert.assertEquals(filteredFeatures.get(1).getAttribute("id"), "test");
    
    featureFilter = new TextColumnFeatureFilter();
    featureFilter.setFilterColumn("id");
    featureFilter.setFilterValue("*test*");
    featureFilter.setFilterType(ColumnFeatureFilter.FilterType.valueOf("TEXT"));
    featureFilter.setParsingMethod(TextColumnFeatureFilter.ParsingMethod.valueOf("WILDCARD"));
    filteredFeatures = new ArrayList<Geometry>();
    for (Geometry unfilteredFeature : unfilteredFeatures)
    {
      Geometry filteredFeature = featureFilter.filter(unfilteredFeature);
      if (filteredFeature != null)
      {
        filteredFeatures.add(filteredFeature);
      }
    }
    Assert.assertEquals(filteredFeatures.size(), 3);
    Assert.assertNotNull(filteredFeatures.get(0));
    Assert.assertEquals(filteredFeatures.get(0).getAttribute("id"), "test1");
    Assert.assertNotNull(filteredFeatures.get(1));
    Assert.assertEquals(filteredFeatures.get(1).getAttribute("id"), "test");
    Assert.assertNotNull(filteredFeatures.get(2));
    Assert.assertEquals(filteredFeatures.get(2).getAttribute("id"), "my3test");
    
    featureFilter = new TextColumnFeatureFilter();
    featureFilter.setFilterColumn("id");
    featureFilter.setFilterValue("test+");
    featureFilter.setFilterType(ColumnFeatureFilter.FilterType.valueOf("TEXT"));
    featureFilter.setParsingMethod(TextColumnFeatureFilter.ParsingMethod.valueOf("WILDCARD"));
    filteredFeatures = new ArrayList<Geometry>();
    for (Geometry unfilteredFeature : unfilteredFeatures)
    {
      Geometry filteredFeature = featureFilter.filter(unfilteredFeature);
      if (filteredFeature != null)
      {
        filteredFeatures.add(filteredFeature);
      }
    }
    Assert.assertEquals(filteredFeatures.size(), 0);
    
    featureFilter = new TextColumnFeatureFilter();
    featureFilter.setFilterColumn("id");
    featureFilter.setFilterValue("test.*");
    featureFilter.setFilterType(ColumnFeatureFilter.FilterType.valueOf("TEXT"));
    featureFilter.setParsingMethod(TextColumnFeatureFilter.ParsingMethod.valueOf("WILDCARD"));
    filteredFeatures = new ArrayList<Geometry>();
    for (Geometry unfilteredFeature : unfilteredFeatures)
    {
      Geometry filteredFeature = featureFilter.filter(unfilteredFeature);
      if (filteredFeature != null)
      {
        filteredFeatures.add(filteredFeature);
      }
    }
    Assert.assertEquals(filteredFeatures.size(), 0);
  }
  
  @Test 
  @Category(UnitTest.class)
  public void testStringFilteringRegex() throws Exception
  {
    TextColumnFeatureFilter featureFilter = new TextColumnFeatureFilter();
    featureFilter.setFilterColumn("id");
    featureFilter.setFilterValue("1");
    featureFilter.setFilterType(ColumnFeatureFilter.FilterType.valueOf("TEXT"));
    featureFilter.setParsingMethod(TextColumnFeatureFilter.ParsingMethod.valueOf("REGEX"));
    List<Geometry> filteredFeatures = new ArrayList<Geometry>();
    for (Geometry unfilteredFeature : unfilteredFeatures)
    {
      Geometry filteredFeature = featureFilter.filter(unfilteredFeature);
      if (filteredFeature != null)
      {
        filteredFeatures.add(filteredFeature);
      }
    }
    Assert.assertEquals(filteredFeatures.size(), 1);
    Assert.assertNotNull(filteredFeatures.get(0));
    Assert.assertEquals(filteredFeatures.get(0).getAttribute("id"), "1");
    
    featureFilter = new TextColumnFeatureFilter();
    featureFilter.setFilterColumn("id");
    featureFilter.setFilterValue("test.*");
    featureFilter.setFilterType(ColumnFeatureFilter.FilterType.valueOf("TEXT"));
    featureFilter.setParsingMethod(TextColumnFeatureFilter.ParsingMethod.valueOf("REGEX"));
    filteredFeatures = new ArrayList<Geometry>();
    for (Geometry unfilteredFeature : unfilteredFeatures)
    {
      Geometry filteredFeature = featureFilter.filter(unfilteredFeature);
      if (filteredFeature != null)
      {
        filteredFeatures.add(filteredFeature);
      }
    }
    Assert.assertEquals(filteredFeatures.size(), 2);
    Assert.assertNotNull(filteredFeatures.get(0));
    Assert.assertEquals(filteredFeatures.get(0).getAttribute("id"), "test1");
    Assert.assertNotNull(filteredFeatures.get(1));
    Assert.assertEquals(filteredFeatures.get(1).getAttribute("id"), "test");
    
    featureFilter = new TextColumnFeatureFilter();
    featureFilter.setFilterColumn("id");
    featureFilter.setFilterValue("my.*test");
    featureFilter.setFilterType(ColumnFeatureFilter.FilterType.valueOf("TEXT"));
    featureFilter.setParsingMethod(TextColumnFeatureFilter.ParsingMethod.valueOf("REGEX"));
    filteredFeatures = new ArrayList<Geometry>();
    for (Geometry unfilteredFeature : unfilteredFeatures)
    {
      Geometry filteredFeature = featureFilter.filter(unfilteredFeature);
      if (filteredFeature != null)
      {
        filteredFeatures.add(filteredFeature);
      }
    }
    Assert.assertEquals(filteredFeatures.size(), 1);
    Assert.assertNotNull(filteredFeatures.get(0));
    Assert.assertEquals(filteredFeatures.get(0).getAttribute("id"), "my3test");
  } 
}
