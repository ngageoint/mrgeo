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
public class NumericColumnFeatureFilterTest extends ColumnFeatureFilterTest
{
  private static final double EPSILON = 1e-10;

  private static List<Geometry> unfilteredFeatures;
  
  @BeforeClass
  public static void init() throws IOException, InterruptedException
  {
    unfilteredFeatures = readFeatures("numericTest1.tsv");
  }
  
  @Test 
  @Category(UnitTest.class)
  public void testNumericFilteringEqualTo() throws Exception
  {
    //this filter should return a single record
    NumericColumnFeatureFilter featureFilter = new NumericColumnFeatureFilter();
    featureFilter.setFilterColumn("num1");
    featureFilter.setFilterValue("1.23");
    featureFilter.setFilterType(ColumnFeatureFilter.FilterType.valueOf("NUMERIC"));
    featureFilter.setComparisonMethod(
      NumericColumnFeatureFilter.ComparisonMethod.valueOf("EQUAL_TO"));
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
    Assert.assertNotNull(filteredFeatures.get(0));
    Assert.assertEquals(1.23, Double.parseDouble(filteredFeatures.get(0).getAttribute("num1")), EPSILON);
  }
  
  @Test 
  @Category(UnitTest.class)
  public void testNumericFilteringNotEqualTo() throws Exception
  {
    NumericColumnFeatureFilter featureFilter = new NumericColumnFeatureFilter();
    featureFilter.setFilterColumn("num1");
    featureFilter.setFilterValue("1.23");
    featureFilter.setFilterType(ColumnFeatureFilter.FilterType.valueOf("NUMERIC"));
    featureFilter.setComparisonMethod(
      NumericColumnFeatureFilter.ComparisonMethod.valueOf("NOT_EQUAL_TO"));
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
  }
  
  @Test 
  @Category(UnitTest.class)
  public void testNumericFilteringGreaterThan() throws Exception
  {
    NumericColumnFeatureFilter featureFilter = new NumericColumnFeatureFilter();
    featureFilter.setFilterColumn("num1");
    featureFilter.setFilterValue("1.23");
    featureFilter.setFilterType(ColumnFeatureFilter.FilterType.valueOf("NUMERIC"));
    featureFilter.setComparisonMethod(
      NumericColumnFeatureFilter.ComparisonMethod.valueOf("GREATER_THAN"));
    List<Geometry> filteredFeatures = new ArrayList<Geometry>();
    for (Geometry unfilteredFeature : unfilteredFeatures)
    {
      Geometry filteredFeature = featureFilter.filter(unfilteredFeature);
      if (filteredFeature != null)
      {
        filteredFeatures.add(filteredFeature);
      }
    }
    Assert.assertTrue(filteredFeatures.size() == 4);
    
    featureFilter = new NumericColumnFeatureFilter();
    featureFilter.setFilterColumn("num1");
    featureFilter.setFilterValue("2");
    featureFilter.setFilterType(ColumnFeatureFilter.FilterType.valueOf("NUMERIC"));
    featureFilter.setComparisonMethod(
      NumericColumnFeatureFilter.ComparisonMethod.valueOf("GREATER_THAN"));
    filteredFeatures = new ArrayList<Geometry>();
    for (Geometry unfilteredFeature : unfilteredFeatures)
    {
      Geometry filteredFeature = featureFilter.filter(unfilteredFeature);
      if (filteredFeature != null)
      {
        filteredFeatures.add(filteredFeature);
      }
    }
    Assert.assertTrue(filteredFeatures.size() == 4);
  }
  
  @Test 
  @Category(UnitTest.class)
  public void testNumericFilteringGreaterThanOrEqualTo() throws Exception
  {
    NumericColumnFeatureFilter featureFilter = new NumericColumnFeatureFilter();
    featureFilter.setFilterColumn("num1");
    featureFilter.setFilterValue("1.23");
    featureFilter.setFilterType(ColumnFeatureFilter.FilterType.valueOf("NUMERIC"));
    featureFilter.setComparisonMethod(
      NumericColumnFeatureFilter.ComparisonMethod.valueOf("GREATER_THAN_OR_EQUAL_TO"));
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
  }
  
  @Test 
  @Category(UnitTest.class)
  public void testNumericFilteringLessThan() throws Exception
  {
    NumericColumnFeatureFilter featureFilter = new NumericColumnFeatureFilter();
    featureFilter.setFilterColumn("num1");
    featureFilter.setFilterValue("1.23");
    featureFilter.setFilterType(ColumnFeatureFilter.FilterType.valueOf("NUMERIC"));
    featureFilter.setComparisonMethod(
      NumericColumnFeatureFilter.ComparisonMethod.valueOf("LESS_THAN"));
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
    
    featureFilter = new NumericColumnFeatureFilter();
    featureFilter.setFilterColumn("num1");
    featureFilter.setFilterValue("   1.23 ");
    featureFilter.setFilterType(ColumnFeatureFilter.FilterType.valueOf("NUMERIC"));
    featureFilter.setComparisonMethod(
      NumericColumnFeatureFilter.ComparisonMethod.valueOf("LESS_THAN"));
    filteredFeatures = new ArrayList<Geometry>();
    for (Geometry unfilteredFeature : unfilteredFeatures)
    {
      Geometry filteredFeature = featureFilter.filter(unfilteredFeature);
      if (filteredFeature != null)
      {
        filteredFeatures.add(filteredFeature);
      }
    }
    Assert.assertTrue(filteredFeatures.size() == 1);
  }
  
  @Test 
  @Category(UnitTest.class)
  public void testNumericFilteringLessThanOrEqualTo() throws Exception
  {
    NumericColumnFeatureFilter featureFilter = new NumericColumnFeatureFilter();
    featureFilter.setFilterColumn("num1");
    featureFilter.setFilterValue("1.23");
    featureFilter.setFilterType(ColumnFeatureFilter.FilterType.valueOf("NUMERIC"));
    featureFilter.setComparisonMethod(
      NumericColumnFeatureFilter.ComparisonMethod.valueOf("LESS_THAN_OR_EQUAL_TO"));
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
    
    boolean exceptionCaught = false;
    featureFilter = new NumericColumnFeatureFilter();
    try
    {
      featureFilter.setFilterValue("<1.23");
    }
    catch (NumberFormatException e)
    {
      exceptionCaught = true;
    }
    Assert.assertTrue(exceptionCaught);
  } 
}
