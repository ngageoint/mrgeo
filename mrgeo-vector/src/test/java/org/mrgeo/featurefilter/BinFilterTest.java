package org.mrgeo.featurefilter;

import junit.framework.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.junit.UnitTest;

@SuppressWarnings("static-method")
@Ignore
// TODO:  Un-ignore if needed
public class BinFilterTest 
{
  @Test 
  @Category(UnitTest.class)
  public void testBasics() throws Exception
  {
//    // this class and its unit tests are a work in progress.
//    try
//    {
//      BinFilter uut = new BinFilter(10, new String[] { "int1" });
//      FeatureSchemaStats schema = new FeatureSchemaStats();
//      schema.addAttribute("date", AttributeType.STRING);
//      schema.addAttribute("int1", AttributeType.INTEGER);
//      schema.setAttributeMin(1, 10);
//      schema.setAttributeMax(1, 20);
//      schema.addAttribute("double1", AttributeType.DOUBLE);
//      schema.setAttributeMin(2, 0);
//      schema.setAttributeMax(2, 20);
//
//      Feature f;
//
//      f = new BasicFeature(schema);
//      f.setAttribute(0, "03OCT2009:17:03:44.260000");
//      f.setAttribute(1, 11);
//      f.setAttribute(2, 10.0);
//
//      f = uut.filterInPlace(f);
//      checkSchema(f.getSchema());
//      Assert.assertEquals(1, f.getAttribute(1));
//      Assert.assertEquals(11, f.getAttribute(2));
//      Assert.assertEquals(5, f.getAttribute(3));
//
//      f = new BasicFeature(schema);
//      f.setAttribute(0, "22OCT2009:21:35:59.390000");
//      f.setAttribute(1, 7);
//      f.setAttribute(2, 18.0);
//
//      f = uut.filterInPlace(f);
//      checkSchema(f.getSchema());
//      Assert.assertEquals(0, f.getAttribute(1));
//      Assert.assertEquals(7, f.getAttribute(2));
//      Assert.assertEquals(9, f.getAttribute(3));
//
//      schema = new FeatureSchemaStats();
//      schema.addAttribute("date2", AttributeType.STRING);
//      schema.addAttribute("int2", AttributeType.INTEGER);
//      schema.setAttributeMin(1, 10);
//      schema.setAttributeMax(1, 20);
//      schema.addAttribute("double2", AttributeType.STRING);
//      schema.setAttributeMin(2, 0);
//      schema.setAttributeMax(2, 20);
//
//      f = new BasicFeature(schema);
//      f.setAttribute(0, "22OCT2009:21:35:59.390000");
//      f.setAttribute(1, 7);
//      f.setAttribute(2, "18.0");
//
//      f = uut.filterInPlace(f);
//      checkSchema2(f.getSchema());
//      Assert.assertEquals(0, f.getAttribute(1));
//      Assert.assertEquals("18.0", f.getAttribute(2));
//    }
//    catch (Exception e)
//    {
//      e.printStackTrace();
//      throw e;
//    }
  }

//  private void checkSchema(FeatureSchema schema)
//  {
//    FeatureSchemaStats stats = (FeatureSchemaStats)schema;
//    System.out.println(schema.toString());
//
//    Assert.assertEquals(4, schema.getAttributeCount());
//    Assert.assertEquals("date", schema.getAttributeName(0));
//    Assert.assertEquals("int1", schema.getAttributeName(1));
//    Assert.assertEquals("int1_numeric", schema.getAttributeName(2));
//    Assert.assertEquals("double1", schema.getAttributeName(3));
//
//    Assert.assertEquals(AttributeType.STRING, schema.getAttributeType(0));
//    Assert.assertEquals(AttributeType.STRING, schema.getAttributeType(1));
//    Assert.assertEquals(AttributeType.INTEGER, schema.getAttributeType(2));
//    Assert.assertEquals(AttributeType.STRING, schema.getAttributeType(3));
//
//    Assert.assertEquals(10.0, stats.getAttributeMin(2));
//    Assert.assertEquals(20.0, stats.getAttributeMax(2));
//  }
//
//  private void checkSchema2(FeatureSchema schema)
//  {
//    System.out.println(schema.toString());
//
//    Assert.assertEquals(3, schema.getAttributeCount());
//    Assert.assertEquals("date2", schema.getAttributeName(0));
//    Assert.assertEquals("int2", schema.getAttributeName(1));
//    Assert.assertEquals("double2", schema.getAttributeName(2));
//
//    Assert.assertEquals(AttributeType.STRING, schema.getAttributeType(0));
//    Assert.assertEquals(AttributeType.STRING, schema.getAttributeType(1));
//    Assert.assertEquals(AttributeType.STRING, schema.getAttributeType(2));
//  }

  @Test
  @Category(UnitTest.class) 
  public void test_filterInPlaceStringNoCopySet() {
//    BinFilter uut = new BinFilter(10);
//    FeatureSchemaStats schema = new FeatureSchemaStats();
//    schema.addAttribute("city", AttributeType.STRING);
//
//    Feature f = new BasicFeature(schema);
//    f.setAttribute(0, "McLean");
//    double beforeMin = schema.getAttributeMin(0);
//    double beforeMax = schema.getAttributeMax(0);
//
//    schema.setAttributeMin(0, 20);
//    schema.setAttributeMax(0, 10);
//
//    f = uut.filterInPlace(f);
//    FeatureSchemaStats stats = (FeatureSchemaStats)f.getSchema();
//    Assert.assertEquals("McLean", f.getAttribute(0));
//
//    Assert.assertEquals(1, stats.getAttributeCount());
//    Assert.assertEquals("city", stats.getAttributeName(0));
//    Assert.assertEquals("STRING", stats.getAttributeType(0).toString());
//    Assert.assertEquals(beforeMin,  stats.getAttributeMin(0), .001);
//    Assert.assertEquals(beforeMax,stats.getAttributeMax(0), .001);
//    Assert.assertEquals(1, f.getAttributes().length);
  }  
  @Test
  @Category(UnitTest.class) 
  public void test_filterInPlaceStringWithCopySet() {
//    BinFilter uut = new BinFilter(10, new String[] { "city"});
//    FeatureSchemaStats schema = new FeatureSchemaStats();
//    schema.addAttribute("city", AttributeType.STRING);
//
//    Feature f = new BasicFeature(schema);
//    f.setAttribute(0, "McLean");
//    double beforeMin = schema.getAttributeMin(0);
//    double beforeMax = schema.getAttributeMax(0);
//
//    schema.setAttributeMin(0, 20);
//    schema.setAttributeMax(0, 10);
//
//    f = uut.filterInPlace(f);
//    FeatureSchemaStats stats = (FeatureSchemaStats)f.getSchema();
//    Assert.assertEquals("McLean", f.getAttribute(0));
//
//    Assert.assertEquals(2, stats.getAttributeCount());
//    Assert.assertEquals("city", stats.getAttributeName(0));
//    Assert.assertEquals("STRING", stats.getAttributeType(0).toString());
//    Assert.assertEquals(beforeMin,  stats.getAttributeMin(0), .001);
//    Assert.assertEquals(beforeMax,stats.getAttributeMax(0), .001);
//    Assert.assertEquals("city_numeric", stats.getAttributeName(1));
//    Assert.assertEquals("STRING", stats.getAttributeType(1).toString());
//    Assert.assertEquals(beforeMin,  stats.getAttributeMin(1), .001);
//    Assert.assertEquals(beforeMax,stats.getAttributeMax(1), .001);
//    Assert.assertEquals(2, f.getAttributes().length);
  }
  
  @Test
  @Category(UnitTest.class) 
  public void test_filterInPlaceDoubleAndIntNoCopySetWithMaxLessThanMin() {
//    BinFilter uut = new BinFilter(10);
//    FeatureSchemaStats schema = new FeatureSchemaStats();
//    schema.addAttribute("int1", AttributeType.INTEGER);
//    schema.addAttribute("double1", AttributeType.DOUBLE);
//    schema.setAttributeCount(0, 2);
//    schema.setAttributeCount(1, 4);
//
//    Feature f = new BasicFeature(schema);
//    f.setAttribute(0, 2);
//    double beforeMin = schema.getAttributeMin(0);
//    double beforeMax = schema.getAttributeMax(0);
//
//    schema.setAttributeMin(0, 20);
//    schema.setAttributeMax(0, 10);
//    f.setAttribute(1, 4.005);
//    schema.setAttributeMin(1, 30);
//    schema.setAttributeMax(1, 10);
//
//    f = uut.filterInPlace(f);
//    FeatureSchemaStats stats = (FeatureSchemaStats)f.getSchema();
//    Assert.assertEquals(0, f.getAttribute(0));
//    Assert.assertEquals(0, f.getAttribute(1));
//
//    Assert.assertEquals(2, stats.getAttributeCount());
//    Assert.assertEquals("int1", stats.getAttributeName(0));
//    Assert.assertEquals("STRING", stats.getAttributeType(0).toString());
//    Assert.assertEquals("double1", stats.getAttributeName(1));
//    Assert.assertEquals("STRING", stats.getAttributeType(1).toString());
//    Assert.assertEquals(2, stats.getAttributeCount(0));
//    Assert.assertEquals(beforeMin,  stats.getAttributeMin(0), .001);
//    Assert.assertEquals(beforeMax,stats.getAttributeMax(0), .001);
//    Assert.assertEquals(4, stats.getAttributeCount(1));
//    Assert.assertEquals(beforeMin, stats.getAttributeMin(1), .001);
//    Assert.assertEquals(beforeMax,stats.getAttributeMax(1), .001);
//    Assert.assertEquals(2, f.getAttributes().length);
  }  

  @Test
  @Category(UnitTest.class) 
  public void test_filterInPlaceDoubleAndIntWithCopySet() {
//    BinFilter uut = new BinFilter(10, new String[] { "int1", "double1" });
//    FeatureSchemaStats schema = new FeatureSchemaStats();
//    schema.addAttribute("int1", AttributeType.INTEGER);
//    schema.addAttribute("double1", AttributeType.DOUBLE);
//    schema.setAttributeCount(0, 2);
//    schema.setAttributeCount(1, 4);
//
//    Feature f = new BasicFeature(schema);
//    f.setAttribute(0, 2);
//    double beforeMin = schema.getAttributeMin(0);
//    double beforeMax = schema.getAttributeMax(0);
//
//    schema.setAttributeMin(0, 0.25);
//    schema.setAttributeMax(0, 20.25);
//    f.setAttribute(1, 8.005);
//    schema.setAttributeMin(1, 0.39);
//    schema.setAttributeMax(1, 10.39);
//
//    f = uut.filterInPlace(f);
//    FeatureSchemaStats stats = (FeatureSchemaStats)f.getSchema();
//    Assert.assertEquals(4, stats.getAttributeCount());
//    Assert.assertEquals("int1", stats.getAttributeName(0));
//    Assert.assertEquals("STRING", stats.getAttributeType(0).toString());
//    Assert.assertEquals(2, stats.getAttributeCount(0));
//    Assert.assertEquals(beforeMin,  stats.getAttributeMin(0), .001);
//    Assert.assertEquals(beforeMax,stats.getAttributeMax(0), .001);
//
//    Assert.assertEquals("int1_numeric", stats.getAttributeName(1));
//    Assert.assertEquals("INTEGER", stats.getAttributeType(1).toString());
//    Assert.assertEquals(.25,  stats.getAttributeMin(1), .001);
//    Assert.assertEquals(20.25,stats.getAttributeMax(1), .001);
//
//    Assert.assertEquals("double1", stats.getAttributeName(2));
//    Assert.assertEquals("STRING", stats.getAttributeType(2).toString());
//    Assert.assertEquals(beforeMin, stats.getAttributeMin(2), .001);
//    Assert.assertEquals(beforeMax,stats.getAttributeMax(2), .001);
//
//    Assert.assertEquals("double1_numeric", stats.getAttributeName(3));
//    Assert.assertEquals("DOUBLE", stats.getAttributeType(3).toString());
//    Assert.assertEquals(.39,  stats.getAttributeMin(3), .001);
//    Assert.assertEquals(10.39,stats.getAttributeMax(3), .001);
//
//    Assert.assertEquals(4, stats.getAttributeCount(1));
//    Assert.assertEquals(4, f.getAttributes().length);
//    Assert.assertEquals(0, f.getAttribute(0));
//    Assert.assertEquals(2, f.getAttribute(1));
//    Assert.assertEquals(7, f.getAttribute(2));
//    Assert.assertEquals(8.005, f.getAttribute(3));
  }
  
  @Test
  @Category(UnitTest.class) 
  public void test_filterInPlaceDoubleAndIntNoCopySetWithMinMax() {
//    BinFilter uut = new BinFilter(10); //, new String[] { "int1", "double1" });
//    FeatureSchemaStats schema = new FeatureSchemaStats();
//    schema.addAttribute("int1", AttributeType.INTEGER);
//    schema.addAttribute("double1", AttributeType.DOUBLE);
//    schema.setAttributeCount(0, 2);
//    schema.setAttributeCount(1, 4);
//
//    Feature f = new BasicFeature(schema);
//    f.setAttribute(0, 2);
//    double beforeMin = schema.getAttributeMin(0);
//    double beforeMax = schema.getAttributeMax(0);
//
//    schema.setAttributeMin(0, 0);
//    schema.setAttributeMax(0, 20);
//    f.setAttribute(1, 4.005);
//    schema.setAttributeMin(1, 0);
//    schema.setAttributeMax(1, 10);
//
//    f = uut.filterInPlace(f);
//    FeatureSchemaStats stats = (FeatureSchemaStats)f.getSchema();
//    Assert.assertEquals(2, stats.getAttributeCount());
//    Assert.assertEquals("int1", stats.getAttributeName(0));
//    Assert.assertEquals("STRING", stats.getAttributeType(0).toString());
//    Assert.assertEquals("double1", stats.getAttributeName(1));
//    Assert.assertEquals("STRING", stats.getAttributeType(1).toString());
//    Assert.assertEquals(2, stats.getAttributeCount(0));
//    Assert.assertEquals(beforeMin,  stats.getAttributeMin(0), .001);
//    Assert.assertEquals(beforeMax,stats.getAttributeMax(0), .001);
//    Assert.assertEquals(4, stats.getAttributeCount(1));
//    Assert.assertEquals(beforeMin, stats.getAttributeMin(1), .001);
//    Assert.assertEquals(beforeMax,stats.getAttributeMax(1), .001);
//    Assert.assertEquals(2, f.getAttributes().length);
//    Assert.assertEquals(1, f.getAttribute(0));
//    Assert.assertEquals(4, f.getAttribute(1));
  }  
}
