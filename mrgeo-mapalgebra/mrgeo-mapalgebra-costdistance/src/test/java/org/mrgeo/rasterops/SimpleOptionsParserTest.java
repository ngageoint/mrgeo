package org.mrgeo.rasterops;

import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.junit.UnitTest;

@SuppressWarnings("static-method")
public class SimpleOptionsParserTest
{ 
  private SimpleOptionsParser parser;
  
  @Before
  public void setup() {
    String args[] = new String[] {
        "-inputpyramid", "input",
        "-outputpyramid", "output"};
    parser = new SimpleOptionsParser(args);    
  }
  
  @Test
  @Category(UnitTest.class)
  public void testGetOptionValue() {
    String input = parser.getOptionValue("inputpyramid");
    String output = parser.getOptionValue("outputpyramid");
    Assert.assertEquals("input", input);
    Assert.assertEquals("output", output);
  }
  
  @Test
  @Category(UnitTest.class)
  public void testIsOptionProvided() {
    Assert.assertEquals(true, parser.isOptionProvided("inputpyramid"));
    Assert.assertEquals(true, parser.isOptionProvided("outputpyramid"));
    Assert.assertEquals(false, parser.isOptionProvided("foobar"));
  }
  
  @Test(expected=IllegalArgumentException.class)
  @Category(UnitTest.class)
  public void testParseErrorOddArgumentLength() {
    String args[] = new String[] {
        "-inputpyramid", "input",
        "-outputpyramid"};
    SimpleOptionsParser parserTwo = new SimpleOptionsParser(args);
  }
  @Test(expected=IllegalArgumentException.class)
  @Category(UnitTest.class)
  public void testParseErrorNoHyphen() {
    String args[] = new String[] {
        "inputpyramid", "input"};
    SimpleOptionsParser parserTwo = new SimpleOptionsParser(args);
  }
}
