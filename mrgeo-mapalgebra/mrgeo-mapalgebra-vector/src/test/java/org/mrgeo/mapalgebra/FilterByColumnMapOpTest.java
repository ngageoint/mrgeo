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

package org.mrgeo.mapalgebra;

import static org.junit.Assert.assertEquals;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.featurefilter.ColumnFeatureFilter;
import org.mrgeo.featurefilter.DateColumnFeatureFilter;
import org.mrgeo.featurefilter.DateColumnFeatureFilter.DateGranularity;
import org.mrgeo.featurefilter.NumericColumnFeatureFilter;
import org.mrgeo.featurefilter.NumericColumnFeatureFilter.ComparisonMethod;
import org.mrgeo.featurefilter.TextColumnFeatureFilter;
import org.mrgeo.featurefilter.TextColumnFeatureFilter.ParsingMethod;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.mapalgebra.old.MapAlgebraParser;
import org.mrgeo.mapalgebra.parser.ParserException;
import org.mrgeo.test.LocalRunnerTest;
import org.mrgeo.test.MapOpTestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//import org.mrgeo.utils.LoggingUtils;

@SuppressWarnings("static-method")
public class FilterByColumnMapOpTest extends LocalRunnerTest
{
  @SuppressWarnings("unused")
  private static final Logger log = LoggerFactory.getLogger(FilterByColumnMapOpTest.class);
  
  private static final boolean GENERATE_BASELINE_DATA = false;

  private static MapOpTestUtils testUtils;

  private static String _ellipse;
  private static String _noColsFile;
  private static String _nonVectorFile;

  @BeforeClass
  public static void init()
  {
    try
    {
      testUtils = new MapOpTestUtils(FilterByColumnMapOpTest.class);

      HadoopFileUtils.copyToHdfs(
        new Path(testUtils.getInputLocal()), testUtils.getInputHdfs(), "InputEllipse.tsv");
      HadoopFileUtils.copyToHdfs(
        new Path(testUtils.getInputLocal()), 
        testUtils.getInputHdfs(), 
        "InputEllipse.tsv.columns");
      _ellipse = testUtils.getInputHdfs().toString() + "/InputEllipse.tsv";
      
      HadoopFileUtils.copyToHdfs(
        new Path(testUtils.getInputLocal()), testUtils.getInputHdfs(), "InputEllipse2.tsv");
      _noColsFile = testUtils.getInputHdfs().toString() + "/InputEllipse2.tsv";
      
      HadoopFileUtils.copyToHdfs(
        new Path(testUtils.getInputLocal()), testUtils.getInputHdfs(), "NonVector.abc");
      _nonVectorFile = testUtils.getInputHdfs().toString() + "/NonVector.abc";
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }
  
  @Test
  @Category(UnitTest.class)
  public void testFeatureFilterForFilterType()
  {
    @SuppressWarnings("unused")
    ColumnFeatureFilter filter = 
      FilterByColumnMapOp.featureFilterForFilterType(
      ColumnFeatureFilter.FilterType.DATE);
    filter = 
      FilterByColumnMapOp.featureFilterForFilterType(
        ColumnFeatureFilter.FilterType.NUMERIC);
    filter = 
      FilterByColumnMapOp.featureFilterForFilterType(
        ColumnFeatureFilter.FilterType.TEXT);
  }
  
  @Test(expected=IllegalArgumentException.class)
  @Category(UnitTest.class)

  public void testFeatureFilterForInvalidFilterType()
  {
    FilterByColumnMapOp.featureFilterForFilterType(null);
  }
  
  @Test(expected=IllegalArgumentException.class)
  @Category(UnitTest.class)
  public void testAddInputWithMissingColumnsFile() throws FileNotFoundException, ParserException,
    IOException
  {
    String expression = 
      String.format("FilterByColumn([%s], 'id', '5', 'text')", _noColsFile).replace("'", "\"");
    MapAlgebraParser parser = new MapAlgebraParser(conf, "", null);
    parser.parse(expression);
  }
  
  @Test(expected=IllegalArgumentException.class)
  @Category(UnitTest.class)
  public void testAddInputNonVector() throws FileNotFoundException, ParserException, IOException
  {
    String expression = 
      String.format("FilterByColumn([%s], 'id', '5', 'text')", _nonVectorFile).replace("'", "\"");
    MapAlgebraParser parser = new MapAlgebraParser(conf, "", null);
    parser.parse(expression);
  }
  
  @Test(expected=IllegalArgumentException.class)
  @Category(UnitTest.class)
  public void testProcessChildrenEmptyFilterType() throws FileNotFoundException, ParserException,
    IOException
  {
    String expression = 
      String.format("FilterByColumn([%s], 'id', '5')", _ellipse).replace("'", "\"");
    MapAlgebraParser parser = new MapAlgebraParser(conf, "", null);
    parser.parse(expression);
  }
  
  @Test(expected=IllegalArgumentException.class)
  @Category(UnitTest.class)
  public void testProcessChildrenEmptyFilterColumn() throws FileNotFoundException, ParserException,
    IOException
  {
    String expression = 
      String.format("FilterByColumn([%s], '5', 'text')", _ellipse).replace("'", "\"");
    MapAlgebraParser parser = new MapAlgebraParser(conf, "", null);
    parser.parse(expression);
  }
  
  @Test(expected=IllegalArgumentException.class)
  @Category(UnitTest.class)
  public void testProcessChildrenEmptyFilterValue() throws FileNotFoundException, ParserException,
    IOException
  {
    String expression = 
      String.format("FilterByColumn([%s], 'id', 'text')", _ellipse).replace("'", "\"");
    MapAlgebraParser parser = new MapAlgebraParser(conf, "", null);
    parser.parse(expression);
  }
  
  //defaults to EXACT
  @Test
  @Category(UnitTest.class)
  public void testProcessChildrenMissingTextParsingMethod() throws FileNotFoundException, 
    ParserException, IOException
  {
    String expression = 
      String.format("FilterByColumn([%s], 'id', '5', 'text')", _ellipse).replace("'", "\"");
    MapAlgebraParser parser = new MapAlgebraParser(conf, "", null);
    FilterByColumnMapOp mapOp = (FilterByColumnMapOp)parser.parse(expression);
    
    assertEquals(
      mapOp.toString(), 
      "FilterByColumnMapOp: input: null column filter: id, value: 5, filter type: TEXT parse type: EXACT");
    TextColumnFeatureFilter filter = (TextColumnFeatureFilter)mapOp.getFilter();
    assertEquals(filter.getFilterColumn(), "id");
    assertEquals(filter.getFilterType(), ColumnFeatureFilter.FilterType.TEXT);
    assertEquals(filter.getFilterValue(), "5");
    assertEquals(filter.getParsingMethod(), ParsingMethod.EXACT);
  }
  
  @Test(expected=IllegalArgumentException.class)
  @Category(UnitTest.class)
  public void testProcessChildrenEmptyTextParsingMethod() throws FileNotFoundException,
    ParserException, IOException
  {
    String expression = 
      String.format("FilterByColumn([%s], 'id', '5', 'text', '')", _ellipse).replace("'", "\"");
    MapAlgebraParser parser = new MapAlgebraParser(conf, "", null);
    parser.parse(expression);
  }
  
  //default to EQUAL_TO
  @Test
  @Category(UnitTest.class)
  public void testProcessChildrenMissingNumericComparisonMethod() throws FileNotFoundException, 
    ParserException, IOException
  {
    String expression = 
      String.format(
        "FilterByColumn([%s], 'major', '52493', 'numeric')", _ellipse).replace("'", "\"");
    MapAlgebraParser parser = new MapAlgebraParser(conf, "", null);
    FilterByColumnMapOp mapOp = (FilterByColumnMapOp)parser.parse(expression);
    
    assertEquals(
      mapOp.toString(), 
      "FilterByColumnMapOp: input: null column filter: major, value: 52493, filter type: NUMERIC comparison type: EQUAL_TO");
    NumericColumnFeatureFilter filter = (NumericColumnFeatureFilter)mapOp.getFilter();
    assertEquals(filter.getFilterColumn(), "major");
    assertEquals(filter.getFilterType(), ColumnFeatureFilter.FilterType.NUMERIC);
    assertEquals(filter.getFilterValue(), "52493");
    assertEquals(filter.getComparisonMethod(), ComparisonMethod.EQUAL_TO);
  }
  
  @Test(expected=IllegalArgumentException.class)
  @Category(UnitTest.class)
  public void testProcessChildrenEmptyNumericComparisonMethod() throws FileNotFoundException,
    ParserException, IOException
  {
    String expression = 
      String.format(
        "FilterByColumn([%s], 'major', '52493', 'numeric', '')", _ellipse).replace("'", "\"");
    MapAlgebraParser parser = new MapAlgebraParser(conf, "", null);
    parser.parse(expression);
  }
  
  //defaults to "MM-dd-yyyy" and SAME_INSTANT
  @Test
  @Category(UnitTest.class)
  public void testProcessChildrenMissingDateFormatAndGranularity() throws FileNotFoundException, 
    ParserException, IOException
  {
    String expression = 
      String.format("FilterByColumn([%s], \"date\", \"2012-12-11T08:00:00\", \"Date\")", _ellipse);
    MapAlgebraParser parser = new MapAlgebraParser(conf, "", null);
    FilterByColumnMapOp mapOp = (FilterByColumnMapOp)parser.parse(expression);
    
    assertEquals(
      mapOp.toString(), 
      "FilterByColumnMapOp: input: null column filter: date, value: 2012-12-11T08:00:00, filter type: DATE date format: MM-dd-yyyy date filter granularity: SAME_INSTANT");
    DateColumnFeatureFilter filter = (DateColumnFeatureFilter)mapOp.getFilter();
    assertEquals(filter.getFilterColumn(), "date");
    assertEquals(filter.getFilterType(), ColumnFeatureFilter.FilterType.DATE);
    assertEquals(filter.getFilterValue(), "2012-12-11T08:00:00");
    assertEquals(filter.getDateFormat(), "MM-dd-yyyy");
    assertEquals(filter.getDateFilterGranularity(), DateGranularity.SAME_INSTANT);
  }
  
  @Test(expected=IllegalArgumentException.class)
  @Category(UnitTest.class)
  public void testProcessChildrenEmptyDateFormatAndGranularity() throws FileNotFoundException,
    ParserException, IOException
  {
    String expression = 
      String.format("FilterByColumn([%s], \"date\", \"2012-12-11T08:00:00\", \"Date\", \"\", \"\")", 
        _ellipse);
    MapAlgebraParser parser = new MapAlgebraParser(conf, "", null);
    parser.parse(expression);
  }
  
}
