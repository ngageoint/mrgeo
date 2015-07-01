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

import junit.framework.Assert;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.geometry.GeometryFactory;
import org.mrgeo.geometry.WritableGeometry;
import org.mrgeo.junit.UnitTest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TimeSpanGeometrySplitterTest
{
  private DateTime overallStart;
  private DateTime overallEnd;
  private int intervalSeconds;
  private int partialIntervalSeconds;
  private DateTimeFormatter dtf = ISODateTimeFormat.dateTime();
  private static final String startFieldName = "start";
  private static final String endFieldName = "end";

  private GeometrySplitter createSplitter(TimeSpanGeometrySplitter.CompareType compareType)
  {
    overallStart = DateTime.now();
    overallEnd = overallStart.plusSeconds(100);
    partialIntervalSeconds = 5;
    intervalSeconds = 10;
    Map<String, String> splitterProperties = new HashMap<String, String>();
    String strCompareType = TimeSpanGeometrySplitter.compareTypeToString(compareType);
    splitterProperties.put(TimeSpanGeometrySplitter.COMPARE_TYPE_PROPERTY, strCompareType);
    splitterProperties.put(TimeSpanGeometrySplitter.START_TIME_PROPERTY, overallStart.toString(dtf));
    splitterProperties.put(TimeSpanGeometrySplitter.END_TIME_PROPERTY, overallEnd.toString(dtf));
    splitterProperties.put(TimeSpanGeometrySplitter.INTERVAL_PROPERTY, Integer.toString(intervalSeconds));
    splitterProperties.put(TimeSpanGeometrySplitter.TIME_FORMAT_PROPERTY, ""); // defaults to ISO
    splitterProperties.put(TimeSpanGeometrySplitter.START_FIELD_PROPERTY, startFieldName);
    splitterProperties.put(TimeSpanGeometrySplitter.END_FIELD_PROPERTY, endFieldName);
    GeometrySplitter splitter = new TimeSpanGeometrySplitter();
    splitter.initialize(splitterProperties, false, null);
    return splitter;
  }

  @Test
  @Category(UnitTest.class)
  public void testGeometryLessThanTestIntervalCompareEither()
  {
    GeometrySplitter splitter = createSplitter(TimeSpanGeometrySplitter.CompareType.EITHER_TIME);
    // Geometry end time is the overall start time. Should result in
    // no returned splits.
    WritableGeometry g = GeometryFactory.createPoint(0.0, 0.0);
    DateTime start = overallStart.minusSeconds(10);
    g.setAttribute(startFieldName, start.toString(dtf));
    DateTime end = overallStart;
    g.setAttribute(endFieldName, end.toString(dtf));
    List<String> result = splitter.splitGeometry(g);
    Assert.assertNotNull(result);
    Assert.assertEquals(0, result.size());

    start = overallStart.minusSeconds(10);
    g.setAttribute(startFieldName, start.toString(dtf));
    end = overallStart.minusSeconds(1);
    g.setAttribute(endFieldName, end.toString(dtf));
    result = splitter.splitGeometry(g);
    Assert.assertNotNull(result);
    Assert.assertEquals(0, result.size());
  }

  @Test
  @Category(UnitTest.class)
  public void testGeometryGreaterThanTestInterval()
  {
    GeometrySplitter splitter = createSplitter(TimeSpanGeometrySplitter.CompareType.EITHER_TIME);
    // Geometry start time is the overall end time. Should result in
    // no returned splits.
    WritableGeometry g = GeometryFactory.createPoint(0.0, 0.0);
    DateTime start = overallEnd;
    g.setAttribute(startFieldName, start.toString(dtf));
    DateTime end = overallEnd.plusSeconds(10);
    g.setAttribute(endFieldName, end.toString(dtf));
    List<String> result = splitter.splitGeometry(g);
    Assert.assertNotNull(result);
    Assert.assertEquals(0, result.size());

    // Geometry start time is after the overall end time. SHould be no
    // returned splits.
    start = overallEnd.plusSeconds(1);
    g.setAttribute(startFieldName, start.toString(dtf));
    end = overallEnd.plusSeconds(10);
    g.setAttribute(endFieldName, end.toString(dtf));
    result = splitter.splitGeometry(g);
    Assert.assertNotNull(result);
    Assert.assertEquals(0, result.size());
  }

  static String getOutputName(final DateTime dateTime, final DateTimeFormatter dtf)
  {
    return dateTime.toString(dtf).replaceAll("[^\\p{Alnum}]", "");
  }

  @Test
  @Category(UnitTest.class)
  public void testGeometrySpansFirstIntervalStart()
  {
    GeometrySplitter splitter = createSplitter(TimeSpanGeometrySplitter.CompareType.EITHER_TIME);
    // Geometry end time is the overall start time. Should result in
    // no returned splits.
    WritableGeometry g = GeometryFactory.createPoint(0.0, 0.0);
    DateTime start = overallStart.minusSeconds(partialIntervalSeconds);
    g.setAttribute(startFieldName, start.toString(dtf));
    DateTime end = start.plusSeconds(intervalSeconds);
    g.setAttribute(endFieldName, end.toString(dtf));
    List<String> result = splitter.splitGeometry(g);
    Assert.assertNotNull(result);
    Assert.assertEquals(1, result.size());
    Assert.assertEquals(getOutputName(overallStart.plusSeconds(intervalSeconds), dtf), result.get(0));
  }

  @Test
  @Category(UnitTest.class)
  public void testGeometrySpansFirstIntervalEndWithStartCompare()
  {
    GeometrySplitter splitter = createSplitter(TimeSpanGeometrySplitter.CompareType.START_TIME);
    // Geometry end time is the overall start time. Should result in
    // no returned splits.
    WritableGeometry g = GeometryFactory.createPoint(0.0, 0.0);
    DateTime start = overallStart.plusSeconds(partialIntervalSeconds);
    g.setAttribute(startFieldName, start.toString(dtf));
    DateTime end = start.plusSeconds(intervalSeconds);
    g.setAttribute(endFieldName, end.toString(dtf));
    List<String> result = splitter.splitGeometry(g);
    Assert.assertNotNull(result);
    Assert.assertEquals(1, result.size());
    Assert.assertEquals(getOutputName(overallStart.plusSeconds(intervalSeconds), dtf), result.get(0));
  }

  @Test
  @Category(UnitTest.class)
  public void testGeometrySpansFirstIntervalEndWithEndCompare()
  {
    GeometrySplitter splitter = createSplitter(TimeSpanGeometrySplitter.CompareType.END_TIME);
    // Geometry end time is the overall start time. Should result in
    // no returned splits.
    WritableGeometry g = GeometryFactory.createPoint(0.0, 0.0);
    DateTime start = overallStart.plusSeconds(partialIntervalSeconds);
    g.setAttribute(startFieldName, start.toString(dtf));
    DateTime end = start.plusSeconds(intervalSeconds);
    g.setAttribute(endFieldName, end.toString(dtf));
    List<String> result = splitter.splitGeometry(g);
    Assert.assertNotNull(result);
    Assert.assertEquals(1, result.size());
    Assert.assertEquals(getOutputName(overallStart.plusSeconds(intervalSeconds * 2), dtf), result.get(0));
  }

  @Test
  @Category(UnitTest.class)
  public void testGeometrySpansFirstIntervalEndWithEitherCompare()
  {
    GeometrySplitter splitter = createSplitter(TimeSpanGeometrySplitter.CompareType.EITHER_TIME);
    // Geometry end time is the overall start time. Should result in
    // no returned splits.
    WritableGeometry g = GeometryFactory.createPoint(0.0, 0.0);
    DateTime start = overallStart.plusSeconds(partialIntervalSeconds);
    g.setAttribute(startFieldName, start.toString(dtf));
    DateTime end = start.plusSeconds(intervalSeconds);
    g.setAttribute(endFieldName, end.toString(dtf));
    List<String> result = splitter.splitGeometry(g);
    Assert.assertNotNull(result);
    Assert.assertEquals(2, result.size());
    // Should include intervals 1 and 2
    Assert.assertEquals(getOutputName(overallStart.plusSeconds(intervalSeconds), dtf), result.get(0));
    Assert.assertEquals(getOutputName(overallStart.plusSeconds(intervalSeconds * 2), dtf), result.get(1));
  }

  @Test
  @Category(UnitTest.class)
  public void testGeometrySpansEntireSecondInterval()
  {
    GeometrySplitter splitter = createSplitter(TimeSpanGeometrySplitter.CompareType.EITHER_TIME);
    // Geometry end time is the overall start time. Should result in
    // no returned splits.
    WritableGeometry g = GeometryFactory.createPoint(0.0, 0.0);
    DateTime start = overallStart.plusSeconds(partialIntervalSeconds);
    g.setAttribute(startFieldName, start.toString(dtf));
    DateTime end = start.plusSeconds(intervalSeconds * 2);
    g.setAttribute(endFieldName, end.toString(dtf));
    List<String> result = splitter.splitGeometry(g);
    Assert.assertNotNull(result);
    Assert.assertEquals(3, result.size());
    // Should include intervals 1, 2 and 3
    Assert.assertEquals(getOutputName(overallStart.plusSeconds(intervalSeconds), dtf), result.get(0));
    Assert.assertEquals(getOutputName(overallStart.plusSeconds(intervalSeconds * 2), dtf), result.get(1));
    Assert.assertEquals(getOutputName(overallStart.plusSeconds(intervalSeconds * 3), dtf), result.get(2));
  }

  @Test
  @Category(UnitTest.class)
  public void testGeometrySpansLastIntervalEnd()
  {
    GeometrySplitter splitter = createSplitter(TimeSpanGeometrySplitter.CompareType.EITHER_TIME);
    // Geometry end time is the overall start time. Should result in
    // no returned splits.
    WritableGeometry g = GeometryFactory.createPoint(0.0, 0.0);
    DateTime start = overallEnd.minusSeconds(partialIntervalSeconds);
    g.setAttribute(startFieldName, start.toString(dtf));
    DateTime end = start.plusSeconds(intervalSeconds);
    g.setAttribute(endFieldName, end.toString(dtf));
    List<String> result = splitter.splitGeometry(g);
    Assert.assertNotNull(result);
    Assert.assertEquals(1, result.size());
    Assert.assertEquals(getOutputName(overallEnd, dtf), result.get(0));
  }

  @Test
  @Category(UnitTest.class)
  public void testGeometrySpansLastIntervalStart()
  {
    GeometrySplitter splitter = createSplitter(TimeSpanGeometrySplitter.CompareType.EITHER_TIME);
    // Geometry end time is the overall start time. Should result in
    // no returned splits.
    WritableGeometry g = GeometryFactory.createPoint(0.0, 0.0);
    DateTime start = overallEnd.minusSeconds(intervalSeconds + partialIntervalSeconds);
    g.setAttribute(startFieldName, start.toString(dtf));
    DateTime end = overallEnd.plusSeconds(partialIntervalSeconds);
    g.setAttribute(endFieldName, end.toString(dtf));
    List<String> result = splitter.splitGeometry(g);
    Assert.assertNotNull(result);
    Assert.assertEquals(2, result.size());
    Assert.assertEquals(getOutputName(overallEnd.minusSeconds(intervalSeconds), dtf), result.get(0));
    Assert.assertEquals(getOutputName(overallEnd, dtf), result.get(1));
  }

  @Test
  @Category(UnitTest.class)
  public void testGeometrySpansOverallTimeframe()
  {
    GeometrySplitter splitter = createSplitter(TimeSpanGeometrySplitter.CompareType.EITHER_TIME);
    // Geometry end time is the overall start time. Should result in
    // no returned splits.
    WritableGeometry g = GeometryFactory.createPoint(0.0, 0.0);
    DateTime start = overallStart.minusSeconds(partialIntervalSeconds);
    g.setAttribute(startFieldName, start.toString(dtf));
    DateTime end = overallEnd.plusSeconds(partialIntervalSeconds);
    g.setAttribute(endFieldName, end.toString(dtf));
    List<String> result = splitter.splitGeometry(g);
    Assert.assertNotNull(result);
    Assert.assertEquals(10, result.size());
    for (int i=0; i < 10; i++)
    {
      Assert.assertEquals("Interval " + i + " is incorrect",
          getOutputName( overallStart.plusSeconds(intervalSeconds * (i+1)), dtf), result.get(i));
    }
  }
}
