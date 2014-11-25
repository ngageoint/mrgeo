/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.classifier;

import junit.framework.Assert;
import org.apache.hadoop.fs.Path;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.column.Column;
import org.mrgeo.column.ColumnDefinitionFile;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.junit.IntegrationTest;
import org.mrgeo.test.LocalRunnerTest;
import org.mrgeo.test.TestUtils;

import java.io.IOException;

public class CalculateStatisticsIntegrationTest extends LocalRunnerTest
{
  private static Path outputHdfs;
  private static String input;

  @BeforeClass
  public static void init() throws IOException
  {
    outputHdfs = TestUtils.composeOutputHdfs(CalculateStatisticsIntegrationTest.class);
    input = TestUtils.composeInputDir(CalculateStatisticsIntegrationTest.class);
  }
  @Test 
  @Category(IntegrationTest.class)
  public void testBasics() throws Exception
  {

    HadoopFileUtils.copyToHdfs(input, outputHdfs, "input.tsv");
    HadoopFileUtils.copyToHdfs(input, outputHdfs, "input.tsv.columns");

    Path inputTsv = new Path(outputHdfs, "input.tsv");
    CalculateStatisticsDriver.runJob(new Path[] { inputTsv }, this.conf, false, null);

    ColumnDefinitionFile cdf = new ColumnDefinitionFile(inputTsv.toString() + ".columns");

    // check the "number" column
    Column col = cdf.getColumn("number");
    Assert.assertNotNull("Unable to get the 'number' column", col);
    Assert.assertEquals(col.getMin(), 1.0);
    Assert.assertEquals(Column.FactorType.Numeric.ordinal(), col.getType().ordinal());
    Assert.assertEquals(col.getMax(), 4.0);
    Assert.assertEquals(col.getCount(), 4);
    Assert.assertEquals(col.getSum(), 10.0);
    Assert.assertFalse(col.isQuartile1Valid());
    Assert.assertFalse(col.isQuartile2Valid());
    Assert.assertFalse(col.isQuartile3Valid());

    // check the "letter" column
    col = cdf.getColumn("letter");
    Assert.assertNotNull("Unable to get the 'letter' column", col);
    Assert.assertEquals(col.getCount(), 3);
    Assert.assertEquals(Column.FactorType.Nominal.ordinal(), col.getType().ordinal());
    Assert.assertFalse(col.isQuartile1Valid());
    Assert.assertFalse(col.isQuartile2Valid());
    Assert.assertFalse(col.isQuartile3Valid());

    // check the "both" column
    col = cdf.getColumn("both");
    Assert.assertNotNull("Unable to get the 'both' column", col);
    Assert.assertEquals(col.getCount(), 2);
    Assert.assertEquals(Column.FactorType.Nominal.ordinal(), col.getType().ordinal());
    Assert.assertFalse(col.isQuartile1Valid());
    Assert.assertFalse(col.isQuartile2Valid());
    Assert.assertFalse(col.isQuartile3Valid());
  }

  @Test 
  @Category(IntegrationTest.class)
  public void testQuartilesAndNulls() throws Exception
  {

    HadoopFileUtils.copyToHdfs(input, outputHdfs, "input1.tsv");
    HadoopFileUtils.copyToHdfs(input, outputHdfs, "input1.tsv.columns");

    Path inputTsv = new Path(outputHdfs, "input1.tsv");
    CalculateStatisticsDriver.runJob(new Path[] { inputTsv }, this.conf, true, null);

    ColumnDefinitionFile cdf = new ColumnDefinitionFile(inputTsv.toString() + ".columns");
    Assert.assertEquals(Column.FactorType.Numeric.ordinal(), cdf.getColumns().get(0).getType().ordinal());

    // check the "col1" column
    {
      Column c = cdf.getColumns().get(0);
      Assert.assertEquals(Column.FactorType.Numeric.ordinal(), cdf.getColumns().get(1).getType().ordinal());
      Assert.assertEquals(10, c.getCount());
      Assert.assertEquals(1.0, c.getMin());
      Assert.assertEquals(10.0, c.getMax());
      Assert.assertEquals(55.0, c.getSum());
      Assert.assertTrue(c.isQuartile1Valid());
      Assert.assertEquals(3.0, c.getQuartile1());
      Assert.assertTrue(c.isQuartile2Valid());
      Assert.assertEquals(5.5, c.getQuartile2());
      Assert.assertTrue(c.isQuartile3Valid());
      Assert.assertEquals(8.0, c.getQuartile3());
    }

    // check the "col2" column
    {
      Column c = cdf.getColumns().get(1);
      Assert.assertEquals(Column.FactorType.Numeric.ordinal(), cdf.getColumns().get(1).getType().ordinal());
      Assert.assertEquals(9, c.getCount());
      Assert.assertEquals(10.0, c.getMin());
      Assert.assertEquals(100.0, c.getMax());
      Assert.assertEquals(510.0, c.getSum());
      Assert.assertTrue(c.isQuartile1Valid());
      Assert.assertEquals(25.0, c.getQuartile1());
      Assert.assertTrue(c.isQuartile2Valid());
      Assert.assertEquals(60.0, c.getQuartile2());
      Assert.assertTrue(c.isQuartile3Valid());
      Assert.assertEquals(85.0, c.getQuartile3());
    }

    // check the "col3" column
    {
      Column c = cdf.getColumns().get(2);
      Assert.assertEquals(Column.FactorType.Numeric.ordinal(), cdf.getColumns().get(2).getType().ordinal());
      Assert.assertEquals(8, c.getCount());
      Assert.assertEquals(200.0, c.getMin());
      Assert.assertEquals(1000.0, c.getMax());
      Assert.assertEquals(5100.0, c.getSum());
      Assert.assertTrue(c.isQuartile1Valid());
      Assert.assertEquals(450.0, c.getQuartile1());
      Assert.assertTrue(c.isQuartile2Valid());
      Assert.assertEquals(650.0, c.getQuartile2());
      Assert.assertTrue(c.isQuartile3Valid());
      Assert.assertEquals(850.0, c.getQuartile3());
    }

    // check the "col4" column - has all null values, so should be no stats
    {
      Column c = cdf.getColumns().get(3);
      Assert.assertEquals(Column.FactorType.Numeric.ordinal(), cdf.getColumns().get(2).getType().ordinal());
      Assert.assertEquals(0, c.getCount());
      Assert.assertFalse(c.isMinValid());
      Assert.assertFalse(c.isMaxValid());
      Assert.assertFalse(c.isQuartile1Valid());
      Assert.assertFalse(c.isQuartile2Valid());
      Assert.assertFalse(c.isQuartile3Valid());
    }
  }
}
