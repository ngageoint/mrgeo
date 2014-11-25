package org.mrgeo.mapreduce;

import junit.framework.Assert;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.junit.IntegrationTest;
import org.mrgeo.test.LocalRunnerTest;
import org.mrgeo.test.TestUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

@SuppressWarnings("static-method")
public class SortVectorNumericDriverIntegrationTest extends LocalRunnerTest
{
  private static Path outputHdfs;
  private static String input;
  
  @BeforeClass
  public static void init() throws IOException
  {
    outputHdfs = TestUtils.composeOutputHdfs(SortVectorNumericDriverIntegrationTest.class);
    input = TestUtils.composeInputDir(SortVectorNumericDriverIntegrationTest.class);
    
    HadoopFileUtils.copyToHdfs(input, outputHdfs, "input.tsv");
    HadoopFileUtils.copyToHdfs(input, outputHdfs, "input.tsv.columns");
    
    HadoopFileUtils.copyToHdfs(input, outputHdfs, "input_null.tsv");
    HadoopFileUtils.copyToHdfs(input, outputHdfs, "input_null.tsv.columns");
  }
  

  // Test sorting an input file with no null values in the sort column.
  @Test
  @Category(IntegrationTest.class)
  public void integrationBasics() throws Exception
  {

    Path inputTest = new Path(outputHdfs, "input.tsv");
    Path outputTest = new Path(outputHdfs, "output_a.tsv");

    SortVectorNumericDriver sv = new SortVectorNumericDriver();
    String columnName = "a";
    // The min/max values below are from the range of values in the input
    sv.addSortColumn(columnName, -1.5, 2.7);
    sv.runJob(inputTest.toString(), outputTest.toString(), TextOutputFormat.class,
        this.conf, null);
    
    // Compare the output from the sort driver with an output file
    // that is already sorted by the same column.
    Path testOut = new Path(outputTest, "part-r-00000");
    byte[] bufferActual = new byte[50000];
    
    FSDataInputStream fdis = HadoopFileUtils.getFileSystem(this.conf, testOut).open(testOut);
    int actualBytesRead = fdis.read(bufferActual);
    fdis.close();
    
    byte[] bufferExpected = new byte[50000];
    FileInputStream fis = new FileInputStream(new File(input + "/output_a.tsv"));
    int expectedBytesRead = fis.read(bufferExpected);
    fis.close();

    Assert.assertEquals(expectedBytesRead, actualBytesRead);
    Assert.assertTrue(java.util.Arrays.equals(bufferExpected, bufferActual));
    Assert.assertEquals(1, sv.getNumPartitions());
    Assert.assertEquals(400, sv.getTotalRecords(columnName));
  }

  // Test sorting by the same input file as integrationBasics1() but by a different column.
  @Test
  @Category(IntegrationTest.class)
  public void integrationBasics2() throws Exception
  {
    Path inputTest = new Path(outputHdfs, "input.tsv");
    Path outputTest = new Path(outputHdfs, "output_b.tsv");

    SortVectorNumericDriver sv = new SortVectorNumericDriver();
    String columnName = "b";
    // The min/max values below are from the range of values in the input
    sv.addSortColumn(columnName, -0.16, 3.2);
    sv.runJob(inputTest.toString(), outputTest.toString(), TextOutputFormat.class,
        this.conf, null);
    
    // Compare the output from the sort driver with an output file
    // that is already sorted by the same column.
    Path testOut = new Path(outputTest, "part-r-00000");
    byte[] bufferActual = new byte[50000];
    
    FSDataInputStream fdis = HadoopFileUtils.getFileSystem(this.conf, testOut).open(testOut);
    int actualBytesRead = fdis.read(bufferActual);
    fdis.close();
    
    byte[] bufferExpected = new byte[50000];
    FileInputStream fis = new FileInputStream(new File(input + "/output_b.tsv"));
    int expectedBytesRead = fis.read(bufferExpected);
    fis.close();

    Assert.assertEquals(expectedBytesRead, actualBytesRead);
    Assert.assertTrue(java.util.Arrays.equals(bufferExpected, bufferActual));
    Assert.assertEquals(1, sv.getNumPartitions());
    Assert.assertEquals(400, sv.getTotalRecords(columnName));
  }

  // Test sorting where some of the input values for the sort column are null
  @Test
  @Category(IntegrationTest.class)
  public void integrationWithNulls() throws Exception
  {
    Path inputTest = new Path(outputHdfs, "input_null.tsv");
    Path outputTest = new Path(outputHdfs, "output_a_null.tsv");

    SortVectorNumericDriver sv = new SortVectorNumericDriver();
    String columnName = "a";
    // The min/max values below are from the range of values in the input
    sv.addSortColumn(columnName, -1.5, 2.7);
    sv.runJob(inputTest.toString(), outputTest.toString(), TextOutputFormat.class,
        this.conf, null);
    
    // Compare the output from the sort driver with an output file
    // that is already sorted by the same column.
    Path testOut = new Path(outputTest, "part-r-00000");
    byte[] bufferActual = new byte[50000];
    FSDataInputStream fdis = HadoopFileUtils.getFileSystem(this.conf, testOut).open(testOut);
    int actualBytesRead = fdis.read(bufferActual);
    fdis.close();
    
    byte[] bufferExpected = new byte[50000];
    FileInputStream fis = new FileInputStream(new File(input + "/output_a_null.tsv"));
    int expectedBytesRead = fis.read(bufferExpected);
    fis.close();

    Assert.assertEquals(expectedBytesRead, actualBytesRead);
    Assert.assertTrue(java.util.Arrays.equals(bufferExpected, bufferActual));
    Assert.assertEquals(1, sv.getNumPartitions());
    Assert.assertEquals(395, sv.getTotalRecords(columnName)); // because 5 of the rows were set to null values
  }
}
