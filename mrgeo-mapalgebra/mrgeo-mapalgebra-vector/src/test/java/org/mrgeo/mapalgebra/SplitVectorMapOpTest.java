package org.mrgeo.mapalgebra;

import java.io.IOException;
import java.util.List;
import java.util.Vector;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mrgeo.column.Column;
import org.mrgeo.column.ColumnDefinitionFile;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.mapalgebra.parser.ParserException;
import org.mrgeo.mapreduce.job.JobCancelledException;
import org.mrgeo.mapreduce.job.JobFailedException;
import org.mrgeo.test.LocalRunnerTest;
import org.mrgeo.test.MapOpTestVectorUtils;
import org.mrgeo.test.TestUtils;

public class SplitVectorMapOpTest extends LocalRunnerTest
{
  private static final double EPSILON = 1e-8;
  private static MapOpTestVectorUtils testUtils;
  private static String _inputSingleTsv;
  private static String _inputMultiTsv;

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void init()
  {
    try
    {
      // Forces it to wipeout the input directory (since we copy files to it).
      TestUtils.composeInputHdfs(SplitVectorMapOpTest.class, true);
      testUtils = new MapOpTestVectorUtils(SplitVectorMapOpTest.class);
      
      HadoopFileUtils.copyToHdfs(
        new Path(testUtils.getInputLocal()), testUtils.getInputHdfs(), "inputSingle.tsv");
      HadoopFileUtils.copyToHdfs(
        new Path(testUtils.getInputLocal()), testUtils.getInputHdfs(), 
        "inputSingle.tsv.columns");
      _inputSingleTsv = new Path(testUtils.getInputHdfs(), "inputSingle.tsv").toString();

      HadoopFileUtils.copyToHdfs(
          new Path(testUtils.getInputLocal()), testUtils.getInputHdfs(), "inputMulti.tsv");
        HadoopFileUtils.copyToHdfs(
          new Path(testUtils.getInputLocal()), testUtils.getInputHdfs(), 
          "inputMulti.tsv.columns");
      _inputMultiTsv = new Path(testUtils.getInputHdfs(), "inputMulti.tsv").toString();
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }

  private void runTest(String inputPath, int splitCount, int currentSplit,
      String splitType, String testName)
          throws IOException, JobFailedException, JobCancelledException, ParserException
  {
    String tsvFileName = testName + ".tsv";
    String expression = 
        String.format("SplitVector([%s], %d, %d, \"%s\")",
          _inputSingleTsv, splitCount, currentSplit, splitType).replace("'", "\"");
    testUtils.runMapAlgebraExpression(conf, tsvFileName, expression);
    List<String> actualOutput = testUtils.readVectorOutputAsText(conf, new Path(testUtils.getOutputHdfs(), tsvFileName));
    List<String> input = testUtils.readVectorOutputAsText(conf, new Path(testUtils.getInputHdfs(), "inputSingle.tsv"));
    int expectedTestTypeOutputs = input.size() / splitCount;
    if (input.size() % splitCount > (currentSplit - 1))
    {
      expectedTestTypeOutputs++;
    }
    int expectedTrainingTypeOutputs = input.size() - expectedTestTypeOutputs;
    if (splitType.equalsIgnoreCase("test"))
    {
      Assert.assertEquals(expectedTestTypeOutputs, actualOutput.size());
    }
    else
    {
      Assert.assertEquals(expectedTrainingTypeOutputs, actualOutput.size());
    }
    int nextRelevantInputIndex = currentSplit - 1;
    int outputIndex = 0;
    int checkedRecords = 0;
    for (int inputIndex = 0; inputIndex < input.size(); inputIndex++)
    {
      if (splitType.equalsIgnoreCase("test"))
      {
        // The "test" split type means every Nth record from the input should be copied
        // from the input to the output (where N is splitCount) beginning with the
        // Kth record (where K is currentSplit - 1). So if splitCount is 5 and currentSplit
        // is 4, then records 3, 8, 13, 18, ... should be copied to the output.
        if (inputIndex == nextRelevantInputIndex)
        {
          Assert.assertEquals(input.get(inputIndex), actualOutput.get(outputIndex));
          checkedRecords++;
          nextRelevantInputIndex += splitCount;
          outputIndex++;
        }
      }
      else
      {
        // The "training" split means to copy all of the records except those that
        // would be copied by the "test" split.
        if (inputIndex == nextRelevantInputIndex)
        {
          // This would match a "test" split, so it should not show up in the output.
          nextRelevantInputIndex += splitCount;
        }
        else
        {
          Assert.assertEquals(input.get(inputIndex), actualOutput.get(outputIndex));
          checkedRecords++;
          outputIndex++;
        }
      }
    }
    if (splitType.equalsIgnoreCase("test"))
    {
      Assert.assertEquals("Did not verify the content of all the expected records.", expectedTestTypeOutputs, checkedRecords);
    }
    else
    {
      Assert.assertEquals("Did not verify the content of all the expected records.", expectedTrainingTypeOutputs, checkedRecords);
    }
    
    // Verify that the columns file is the same as that of the input
    Path inputColumnsFile = new Path(inputPath.toString() + ".columns");
    Path outputColumnsFile = new Path(testUtils.getOutputHdfs(), tsvFileName + ".columns");
    ColumnDefinitionFile inputCDF = new ColumnDefinitionFile(inputColumnsFile);
    ColumnDefinitionFile outputCDF = new ColumnDefinitionFile(outputColumnsFile);
    Vector<Column> inputColumns = inputCDF.getColumns();
    for (Column column : inputColumns)
    {
      Column outputColumn = outputCDF.getColumn(column.getName());
      Assert.assertNotNull("Missing column " + column.getName() + " in output", outputColumn);
      Assert.assertEquals(column.getType(), outputColumn.getType());
      Assert.assertEquals(0, outputColumn.getCount());
      Assert.assertEquals(0.0, outputColumn.getSum(), EPSILON);
      Assert.assertEquals(Double.MAX_VALUE, outputColumn.getMin(), EPSILON);
      Assert.assertEquals(-Double.MAX_VALUE, outputColumn.getMax(), EPSILON);
      Assert.assertEquals(Double.MAX_VALUE, outputColumn.getQuartile1(), EPSILON);
      Assert.assertEquals(Double.MAX_VALUE, outputColumn.getQuartile2(), EPSILON);
      Assert.assertEquals(Double.MAX_VALUE, outputColumn.getQuartile3(), EPSILON);
    }
  }

  private void runInlineCsvTest(int splitCount, int currentSplit,
      String splitType, String testName)
          throws IOException, JobFailedException, JobCancelledException, ParserException
  {
    String tsvFileName = testName + ".tsv";
    String[] features = new String[] {
        "'EVT0','POINT (65.88 32.63)'",
        "'EVT1','POINT (66.1338 32.1515)'",
        "'EVT2','POINT (65.48 31.99)'",
        "'EVT3','POINT (66.32 31.8)'",
        "'EVT4','POINT (65.77 31.64)'",
        "'EVT5','POINT (65.7615 31.603)'",
        "'EVT6','POINT (66.11 32.12)'",
        "'EVT7','POINT (65.78 31.83)'",
        "'EVT8','POINT (65.06 32.36)'",
        "'EVT9','POINT (65.56 31.63)'",
        "'EVT10','POINT (65.46 31.52)'",
        "'EVT11','POINT (65.65 31.66)'",
        "'EVT12','POINT (65.132 31.732517)'",
        "'EVT13','POINT (65.96 31.32)'",
        "'EVT14','POINT (65.46 32.62)'"
    };
    StringBuffer expression = new StringBuffer("events = InlineCsv(\"NAME,GEOMETRY\",\"");
    for (int i=0; i < features.length; i++)
    {
      if (i > 0)
      {
        expression.append(';');
      }
      expression.append(features[i]);
    }
    expression.append("\"); ");
    String splitVectorExpr = 
        String.format("SplitVector(events, %d, %d, \"%s\")",
          splitCount, currentSplit, splitType).replace("'", "\"");
    expression.append(splitVectorExpr);
    testUtils.runMapAlgebraExpression(conf, tsvFileName, expression.toString());
    List<String> actualOutput = testUtils.readVectorOutputAsText(conf, new Path(testUtils.getOutputHdfs(), tsvFileName));
    int expectedTestTypeOutputs = features.length / splitCount;
    if (features.length % splitCount > (currentSplit - 1))
    {
      expectedTestTypeOutputs++;
    }
    int expectedTrainingTypeOutputs = features.length - expectedTestTypeOutputs;
    if (splitType.equalsIgnoreCase("test"))
    {
      Assert.assertEquals(expectedTestTypeOutputs, actualOutput.size());
    }
    else
    {
      Assert.assertEquals(expectedTrainingTypeOutputs, actualOutput.size());
    }
    int nextRelevantInputIndex = currentSplit - 1;
    int outputIndex = 0;
    int checkedRecords = 0;
    for (int inputIndex = 0; inputIndex < features.length; inputIndex++)
    {
      if (splitType.equalsIgnoreCase("test"))
      {
        // The "test" split type means every Nth record from the input should be copied
        // from the input to the output (where N is splitCount) beginning with the
        // Kth record (where K is currentSplit - 1). So if splitCount is 5 and currentSplit
        // is 4, then records 3, 8, 13, 18, ... should be copied to the output.
        if (inputIndex == nextRelevantInputIndex)
        {
          // InlineCsv requires single quotes around individual fields, and that what is used in the
          // "features" test data. However, the output from SplitVector returns fields encapsulated
          // with a double quote. In order to reuse the "features" test data as the expected output
          // from SplitVector, we just replcae the single quote with a double quote before the
          // comparison.
          Assert.assertEquals(features[inputIndex].replace('\'', '"'), actualOutput.get(outputIndex));
          checkedRecords++;
          nextRelevantInputIndex += splitCount;
          outputIndex++;
        }
      }
      else
      {
        // The "training" split means to copy all of the records except those that
        // would be copied by the "test" split.
        if (inputIndex == nextRelevantInputIndex)
        {
          // This would match a "test" split, so it should not show up in the output.
          nextRelevantInputIndex += splitCount;
        }
        else
        {
          Assert.assertEquals(features[inputIndex], actualOutput.get(outputIndex));
          checkedRecords++;
          outputIndex++;
        }
      }
    }
    if (splitType.equalsIgnoreCase("test"))
    {
      Assert.assertEquals("Did not verify the content of all the expected records.", expectedTestTypeOutputs, checkedRecords);
    }
    else
    {
      Assert.assertEquals("Did not verify the content of all the expected records.", expectedTrainingTypeOutputs, checkedRecords);
    }
    
    // Verify that the columns file is the same as that of the input
//    Path inputColumnsFile = new Path(inputPath.toString() + ".columns");
//    Path outputColumnsFile = new Path(testUtils.getOutputHdfs(), tsvFileName + ".columns");
//    ColumnDefinitionFile inputCDF = new ColumnDefinitionFile(inputColumnsFile);
//    ColumnDefinitionFile outputCDF = new ColumnDefinitionFile(outputColumnsFile);
//    Vector<Column> inputColumns = inputCDF.getColumns();
//    for (Column column : inputColumns)
//    {
//      Column outputColumn = outputCDF.getColumn(column.getName());
//      Assert.assertNotNull("Missing column " + column.getName() + " in output", outputColumn);
//      Assert.assertEquals(column.getType(), outputColumn.getType());
//      Assert.assertEquals(0, outputColumn.getCount());
//      Assert.assertEquals(0.0, outputColumn.getSum(), EPSILON);
//      Assert.assertEquals(Double.MAX_VALUE, outputColumn.getMin(), EPSILON);
//      Assert.assertEquals(-Double.MAX_VALUE, outputColumn.getMax(), EPSILON);
//      Assert.assertEquals(Double.MAX_VALUE, outputColumn.getQuartile1(), EPSILON);
//      Assert.assertEquals(Double.MAX_VALUE, outputColumn.getQuartile2(), EPSILON);
//      Assert.assertEquals(Double.MAX_VALUE, outputColumn.getQuartile3(), EPSILON);
//    }
  }

  @Test
  @Category(UnitTest.class)
  public void testFromInlineCsv() throws Exception
  {
    runInlineCsvTest(3, 1, "test", name.getMethodName());
  }

  @Test
  @Category(UnitTest.class)
  public void testSplit1of3FromTsvSingleFile() throws Exception
  {
    runTest(_inputSingleTsv, 3, 1, "test", name.getMethodName());
  }

  @Test
  @Category(UnitTest.class)
  public void testSplit2of3FromTsvSingleFile() throws Exception
  {
    runTest(_inputSingleTsv, 3, 2, "test", name.getMethodName());
  }

  @Test
  @Category(UnitTest.class)
  public void testSplit3of3FromTsvSingleFile() throws Exception
  {
    runTest(_inputSingleTsv, 3, 3, "test", name.getMethodName());
  }

  @Test
  @Category(UnitTest.class)
  public void testSplit1of6FromTsvSingleFile() throws Exception
  {
    runTest(_inputSingleTsv, 6, 1, "test", name.getMethodName());
  }

  @Test
  @Category(UnitTest.class)
  public void testSplit6of6FromTsvSingleFile() throws Exception
  {
    runTest(_inputSingleTsv, 6, 6, "TEST", name.getMethodName());
  }

  @Test
  @Category(UnitTest.class)
  public void trainingSplit1of3FromTsvSingleFile() throws Exception
  {
    runTest(_inputSingleTsv, 3, 1, "training", name.getMethodName());
  }

  @Test
  @Category(UnitTest.class)
  public void trainingSplit2of3FromTsvSingleFile() throws Exception
  {
    runTest(_inputSingleTsv, 3, 2, "training", name.getMethodName());
  }

  @Test
  @Category(UnitTest.class)
  public void trainingSplit3of3FromTsvSingleFile() throws Exception
  {
    runTest(_inputSingleTsv, 3, 3, "training", name.getMethodName());
  }

  @Test
  @Category(UnitTest.class)
  public void trainingSplit1of5FromTsvSingleFile() throws Exception
  {
    runTest(_inputSingleTsv, 5, 1, "training", name.getMethodName());
  }

  @Test
  @Category(UnitTest.class)
  public void trainingSplit5of5FromTsvSingleFile() throws Exception
  {
    runTest(_inputSingleTsv, 5, 5, "TRAINING", name.getMethodName());
  }

  @Test
  @Category(UnitTest.class)
  public void testSplit1of6FromTsvMultiFile() throws Exception
  {
    runTest(_inputMultiTsv, 6, 1, "test", name.getMethodName());
  }

  @Test
  @Category(UnitTest.class)
  public void testSplit6of6FromTsvMultiFile() throws Exception
  {
    runTest(_inputMultiTsv, 6, 6, "TEST", name.getMethodName());
  }
  @Test
  @Category(UnitTest.class)
  public void trainingSplit1of7FromTsvMultiFile() throws Exception
  {
    runTest(_inputSingleTsv, 7, 1, "training", name.getMethodName());
  }

  @Test
  @Category(UnitTest.class)
  public void trainingSplit5of7FromTsvMultiFile() throws Exception
  {
    runTest(_inputSingleTsv, 7, 5, "TRAINING", name.getMethodName());
  }

}
