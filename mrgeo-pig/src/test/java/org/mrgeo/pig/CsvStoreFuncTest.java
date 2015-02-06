package org.mrgeo.pig;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.rasterops.OpImageRegistrar;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.junit.IntegrationTest;
import org.mrgeo.test.LocalRunnerTest;
import org.mrgeo.test.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

@SuppressWarnings("static-method")
public class CsvStoreFuncTest extends LocalRunnerTest
{
  @SuppressWarnings("unused")
  private static final Logger log = LoggerFactory.getLogger(CsvStoreFuncTest.class);

  private static String _input;
  private static Path _outputHdfs;

  @Before
  public void setUp()
  {
    OpImageRegistrar.registerMrGeoOps();
  }

  @BeforeClass 
  public static void init()
  {
    try
    {
      _input = TestUtils.composeInputDir(CsvStoreFuncTest.class);
      _outputHdfs = TestUtils.composeOutputHdfs(CsvStoreFuncTest.class);

      HadoopFileUtils.copyToHdfs(new Path(_input), _outputHdfs, "test1-in.tsv");
      HadoopFileUtils.copyToHdfs(new Path(_input), _outputHdfs, "test1-in.tsv.columns");

    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }

  @Test
  @Category(IntegrationTest.class)
  public void testBasics() throws Exception
  {
    try
    {

      PigQuerier pq = new PigQuerier();

      pq.query("result = load '" + _outputHdfs + "/test1-in.tsv' using PigStorage() AS (a:double, b:double, c:double, class:chararray);", new Path(
          _outputHdfs, "test1.tsv"), this.conf);

      TestUtils.compareTextFiles(_outputHdfs + "/test1.tsv/part-m-00000.tsv", _input + "/test1.tsv");
      TestUtils.compareTextFiles(_outputHdfs + "/test1.tsv.columns", _input + "/test1.tsv.columns");

//    Assert.assertEquals(TestUtils.readPath(new Path(_outputHdfs + "/test1.tsv/part-m-00000.tsv")),
//         TestUtils.readFile(new File(_input + "/test1.tsv")));

//      Assert.assertEquals(TestUtils.readPath(new Path(_outputHdfs + "/test1.tsv.columns")),
//          TestUtils.readFile(new File(_input + "/test1.tsv.columns")));

    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }

}
