package org.mrgeo.pig;

import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.rasterops.OpImageRegistrar;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.junit.IntegrationTest;
import org.mrgeo.test.LocalRunnerTest;
import org.mrgeo.utils.MapAlgebraTestUtils;
import org.mrgeo.test.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("static-method")
public class AutoLoadFuncTest extends LocalRunnerTest
{
  @SuppressWarnings("unused")
  private static final Logger log = LoggerFactory.getLogger(AutoLoadFuncTest.class);

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
      _input = TestUtils.composeInputDir(AutoLoadFuncTest.class);
      _outputHdfs = TestUtils.composeOutputHdfs(AutoLoadFuncTest.class);
      
      HadoopFileUtils.copyToHdfs(new Path(_input), _outputHdfs, "input.tsv");
      HadoopFileUtils.copyToHdfs(new Path(_input), _outputHdfs, "input.tsv.columns");

      HadoopFileUtils.copyToHdfs(new Path(_input), _outputHdfs, "AmbulatoryPt.shp");
      HadoopFileUtils.copyToHdfs(new Path(_input), _outputHdfs, "AmbulatoryPt.prj");
      HadoopFileUtils.copyToHdfs(new Path(_input), _outputHdfs, "AmbulatoryPt.dbf");
      HadoopFileUtils.copyToHdfs(new Path(_input), _outputHdfs, "AmbulatoryPt.shx");

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

      pq.query("result = load '" + _outputHdfs + "/input.tsv' using MrGeoLoader;", new Path(
          _outputHdfs, "myoutput.tsv"), this.conf);

//      Assert.assertEquals(TestUtils.readPath(new Path(_outputHdfs + "/myoutput.tsv/part-m-00000.tsv")),
//        TestUtils.readFile(new File(_input + "/myoutput.tsv")));
      MapAlgebraTestUtils.compareTSV(_input + "/myoutput.tsv", _outputHdfs + "/myoutput.tsv/part-m-00000.tsv");



      pq.query("result = load '" + _outputHdfs + "/AmbulatoryPt.shp' using MrGeoLoader;", new Path(
          _outputHdfs, "AmbulatoryPt.tsv"), this.conf);

//      Assert.assertEquals(TestUtils
//        .readPath(new Path(_outputHdfs + "/AmbulatoryPt.tsv/part-m-00000.tsv")), TestUtils
//        .readFile(new File(_input + "/AmbulatoryPtOut.tsv")));
      
      MapAlgebraTestUtils.compareTSV(_input + "/AmbulatoryPtOut.tsv", _outputHdfs + "/AmbulatoryPt.tsv/part-m-00000.tsv");

      //TODO:  make this test work
//      pq.query("in = LOAD '" + _outputHdfs + "/input.tsv' using MrGeoLoader;\n" +
//          "result = FILTER in BY a >= 0;", new Path(
//          _outputHdfs, "test3.tsv"), this.conf);
//
////      Assert.assertEquals(TestUtils.readPath(new Path(_outputHdfs + "/test3.tsv/part-m-00000.tsv")),
////        TestUtils.readFile(new File(_input + "/test3.tsv")));
//
//      MapAlgebraTestUtils.compareTSV(_input + "/test3.tsv", _outputHdfs + "/test3.tsv/part-m-00000.tsv");
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }

}
