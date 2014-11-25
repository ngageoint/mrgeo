/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */

package org.mrgeo.mapreduce;

import org.mrgeo.test.LocalRunnerTest;

/**
 * @author Mingjie Su
 * 
 */
public class VectorizeRasterIntegrationTest extends LocalRunnerTest
{
  private static final double epsilon = 0.00000001;

//  private static MrsPyramidv1 outputPyramid;
//  private static WKTReader wktReader;
//  private static HashMap<Double, Geometry> baselinePolys;
//  private static HashMap<Double, Double> baselineAreaValues;


//  @BeforeClass
//  public static void init() throws IOException, ParseException
//  {
//    Path outputHdfs = TestUtils.composeOutputHdfs(VectorizeRasterIntegrationTest.class);
//    String input = TestUtils.composeInputDir(VectorizeRasterIntegrationTest.class);
//
//    Path path = new Path(input, "rc_watershed_test.tif");
//    BufferedImage raster = ImageIO.read(new File(path.toString()));
//
//    Path rasterPath = new Path(outputHdfs, "raster");
//    MrsPyramidv1.delete(rasterPath);
//    outputPyramid = MrsPyramidv1.createCopy(rasterPath, new Bounds(-118, 38.3, -117.988, 38.29),
//        raster, 256, -1);
//    outputPyramid.save();
//
//
//    wktReader = new WKTReader();
//    baselinePolys = new HashMap<Double, Geometry>();
//    baselineAreaValues = new HashMap<Double, Double>();
//
//    String baseline = TestUtils.readFile(new File(input + "result.tsv"));
//    String[] baseLineVector = baseline.split("\n");
//    for (int i = 0; i < baseLineVector.length; i++)
//    {
//      String line = baseLineVector[i];
//      String[] lineInfo = line.split("\t");
//      if (lineInfo.length == 3)
//      {
//        Geometry poly = wktReader.read(lineInfo[1]);
//        baselinePolys.put(Double.valueOf(lineInfo[0]), poly);
//        baselineAreaValues.put(Double.valueOf(lineInfo[0]), Double.valueOf(lineInfo[2]));
//      }
//    }
//  }
//
//  @Test
//  @Category(IntegrationTest.class)
//  public void testBasics() throws Exception
//  {
//    Path outputHdfs = TestUtils.composeOutputHdfs(VectorizeRasterIntegrationTest.class);
//
//    init();
//    Path result = new Path(outputHdfs, "result");
//    new VectorizeRasterDriver().run(outputPyramid, result, this.conf, null, null);
//
//    String test = TestUtils.readPath(new Path(result, "part-r-00000"));
//
//    ArrayList<Geometry> testPolys = new ArrayList<Geometry>();
//    ArrayList<Double> testValues = new ArrayList<Double>();
//    ArrayList<Double> testAreaValues = new ArrayList<Double>();
//
//    String[] testVector = test.split("\n");
//    for (int i = 0; i < testVector.length; i++)
//    {
//      String line = testVector[i];
//      String[] lineInfo = line.split("\t");
//      if (lineInfo.length == 3)
//      {
//        testValues.add(Double.valueOf(lineInfo[0]));
//        Geometry poly = wktReader.read(lineInfo[1]);
//        testPolys.add(poly);
//        testAreaValues.add(Double.valueOf(lineInfo[2]));
//      }
//    }
//
//    Assert.assertEquals(baselinePolys.size(), testPolys.size());
//    Assert.assertEquals(baselinePolys.size(), testValues.size());
//    Assert.assertEquals(baselinePolys.size(), testAreaValues.size());
//
//    for (int i = 0; i < baselinePolys.size(); i++)
//    {
//      Assert.assertTrue(testPolys.get(i).equals(baselinePolys.get(testValues.get(i))));
//      Assert.assertTrue((testAreaValues.get(i) - baselineAreaValues.get(testValues.get(i)) < epsilon));
//    }
//  }
//  @Test
//  @Category(UnitTest.class)
//  public void testMapper() throws IOException, ParseException
//  {
//    Configuration config = new Configuration();
//    init();
//    if (outputPyramid != null)
//    {
//      MrsPyramidTiledWritable writable = new MrsPyramidTiledWritable();
//      writable.setPath(outputPyramid.getPath().toString());
//      writable.setLevel(0);
//      writable.setTileX(0);
//      writable.setTileY(0);
//
//      MapDriver<LongWritable, MrsPyramidTiledWritable, Text, Text> driver = 
//          new MapDriver<LongWritable, MrsPyramidTiledWritable, Text, Text>()
//          .withConfiguration(config)
//          .withMapper(new VectorizeRasterMapper())
//          .withInputKey(new LongWritable(1))
//          .withInputValue(writable);
//
//      java.util.List<Pair<Text, Text>> l = driver.run();
//
//      Assert.assertEquals(baselinePolys.size(), l.size());
//      java.util.ListIterator<Pair<Text, Text>> iter = l.listIterator();
//      while (iter.hasNext())
//      {
//        Pair<Text, Text> item = iter.next();
//        String first = item.getFirst().toString();
//        String second = item.getSecond().toString();
//        Geometry geom = wktReader.read(second);
//        Assert.assertTrue(baselinePolys.get(Double.valueOf(first)).equals(geom));
//      }
//    }
//  }
//
//  @Test
//  @Category(UnitTest.class)
//  public void testReducer() throws IOException, ParseException
//  {
//    Configuration config = new Configuration();
//    List<Text> inputs = new ArrayList<Text>();
//    String inputValue1 = "POLYGON ((0 0,0 1,1 1,1 0,0 0))";
//    String inputValue2 = "POLYGON ((1 0,1 1,2 1,2 0,1 0))";
//    Geometry baseGeom = wktReader.read("POLYGON ((0 0,0 1,2 1,2 0,0 0))");
//    inputs.add(new Text(inputValue1));
//    inputs.add(new Text(inputValue2));
//    ReduceDriver<Text, Text, Text, Text> driver = new ReduceDriver<Text, Text, Text, Text>()
//        .withConfiguration(config)
//        .withReducer(new VectorizeRasterReducer())
//        .withInputKey(new Text("3712.0"))
//        .withInputValues(inputs);
//    java.util.List<Pair<Text, Text>> l = driver.run();
//    // Test the results
//    Assert.assertEquals(1, l.size());
//    java.util.ListIterator<Pair<Text, Text>> iter = l.listIterator();
//    Assert.assertTrue(iter.hasNext());
//    Pair<Text, Text> item = iter.next();
//    Geometry output = wktReader.read(item.getSecond().toString());
//    Assert.assertTrue(baseGeom.equals(output));
//  }
}
