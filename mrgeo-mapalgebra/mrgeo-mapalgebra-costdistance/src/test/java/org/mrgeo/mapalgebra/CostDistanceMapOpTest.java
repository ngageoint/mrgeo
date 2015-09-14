package org.mrgeo.mapalgebra;

import java.util.Vector;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.mapalgebra.parser.ParserAdapter;
import org.mrgeo.mapalgebra.parser.ParserAdapterFactory;
import org.mrgeo.mapalgebra.parser.ParserException;
import org.mrgeo.mapalgebra.parser.ParserNode;
import org.mrgeo.test.LocalRunnerTest;

/**
 * Contains fast-running tests for CostDistance map algebra function.
 * Tests that need to actually execute the cost distance functionality
 * should be placed in CostDistanceMapOpIntegrationTest, not here.
 */
public class CostDistanceMapOpTest extends LocalRunnerTest
{
  private static final double EPSILON = 1e-8;
  private static final String usage = "CostDistance takes the following arguments " + 
      "(source point, [friction zoom level], friction raster, [maxCost], [minX, minY, maxX, maxY])";

  private ParserAdapter parser;

  @Before
  public void setup()
  {
    parser = ParserAdapterFactory.createParserAdapter();
    parser.initialize();
    parser.addFunction("CostDistance");
    parser.initializeForTesting();
  }

  @Test
  @Category(UnitTest.class)
  public void testNoArgs() throws ParserException
  {
    Vector<ParserNode> children = new Vector<ParserNode>();
    CostDistanceMapOp mapOp = new CostDistanceMapOp();

    String exp = "CostDistance()";
//    MapAlgebraParser map = new MapAlgebraParser();
    ParserNode node = parser.parse(exp, null);
    for (int i=0; i < node.getNumChildren(); i++)
    {
      children.add(node.getChild(i));
    }
    try
    {
      mapOp.processChildren(children, parser);
      Assert.fail("Expected IllegalArgumentException due to missing arguments");
    }
    catch(IllegalArgumentException e)
    {
      Assert.assertEquals(usage, e.getMessage());
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testOneArgs() throws ParserException
  {
    Vector<ParserNode> children = new Vector<ParserNode>();
    CostDistanceMapOp mapOp = new CostDistanceMapOp();

    String exp = "CostDistance([source])";
//    MapAlgebraParser map = new MapAlgebraParser();
    ParserNode node = parser.parse(exp, null);
    for (int i=0; i < node.getNumChildren(); i++)
    {
      children.add(node.getChild(i));
    }
    try
    {
      mapOp.processChildren(children, parser);
      Assert.fail("Expected IllegalArgumentException due to missing arguments");
    }
    catch(IllegalArgumentException e)
    {
      Assert.assertEquals(usage, e.getMessage());
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testBaseArgs() throws ParserException
  {
    Vector<ParserNode> children = new Vector<ParserNode>();
    CostDistanceMapOp mapOp = new CostDistanceMapOp();

    String exp = "CostDistance([source], [friction])";
//    MapAlgebraParser map = new MapAlgebraParser();
    ParserNode node = parser.parse(exp, null);
    for (int i=0; i < node.getNumChildren(); i++)
    {
      children.add(node.getChild(i));
    }
    Vector<ParserNode> result = mapOp.processChildren(children, parser);
    Assert.assertEquals(2, result.size());
    Assert.assertEquals((double)-1.0,  mapOp.maxCost, EPSILON);
    Assert.assertEquals(-1, mapOp.zoomLevel);
    Assert.assertEquals(-1.0, mapOp.maxCost, EPSILON);
  }

  @Test
  @Category(UnitTest.class)
  public void testWithOptionalZoom() throws ParserException
  {
    Vector<ParserNode> children = new Vector<ParserNode>();
    CostDistanceMapOp mapOp = new CostDistanceMapOp();

    String exp = "CostDistance([source], 10, [friction])";
//    MapAlgebraParser map = new MapAlgebraParser();
    ParserNode node = parser.parse(exp, null);
    for (int i=0; i < node.getNumChildren(); i++)
    {
      children.add(node.getChild(i));
    }
    Vector<ParserNode> result = mapOp.processChildren(children, parser);
    Assert.assertEquals(2, result.size());
    Assert.assertEquals((double)-1.0,  mapOp.maxCost, EPSILON);
    Assert.assertEquals(10, mapOp.zoomLevel);
    Assert.assertEquals(-1.0, mapOp.maxCost, EPSILON);
  }

  @Test
  @Category(UnitTest.class)
  public void testWithInvalidZoom() throws ParserException
  {
    Vector<ParserNode> children = new Vector<ParserNode>();
    CostDistanceMapOp mapOp = new CostDistanceMapOp();

    String exp = "CostDistance([source], \"abc\", [friction])";
//    MapAlgebraParser map = new MapAlgebraParser();
    ParserNode node = parser.parse(exp, null);
    for (int i=0; i < node.getNumChildren(); i++)
    {
      children.add(node.getChild(i));
    }
    try
    {
      mapOp.processChildren(children, parser);
      Assert.fail("Expected IllegalArgumentException for invalid zoom level");
    }
    catch(IllegalArgumentException e)
    {
      Assert.assertNotNull(e.getMessage());
      Assert.assertTrue(e.getMessage().startsWith("You must specify a valid number for maxCost."));
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testWithOptionalMaxCost() throws ParserException
  {
    Vector<ParserNode> children = new Vector<ParserNode>();
    CostDistanceMapOp mapOp = new CostDistanceMapOp();

    String exp = "CostDistance([source], [friction], 50000.0)";
//    MapAlgebraParser map = new MapAlgebraParser();
    ParserNode node = parser.parse(exp, null);
    for (int i=0; i < node.getNumChildren(); i++)
    {
      children.add(node.getChild(i));
    }
    Vector<ParserNode> result = mapOp.processChildren(children, parser);
    Assert.assertEquals(2, result.size());
    Assert.assertEquals(-1, mapOp.zoomLevel);
    Assert.assertEquals(50000.0, mapOp.maxCost, EPSILON);
  }

  @Test
  @Category(UnitTest.class)
  public void testWithInvalidMaxCost() throws ParserException
  {
    Vector<ParserNode> children = new Vector<ParserNode>();
    CostDistanceMapOp mapOp = new CostDistanceMapOp();

    String exp = "CostDistance([source], [friction], \"abc\")";
//    MapAlgebraParser map = new MapAlgebraParser();
    ParserNode node = parser.parse(exp, null);
    for (int i=0; i < node.getNumChildren(); i++)
    {
      children.add(node.getChild(i));
    }
    try
    {
      mapOp.processChildren(children, parser);
      Assert.fail("Expected IllegalArgumentException for invalid zoom level");
    }
    catch(IllegalArgumentException e)
    {
      Assert.assertNotNull(e.getMessage());
      Assert.assertTrue(e.getMessage().startsWith("You must specify a valid number for maxCost."));
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testWithZoomAndMaxCost() throws ParserException
  {
    Vector<ParserNode> children = new Vector<ParserNode>();
    CostDistanceMapOp mapOp = new CostDistanceMapOp();

    String exp = "CostDistance([source], 10, [friction], 50000.0)";
//    MapAlgebraParser map = new MapAlgebraParser();
    ParserNode node = parser.parse(exp, null);
    for (int i=0; i < node.getNumChildren(); i++)
    {
      children.add(node.getChild(i));
    }
    Vector<ParserNode> result = mapOp.processChildren(children, parser);
    Assert.assertEquals(2, result.size());
    Assert.assertEquals(10, mapOp.zoomLevel);
    Assert.assertEquals(50000.0, mapOp.maxCost, EPSILON);
  }

  @Test
  @Category(UnitTest.class)
  public void testWithBounds() throws ParserException
  {
    Vector<ParserNode> children = new Vector<ParserNode>();
    CostDistanceMapOp mapOp = new CostDistanceMapOp();

    String exp = "CostDistance([source], [friction], 10, 20, 15, 25)";
//    MapAlgebraParser map = new MapAlgebraParser();
    ParserNode node = parser.parse(exp, null);
    for (int i=0; i < node.getNumChildren(); i++)
    {
      children.add(node.getChild(i));
    }
    Vector<ParserNode> result = mapOp.processChildren(children, parser);
    Assert.assertEquals(2, result.size());
    Assert.assertEquals(-1, mapOp.zoomLevel);
    Assert.assertEquals(-1.0, mapOp.maxCost, EPSILON);
    Assert.assertNotNull(mapOp.bounds);
    Assert.assertEquals(10, mapOp.bounds.getMinX(), EPSILON);
    Assert.assertEquals(20, mapOp.bounds.getMinY(), EPSILON);
    Assert.assertEquals(15, mapOp.bounds.getMaxX(), EPSILON);
    Assert.assertEquals(25, mapOp.bounds.getMaxY(), EPSILON);
  }

  @Test
  @Category(UnitTest.class)
  public void testWithZoomAndBounds() throws ParserException
  {
    Vector<ParserNode> children = new Vector<ParserNode>();
    CostDistanceMapOp mapOp = new CostDistanceMapOp();

    String exp = "CostDistance([source], 10, [friction], 10, 20, 15, 25)";
//    MapAlgebraParser map = new MapAlgebraParser();
    ParserNode node = parser.parse(exp, null);
    for (int i=0; i < node.getNumChildren(); i++)
    {
      children.add(node.getChild(i));
    }
    Vector<ParserNode> result = mapOp.processChildren(children, parser);
    Assert.assertEquals(2, result.size());
    Assert.assertEquals(10, mapOp.zoomLevel);
    Assert.assertEquals(-1.0, mapOp.maxCost, EPSILON);
    Assert.assertNotNull(mapOp.bounds);
    Assert.assertEquals(10, mapOp.bounds.getMinX(), EPSILON);
    Assert.assertEquals(20, mapOp.bounds.getMinY(), EPSILON);
    Assert.assertEquals(15, mapOp.bounds.getMaxX(), EPSILON);
    Assert.assertEquals(25, mapOp.bounds.getMaxY(), EPSILON);
  }

  @Test
  @Category(UnitTest.class)
  public void testWithZoomAndMaxCostAndBounds() throws ParserException
  {
    Vector<ParserNode> children = new Vector<ParserNode>();
    CostDistanceMapOp mapOp = new CostDistanceMapOp();

    String exp = "CostDistance([source], 10, [friction], 50000.0, 10, 20, 15, 25)";
//    MapAlgebraParser map = new MapAlgebraParser();
    ParserNode node = parser.parse(exp, null);
    for (int i=0; i < node.getNumChildren(); i++)
    {
      children.add(node.getChild(i));
    }
    Vector<ParserNode> result = mapOp.processChildren(children, parser);
    Assert.assertEquals(2, result.size());
    Assert.assertEquals(10, mapOp.zoomLevel);
    Assert.assertEquals(50000.0, mapOp.maxCost, EPSILON);
    Assert.assertNotNull(mapOp.bounds);
    Assert.assertEquals(10, mapOp.bounds.getMinX(), EPSILON);
    Assert.assertEquals(20, mapOp.bounds.getMinY(), EPSILON);
    Assert.assertEquals(15, mapOp.bounds.getMaxX(), EPSILON);
    Assert.assertEquals(25, mapOp.bounds.getMaxY(), EPSILON);
  }

  @Test
  @Category(UnitTest.class)
  public void testWithIncompleteBounds() throws ParserException
  {
    Vector<ParserNode> children = new Vector<ParserNode>();
    CostDistanceMapOp mapOp = new CostDistanceMapOp();

    String exp = "CostDistance([source], 10, [friction], 10, 20)";
//    MapAlgebraParser map = new MapAlgebraParser();
    ParserNode node = parser.parse(exp, null);
    for (int i=0; i < node.getNumChildren(); i++)
    {
      children.add(node.getChild(i));
    }
    try
    {
      Vector<ParserNode> result = mapOp.processChildren(children, parser);
      Assert.fail("Expected IllegalArgumentException due to too many arguments");
    }
    catch(IllegalArgumentException e)
    {
      Assert.assertEquals("The bounds argument must include minX, minY, maxX and maxY", e.getMessage());
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testWithZoomAndMaxCostAndBoundsAndMore() throws ParserException
  {
    Vector<ParserNode> children = new Vector<ParserNode>();
    CostDistanceMapOp mapOp = new CostDistanceMapOp();

    String exp = "CostDistance([source], 10, [friction], 50000.0, 10, 20, 15, 25, \"extra\")";
//    MapAlgebraParser map = new MapAlgebraParser();
    ParserNode node = parser.parse(exp, null);
    for (int i=0; i < node.getNumChildren(); i++)
    {
      children.add(node.getChild(i));
    }
    try
    {
      Vector<ParserNode> result = mapOp.processChildren(children, parser);
      Assert.fail("Expected IllegalArgumentException due to too many arguments");
    }
    catch(IllegalArgumentException e)
    {
      Assert.assertEquals(usage, e.getMessage());
    }
  }
}
