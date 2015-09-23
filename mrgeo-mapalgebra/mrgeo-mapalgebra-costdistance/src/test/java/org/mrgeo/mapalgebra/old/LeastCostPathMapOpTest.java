package org.mrgeo.mapalgebra.old;

import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.mapalgebra.parser.ParserException;
import org.mrgeo.mapalgebra.parser.ParserNode;
import org.mrgeo.test.LocalRunnerTest;

import java.util.Vector;

/**
 * Contains fast-running tests for LeastCostPath map algebra function.
 * Tests that need to actually execute the least cost path functionality
 * should be placed in LeastCostPathMapOpIntegrationTest, not here.
 */
public class LeastCostPathMapOpTest extends LocalRunnerTest
{
  private static final String usage =
      "LeastCostPath takes the following arguments ([cost zoom level], cost raster, destination points";

  private ParserAdapterHadoop parser;

//  public class TestMapOpFactory implements MapOpFactory
//  {
//
//    @Override
//    public ArrayList<String> getMapOpNames()
//    {
//      return null;
//    }
//
//    @Override
//    public MapOp convertToMapOp(ParserFunctionNode node) throws ParserException
//    {
//      return null;
//    }
//
//    @Override
//    public MapOp convertToMapOp(ParserNode node) throws ParserException
//    {
//      return null;
//    }
//
//    @Override
//    public void setRootFactory(MapOpFactory rootFactory)
//    {
//    }
//  }

  @Before
  public void setup()
  {
    parser = ParserAdapterFactoryHadoop.createParserAdapter();
    parser.initialize();
    parser.addFunction("LeastCostPath");
    parser.initializeForTesting();
  }

  @Test
  @Category(UnitTest.class)
  public void testNoArgs() throws ParserException
  {
    Vector<ParserNode> children = new Vector<ParserNode>();
    LeastCostPathMapOp mapOp = new LeastCostPathMapOp();

    String exp = "LeastCostPath()";
//    MapAlgebraParser map = new MapAlgebraParser();
    try
    {
      ParserNode node = parser.parse(exp, null);
      for (int i=0; i < node.getNumChildren(); i++)
      {
        children.add(node.getChild(i));
      }
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
    LeastCostPathMapOp mapOp = new LeastCostPathMapOp();

    String exp = "LeastCostPath([source])";
//    MapAlgebraParser map = new MapAlgebraParser();
    try
    {
      ParserNode node = parser.parse(exp, null);
      for (int i=0; i < node.getNumChildren(); i++)
      {
        children.add(node.getChild(i));
      }
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
    LeastCostPathMapOp mapOp = new LeastCostPathMapOp();

    String exp = "LeastCostPath([source], [friction])";
//    TestMapOpFactory factory = new TestMapOpFactory();
//    MapAlgebraParser map = new MapAlgebraParser();
    ParserNode node = parser.parse(exp, null);
    for (int i=0; i < node.getNumChildren(); i++)
    {
      children.add(node.getChild(i));
    }
    Vector<ParserNode> result = mapOp.processChildren(children, parser);
    Assert.assertEquals(2, result.size());
    Assert.assertEquals(-1, mapOp.zoom);
  }

  @Test
  @Category(UnitTest.class)
  public void testWithOptionalZoom() throws ParserException
  {
    Vector<ParserNode> children = new Vector<ParserNode>();
    LeastCostPathMapOp mapOp = new LeastCostPathMapOp();

    String exp = "LeastCostPath(10, [source], [friction])";
//    MapAlgebraParser map = new MapAlgebraParser();
    ParserNode node = parser.parse(exp, null);
    for (int i=0; i < node.getNumChildren(); i++)
    {
      children.add(node.getChild(i));
    }
    Vector<ParserNode> result = mapOp.processChildren(children, parser);
    Assert.assertEquals(2, result.size());
    Assert.assertEquals(10, mapOp.zoom);
  }

  @Test
  @Category(UnitTest.class)
  public void testWithInvalidZoom() throws ParserException
  {
    Vector<ParserNode> children = new Vector<ParserNode>();
    LeastCostPathMapOp mapOp = new LeastCostPathMapOp();

    String exp = "LeastCostPath(\"abc\", [source], [friction])";
//    MapAlgebraParser map = new MapAlgebraParser();
    try
    {
      ParserNode node = parser.parse(exp, null);
      for (int i=0; i < node.getNumChildren(); i++)
      {
        children.add(node.getChild(i));
      }
      mapOp.processChildren(children, parser);
      Assert.fail("Expected IllegalArgumentException for invalid zoom level");
    }
    catch(IllegalArgumentException e)
    {
      Assert.assertNotNull(e.getMessage());
      Assert.assertTrue(e.getMessage().startsWith("You must specify a valid number for cost zoom."));
    }
  }
}
