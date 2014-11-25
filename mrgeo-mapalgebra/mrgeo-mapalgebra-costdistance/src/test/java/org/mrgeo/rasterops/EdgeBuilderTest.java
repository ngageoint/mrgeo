package org.mrgeo.rasterops;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.rasterops.CostDistanceVertex.Position;
import org.mrgeo.rasterops.EdgeBuilder.PositionEdge;
import org.mrgeo.junit.UnitTest;

import java.util.List;

public class EdgeBuilderTest
{
  /* This test uses friction_allones so below metadata are based off of it */
  private static final long minTileId = 66;
  private static final long maxTileId = 117;
  private static final int zoomlevel = 4;

  EdgeBuilder builder;

  public static void assertEdgeEquals(final long[] expectedNeighbors,
    final Position[] expectedPositions, final List<PositionEdge> actualNeighbors)
  {
    Assert.assertEquals("expectedNeighbors.length == expectedPositions.length",
      expectedNeighbors.length, expectedPositions.length);
    for (int i = 0; i < expectedNeighbors.length; i++)
    {
      boolean found = false;
      for (final PositionEdge actualNeighbor : actualNeighbors)
      {
        if (actualNeighbor.targetVertexId == expectedNeighbors[i] &&
          actualNeighbor.position == expectedPositions[i])
        {
          found = true;
        }
      }
      if (!found)
      {
        Assert.assertEquals(String.format("Couldn't find %d in position %s", expectedNeighbors[i],
          expectedPositions[i]), false, true);
      }
    }
  }

  @Test
  @Category(UnitTest.class)
  public void getEdges()
  {
    List<PositionEdge> neighbors;

    // cluster of 4 x 4 tiles, here we check some of them

    // bottom row
    neighbors = builder.getNeighbors(66);
    assertEdgeEquals(new long[] { 67, 82, 83 }, new Position[] { Position.RIGHT, Position.TOP,
      Position.TOPRIGHT }, neighbors);
    neighbors = builder.getNeighbors(67);
    assertEdgeEquals(new long[] { 66, 68, 82, 83, 84 }, new Position[] { Position.LEFT,
      Position.RIGHT, Position.TOPLEFT, Position.TOP, Position.TOPRIGHT }, neighbors);
    neighbors = builder.getNeighbors(68);
    assertEdgeEquals(new long[] { 67, 69, 83, 84, 85 }, new Position[] { Position.LEFT,
      Position.RIGHT, Position.TOPLEFT, Position.TOP, Position.TOPRIGHT }, neighbors);
    neighbors = builder.getNeighbors(69);
    assertEdgeEquals(new long[] { 68, 84, 85 }, new Position[] { Position.LEFT, Position.TOPLEFT,
      Position.TOP }, neighbors);

    // second from bottom row
    neighbors = builder.getNeighbors(82);

    assertEdgeEquals(new long[] { 66, 67, 83, 98, 99 }, new Position[] { Position.BOTTOM,
      Position.BOTTOMRIGHT, Position.RIGHT, Position.TOP, Position.TOPRIGHT }, neighbors);
    neighbors = builder.getNeighbors(83);
    assertEdgeEquals(new long[] { 66, 67, 68, 82, 84, 98, 99, 100 }, new Position[] {
      Position.BOTTOMLEFT, Position.BOTTOM, Position.BOTTOMRIGHT, Position.LEFT, Position.RIGHT,
      Position.TOPLEFT, Position.TOP, Position.TOPRIGHT }, neighbors);
    neighbors = builder.getNeighbors(84);
    assertEdgeEquals(new long[] { 67, 68, 69, 83, 85, 99, 100, 101 }, new Position[] {
      Position.BOTTOMLEFT, Position.BOTTOM, Position.BOTTOMRIGHT, Position.LEFT, Position.RIGHT,
      Position.TOPLEFT, Position.TOP, Position.TOPRIGHT }, neighbors);
    neighbors = builder.getNeighbors(85);
    assertEdgeEquals(new long[] { 68, 69, 84, 100, 101 }, new Position[] { Position.BOTTOMLEFT,
      Position.BOTTOM, Position.LEFT, Position.TOPLEFT, Position.TOP }, neighbors);

    // third from bottom row
    neighbors = builder.getNeighbors(98);
    assertEdgeEquals(new long[] { 82, 83, 99, 114, 115 }, new Position[] { Position.BOTTOM,
      Position.BOTTOMRIGHT, Position.RIGHT, Position.TOP, Position.TOPRIGHT }, neighbors);
    neighbors = builder.getNeighbors(99);
    assertEdgeEquals(new long[] { 82, 83, 84, 98, 100, 114, 115, 116 }, new Position[] {
      Position.BOTTOMLEFT, Position.BOTTOM, Position.BOTTOMRIGHT, Position.LEFT, Position.RIGHT,
      Position.TOPLEFT, Position.TOP, Position.TOPRIGHT }, neighbors);
    neighbors = builder.getNeighbors(100);
    assertEdgeEquals(new long[] { 83, 84, 85, 99, 101, 115, 116, 117 }, new Position[] {
      Position.BOTTOMLEFT, Position.BOTTOM, Position.BOTTOMRIGHT, Position.LEFT, Position.RIGHT,
      Position.TOPLEFT, Position.TOP, Position.TOPRIGHT }, neighbors);
    neighbors = builder.getNeighbors(101);
    assertEdgeEquals(new long[] { 84, 85, 100, 116, 117 }, new Position[] { Position.BOTTOMLEFT,
      Position.BOTTOM, Position.LEFT, Position.TOPLEFT, Position.TOP }, neighbors);

    // top row
    neighbors = builder.getNeighbors(114);
    assertEdgeEquals(new long[] { 98, 99, 115 }, new Position[] { Position.BOTTOM,
      Position.BOTTOMRIGHT, Position.RIGHT }, neighbors);
    neighbors = builder.getNeighbors(115);
    assertEdgeEquals(new long[] { 98, 99, 100, 114, 116 }, new Position[] { Position.BOTTOMLEFT,
      Position.BOTTOM, Position.BOTTOMRIGHT, Position.LEFT, Position.RIGHT }, neighbors);
    neighbors = builder.getNeighbors(116);
    assertEdgeEquals(new long[] { 99, 100, 101, 115, 117 }, new Position[] { Position.BOTTOMLEFT,
      Position.BOTTOM, Position.BOTTOMRIGHT, Position.LEFT, Position.RIGHT }, neighbors);
    neighbors = builder.getNeighbors(117);
    assertEdgeEquals(new long[] { 100, 101, 116 }, new Position[] { Position.BOTTOMLEFT,
      Position.BOTTOM, Position.LEFT }, neighbors);
  }

  @Before
  public void setUp()
  {
    builder = new EdgeBuilder(minTileId, maxTileId, zoomlevel);
  }

}
