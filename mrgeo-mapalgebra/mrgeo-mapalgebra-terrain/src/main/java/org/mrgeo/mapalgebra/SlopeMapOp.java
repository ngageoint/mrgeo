package org.mrgeo.mapalgebra;

import java.util.Vector;

import org.mrgeo.mapalgebra.parser.ParserAdapter;
import org.mrgeo.mapalgebra.parser.ParserNode;
import org.mrgeo.mapreduce.formats.TileClusterInfo;
import org.mrgeo.opimage.SlopeDescriptor;

public class SlopeMapOp extends RenderedImageMapOp implements TileClusterInfoCalculator
{
  public static String[] register()
  {
    return new String[] { "slope" };
  }

  public SlopeMapOp()
  {
    _factory = new SlopeDescriptor();
  }

  @Override
  public Vector<ParserNode> processChildren(final Vector<ParserNode> children, final ParserAdapter parser)
  {
    Vector<ParserNode> result = new Vector<ParserNode>();
    if (children.size() != 1)
    {
      throw new IllegalArgumentException("slope takes one argument - single-band raster elevation");
    }
    result.add(children.get(0));
    return result;
  }

  @Override
  public TileClusterInfo calculateTileClusterInfo()
  {
    // Slope uses HornNormalOpImage which needs access to the
    // eight pixels surrounding each pixel. This means we need
    // to get the eight surrounding tiles.
    return new TileClusterInfo(-1, -1, 3, 3, 1);
  }

  @Override
  public String toString()
  {
    return "slope()";
  }
}
