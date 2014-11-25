package org.mrgeo.mapalgebra;

import java.util.Vector;

import org.mrgeo.mapalgebra.parser.ParserAdapter;
import org.mrgeo.mapalgebra.parser.ParserNode;
import org.mrgeo.progress.Progress;

public class InlineCsvMapOp extends VectorMapOp
{
  private String _columns, _values;

  @Override
  public void addInput(MapOp n) throws IllegalArgumentException
  {
    throw new IllegalArgumentException("This ExecuteNode takes no arguments.");
  }

  @Override
  public void build(Progress p)
  {
    if (p != null)
    {
      p.complete();
    }
    _output = new InlineCsvInputFormatDescriptor(_columns, _values);
  }

  @Override
  public Vector<ParserNode> processChildren(final Vector<ParserNode> children, final ParserAdapter parser)
  {
    Vector<ParserNode> result = new Vector<ParserNode>();

    if (children.size() != 2)
    {
      throw new IllegalArgumentException(
          "Inline CSV takes two arguments. (columns and values)");
    }

    _columns = parseChildString(children.get(0), "columns", parser);
    _values = parseChildString(children.get(1), "values", parser);
    
    return result;
  }
  
  @Override
  public String toString()
  {
    return String.format("InlineCsvMapOp %s",
       _outputName == null ? "null" : _outputName.toString() );
  }

}
