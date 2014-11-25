package org.mrgeo.mapalgebra;

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.fs.Path;
import org.mrgeo.mapalgebra.parser.ParserAdapter;
import org.mrgeo.mapalgebra.parser.ParserNode;
import org.mrgeo.mapreduce.PgQueryDriver;
import org.mrgeo.mapreduce.job.JobCancelledException;
import org.mrgeo.mapreduce.job.JobFailedException;
import org.mrgeo.progress.Progress;

public class PgQueryMapOp extends VectorMapOp
{
  public static String USERNAME = "username";
  public static String PASSWORD = "password";
  public static String DBCONNECTION = "dbconnection";

  String _column = null;
  double _defaultValue = 0.0;
  String _username = null;
  String _password = null;
  String _dbconnection = null;

  public static String[] register()
  {
    return new String[] { "pgQuery" };
  }

  @Override
  public void addInput(MapOp n) throws IllegalArgumentException
  {
    if (!(n instanceof VectorMapOp))
    {
      throw new IllegalArgumentException("Only vector inputs are supported.");
    }
    if (_inputs.size() != 0)
    {
      throw new IllegalArgumentException("Only one input is supported.");
    }
    _inputs.add(n);
  }

  @Override
  public void build(Progress p) throws IOException, JobFailedException, JobCancelledException
  {
    if (p != null)
    {
      p.starting();
    }

    MapOp mo = _inputs.get(0);
    String inputPath = null;
    if (mo instanceof VectorReaderMapOp)
    {
      VectorReaderMapOp vmo = (VectorReaderMapOp)mo;
      inputPath = vmo.getOutputName();
    }

    PgQueryDriver pgd = new PgQueryDriver();
    pgd.setUsername(_username);
    pgd.setPassword(_password);
    pgd.setDbConnection(_dbconnection);
    pgd.run(new Path(inputPath), new Path(_outputName), p, jobListener);
    _output = new BasicInputFormatDescriptor(_outputName);

    if (p != null)
    {
      p.complete();
    }
  }


  @Override
  public Vector<ParserNode> processChildren(final Vector<ParserNode> children, final ParserAdapter parser)
  {
    Vector<ParserNode> result = new Vector<ParserNode>();

    if (children.size() != 4)
    {
      throw new IllegalArgumentException(
          "PgQuery takes four arguments. (sql file, username, password and dbconnection)");
    }

    result.add(children.get(0));

    _username = parseChildString(children.get(1), "username", parser);
    _password = parseChildString(children.get(2), "password", parser);
    _dbconnection = parseChildString(children.get(3), "dbconnection", parser);

    return result;
  }

  @Override
  public String toString()
  {
    return String.format("PgQueryMapOp %s",
        _outputName == null ? "null" : _outputName.toString() );
  }
}
