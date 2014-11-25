/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.cmd;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.mrgeo.mapreduce.ColumnJoin;
import org.mrgeo.rasterops.OpImageRegistrar;
import org.mrgeo.utils.HadoopUtils;

import java.util.Vector;

/**
 * 
 */
public class JoinOnColumn extends Configured implements Tool
{
  public static void main(String[] args) throws Exception
  {
    OpImageRegistrar.registerMrGeoOps();

    int res = ToolRunner.run(HadoopUtils.createConfiguration(), new JoinOnColumn(), args);

    System.exit(res);
  }

  static int printUsage()
  {
    System.out.println("JoinOnColumn <output> <column> <inputs ...>");
    System.out.println("");
    ToolRunner.printGenericCommandUsage(System.out);
    return -1;
  }

  @Override
  public int run(String[] args) throws Exception
  {
    if (args.length < 3)
    {
      printUsage();
      return -1;
    }

    String output = args[0];
    String column = args[1];
    Vector<Path> inputs = new Vector<Path>();
    for (int i = 2; i < args.length; i++)
    {
      inputs.add(new Path(args[i]));
    }

    ColumnJoin.run(new Path(output), inputs, column, 1, getConf(), null, null);

    return 0;
  }
}
