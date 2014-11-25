/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.cmd;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.mrgeo.rasterops.OpImageRegistrar;
import org.mrgeo.utils.HadoopUtils;

//import org.apache.hadoop.fs.Path;
//import org.mrgeo.core.wps.TriProcesslet;

/**
 *
 */
public class CalculateTri extends Configured implements Tool
{
  public static void main(String[] args) throws Exception
  {
    OpImageRegistrar.registerMrGeoOps();

    int res = ToolRunner.run(HadoopUtils.createConfiguration(), new CalculateTri(), args);
    System.exit(res);
  }

  static int printUsage()
  {
    System.out.println("CalculateTri <elevation> <output>");
    System.out.println("");
    ToolRunner.printGenericCommandUsage(System.out);
    return -1;
  }

  @Override
  public int run(String[] args) throws Exception
  {
    System.err.println("CalculateTri needs to be re-factored to use a REST endpoint instead of WPS");
    return -1;
//    if (args.length != 2)
//    {
//      printUsage();
//      return -1;
//    }
//
//    String elevation = args[0];
//    String output = args[1];
//
//    System.out.printf("Reading elevation from %s and exporting TRI to %s\n", elevation, output);
//
//    new TriProcesslet().run(new Path(elevation), new Path(output), null);
//
//    return 0;
  }

}
