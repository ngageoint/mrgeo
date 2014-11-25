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
//import org.mrgeo.core.wps.DiffuseReflectionShadingProcesslet;

/**
 * 
 */
public class CalculateReliefShading extends Configured implements Tool
{
  public static void main(String[] args) throws Exception
  {
    OpImageRegistrar.registerMrGeoOps();

    int res = ToolRunner.run(HadoopUtils.createConfiguration(), new CalculateReliefShading(), args);
    System.exit(res);
  }

  static int printUsage()
  {
    System.out
        .println("CalculateReliefShading <elevation data> <output> <azimuth> <elevation angle>");
    System.out.println("");
    ToolRunner.printGenericCommandUsage(System.out);
    return -1;
  }

  @Override
  public int run(String[] args) throws Exception
  {
    System.err.println("CalculateReliefShading needs to be re-factored to use REST endpoint instead of WPS");
    return -1;
//    if (args.length != 4)
//    {
//      printUsage();
//      return -1;
//    }
//
//    String elevation = args[0];
//    String output = args[1];
//    double azimuth = Double.valueOf(args[2]) * Math.PI / 180.0;
//    double elevationAngle = Double.valueOf(args[3]) * Math.PI / 180.0;
//
//    System.out.printf("Reading elevation from %s and exporting relief shading to %s\n", elevation,
//        output);
//
//    new DiffuseReflectionShadingProcesslet().run(new Path(elevation), new Path(output), azimuth,
//        elevationAngle, null);
//
//    return 0;
  }

}
