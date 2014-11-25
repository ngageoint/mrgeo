/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.cmd;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.mrgeo.rasterops.OpImageRegistrar;
import org.mrgeo.utils.HadoopUtils;

/**
 * 
 * 
 */
public class CalculateCostSurface extends Configured implements Tool
{
  public static void main(String[] args) throws Exception
  {
    OpImageRegistrar.registerMrGeoOps();

    int res = ToolRunner.run(HadoopUtils.createConfiguration(), new CalculateCostSurface(), args);
    System.exit(res);
  }

  static int printUsage()
  {
    System.out.println("org.mrgeo.cmd.CalculateCostSurface <friction> <cost> [maxCost]");
    System.out.println(" * friction - the friction surface to use.");
    System.out.println(" * cost - the cost surface to use.");
    System.out.println(" * maxCost - the maximum cost to calculate to. This defaults to -1.");
    ToolRunner.printGenericCommandUsage(System.out);
    return -1;
  }

  /**
   * 
   */
  @Override
  public int run(String[] args) throws Exception
  {
    if (args.length < 3 || args.length > 4)
    {
      printUsage();
      return -1;
    }

//    String friction = args[0];
//    String cost = args[1];
//    double maxCost = -1;
//    if (args.length == 3)
//    {
//      maxCost = Double.valueOf(args[2]);
//    }
    
    printUsage();
    return -1;

//    System.out.println("Calculating cost surface");
//    System.out.printf(" * friction: %s\n", friction);
//    System.out.printf(" * cost: %s\n", cost);
//    System.out.printf(" * maxCost: %f\n", maxCost);
//
//    CostSurfaceCalculator.run(new Path(friction), new Path(cost), maxCost, null, null);
//
//    return 0;
  }

}
