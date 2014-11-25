/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.cmd;

//import javax.media.jai.JAI;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.util.ToolRunner;
//import org.mrgeo.opimage.OpImageRegistrar;
//import org.mrgeo.utils.HadoopUtils;
//import org.mrgeo.core.wps.RelevationProcesslet;

/**
 * 
 */
public class CalculateRelevation extends Configured implements Tool
{
  public static void main(String[] args) throws Exception
  {
    System.err.println("Not implemented");
    System.exit(-1);
//    OpImageRegistrar.registerMrGeoOps();
//
//    int res;
//    if (HadoopUtils.hasHadoop())
//    {
//      res = ToolRunner.run(HadoopUtils.createConfiguration(), new CalculateRelevation(), args);
//    }
//    else
//    {
//      CalculateRelevation mii = new CalculateRelevation();
//      res = mii.run(args);
//    }
//
//    System.exit(res);
  }

//  static int printUsage()
//  {
//    System.out.println("CalculateRelevation <elevation> <output> <sigma>");
//    System.out.println("");
//    ToolRunner.printGenericCommandUsage(System.out);
//    return -1;
//  }

  @Override
  public int run(String[] args) throws Exception
  {
    throw new Exception("Not implemented");
//    if (args.length != 3)
//    {
//      printUsage();
//      return -1;
//    }
//
//    String elevation = args[0];
//    String output = args[1];
//    double sigma = Double.valueOf(args[2]);
//
//    System.out.printf("Reading elevation from %s and exporting relevation to %s\n", elevation,
//        output);
//
//    // increase the tile cache size to speed things up
//    JAI.getDefaultInstance().getTileCache().setMemoryCapacity(512000000);
//
//    new RelevationProcesslet().run(new Path(elevation), new Path(output), sigma, null);
//
//    return 0;
  }

}
