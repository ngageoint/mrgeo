/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */

package org;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.mrgeo.geometry.PointFilter;
import org.mrgeo.geometry.WellKnownProjections;
import org.mrgeo.geometryfilter.Reprojector;
import org.mrgeo.mapreduce.FilterGeometryTool;
import org.mrgeo.data.shp.ShapefileReader;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.utils.HadoopUtils;

/**
 * 
 */
public class ReprojectVectorData extends Configured implements Tool
{

  public static void main(String[] args) throws Exception
  {
    int res = ToolRunner.run(HadoopUtils.createConfiguration(), new ReprojectVectorData(), args);
    System.exit(res);
  }

  @Override
  public int run(String[] args) throws Exception
  {
    if (args.length != 2)
    {
      printUsage();
      return 1;
    }
    Path inputPath = new Path(args[0]);
    Path outputPath = new Path(args[1]);

    FileSystem fs = HadoopFileUtils.getFileSystem(getConf(), outputPath);
    fs.delete(outputPath, true);

    ShapefileReader gc = new ShapefileReader(inputPath);

    PointFilter filter = Reprojector.createFromWkt(gc.getProjection(), WellKnownProjections.WGS84);

    FilterGeometryTool.run(gc, filter, outputPath, getConf(), null, null);

    gc.close();
    
    return 0;
  }

  static void printUsage()
  {
    System.out.println("ReprojectVectorData <input> <output>");
    System.out.println("\nReprojects data from the existing projection to WGS84.");
    System.out.println("\nOptions:");
    System.out.println(" * input - Input shapefile on HDFS.");
    System.out.println(" * output - Output directory on HDFS -- outputs in WKT, kinda.");
    System.out.println("");

    ToolRunner.printGenericCommandUsage(System.out);
  }
}
