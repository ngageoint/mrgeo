/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.data.shp.util;

import java.io.*;
import java.nio.channels.FileChannel;

public class ProjectionBlast
{

  public static void main(String[] args)
  {
    try
    {
      // check config
      if (args.length == 0)
      {
        System.out.println("USAGE: ProjectionBlast <config>");
        System.exit(1);
      }

      // load config
      ConfigurationFile cf = new ConfigurationFile(args[0]);

      String dir = cf.getProperty("projectionblast.dir");
      String prj = cf.getProperty("projectionblast.prj");

      // go
      run(dir, prj);

      System.exit(0);
    }
    catch (Exception e)
    {
      e.printStackTrace();
      System.exit(1);
    }
  }

  public static void run(String dir, String prj) throws IOException
  {
    File directory = new File(dir);
    if (!directory.exists())
    {
      System.out.println("The directory does not exist!");
      System.exit(1);
    }

    File projection = new File(prj);
    if (!projection.exists())
    {
      System.out.println("The projection file does not exist!");
      System.exit(1);
    }

    // It is also possible to filter the list of returned files.
    // This example does not return any files that start with `.'.
    FilenameFilter filter = new FilenameFilter()
    {
      @Override
      public boolean accept(File f, String name)
      {
        return name.endsWith(".shp");
      }
    };

    String[] children = directory.list(filter);
    if (children == null)
    {
      System.out.println("There are no shapefiles to process!");
    }
    else
    {
      for (int i = 0; i < children.length; i++)
      {
        String filename = children[i];
        System.out.println(filename);
        // Create channel on the source
        FileInputStream fis = new FileInputStream(prj);
        FileChannel srcChannel = fis.getChannel();

        // Create channel on the destination
        FileOutputStream fos = new FileOutputStream(dir.toString() + File.separator
            + filename.substring(0, filename.length() - 4) + ".prj");
        FileChannel dstChannel = fos.getChannel();

        // Copy file contents from source to destination
        dstChannel.transferFrom(srcChannel, 0, srcChannel.size());

        // Close the channels
        srcChannel.close();
        dstChannel.close();
        
        // close the streams
        fis.close();
        fos.close();
      }
    }
  }
}
