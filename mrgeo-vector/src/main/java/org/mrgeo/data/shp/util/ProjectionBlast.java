/*
 * Copyright 2009-2015 DigitalGlobe, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
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
