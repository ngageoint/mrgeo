/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.data.shp.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

public class FileDump extends java.lang.Object
{

  /**
   * @param args
   *          the command line arguments
   */
  @SuppressWarnings("unused")
  public static void main(String args[])
  {
    if (args.length < 1)
    {
      System.out.println("USAGE: FileDump <filename> [limit]");
      System.exit(0);
    }
    int limit = Integer.MAX_VALUE;
    if (args.length == 2)
    {
      try
      {
        limit = Integer.parseInt(args[1]);
      }
      catch (Exception e)
      {
        System.out.println("<<< Invalid limit!  HexDump defaulting to 1000 bytes... >>>");
        limit = 1000;
      }
    }
    new FileDump(args[0], limit);
  }

  /** Creates new FileDump */
  public FileDump(String filename, int limit)
  {
    FileInputStream fis = null;

    try
    {
      File f = new File(filename);
      int length = (int) f.length();
      if (length > limit)
        length = limit; // limit results
      fis = new FileInputStream(f);
      byte[] data = new byte[length];
      int read = fis.read(data, 0, length);
      if (read != length)
        throw new IOException();
      fis.close();
      System.out.println("<<< HexDump for " + filename + " >>>\n");
      System.out.println(HexDump.hexDump(data));
      if (length == limit)
        System.out.println("\n<<< HexDump limited to " + limit + " bytes... >>>");
    }
    catch (FileNotFoundException e)
    {
      System.out.println("<<< File not found! >>>");
    }
    catch (IOException e)
    {
      System.out.println("<<< Error reading file! >>>");
    }
    finally
    {
      try
      {
        if (fis != null)
        {
          fis.close();
        }
      }
      catch (Exception e)
      {
      }
      fis = null;
    }
  }
}
