/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.data.shp.util;

public class RegExTest
{

  /**
   * @param args
   */
  public static void main(String[] args)
  {
    if (args.length != 3)
    {
      System.out.println("USAGE: RegExTest <string> <regex> <replacement>");
      System.exit(1);
    }
    try
    {
      String output = args[0].replaceAll(args[1], args[2]);
      System.out.println("<" + args[0] + "," + args[1] + "," + args[2]);
      System.out.println(">" + output);
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }

}
