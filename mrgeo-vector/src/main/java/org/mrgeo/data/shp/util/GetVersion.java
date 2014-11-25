/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.data.shp.util;

public class GetVersion
{
  private static final String DATE = "$Date: 2005-09-15 23:10:16 -0400 (Thu, 15 Sep 2005) $";
  private static final String VERSION = "$Revision: 530 $";

  private static String getDate()
  {
    return DATE.substring("$Date: ".length(), DATE.length() - 2);
  }

  public static String getInfo()
  {
    return "Common Build: " + getVersion() + " (" + getDate() + ")";
  }

  private static String getVersion()
  {
    String temp = VERSION.substring("$Revision: ".length(), VERSION.length() - 2);
    return temp;
    // require STRING return, so that 1.10 or 1.20 don't come back as 1.1 or 1.2
    // when returned as DOUBLEs.
  }

  public static void main(String[] args)
  {
    System.out.println("");
    System.out.println(getInfo());
    System.out.println("");
    System.exit(0);
  }
}
