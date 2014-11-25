/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.data.shp.util;

import java.io.File;
import java.io.FilenameFilter;

public class BasenameFilter implements FilenameFilter
{
  private String baseFileName = null;

  public BasenameFilter(String baseFileName)
  {
    this.baseFileName = baseFileName;
  }

  @Override
  public boolean accept(File dir, String name)
  {
    File f = new File(dir + System.getProperty("file.separator") + name);
    if (!f.exists() || !f.isFile())
      return false;
    if (baseFileName == null)
      return true;
    int pos = name.indexOf(".");
    if (pos == -1)
      return false;
    name = name.substring(0, pos);
    if (name.equalsIgnoreCase(baseFileName))
      return true;
    return false;
  }
}
