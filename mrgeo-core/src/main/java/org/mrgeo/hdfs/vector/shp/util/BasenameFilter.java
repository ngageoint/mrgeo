/*
 * Copyright 2009-2016 DigitalGlobe, Inc.
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
 *
 */

package org.mrgeo.hdfs.vector.shp.util;

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
