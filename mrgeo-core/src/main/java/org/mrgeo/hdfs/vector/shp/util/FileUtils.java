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

import org.apache.commons.io.FilenameUtils;

import java.io.*;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;

public class FileUtils extends Object
{
  private static NumberFormat formatter = null;
  public final static int KB = 1024;
  public final static int SOURCE_DOS = 1;
  public final static int SOURCE_UNIX = 0;

  static
  {
    formatter = new DecimalFormat("#,###,###");
  }
  
  /**
   * For a given shape file name returns a list of it along with all its auxiliary files
   * @param shapeFilePath path of the shape file to retrieve the files list for
   * @return an array of all files that belong to the shape file
   */
  public static String[] getShapeFilesList(String shapeFilePath)
  {
    if (!FilenameUtils.getExtension(shapeFilePath).equals("shp"))
    {
      throw new IllegalArgumentException("Input file must be a shape file.");
    }
    
    List<String> fileNames = new ArrayList<String>();
    fileNames.add(shapeFilePath);
    fileNames.add(shapeFilePath.replaceAll(".shp", ".dbf"));
    fileNames.add(shapeFilePath.replaceAll(".shp", ".prj"));
    fileNames.add(shapeFilePath.replaceAll(".shp", ".qpj"));
    fileNames.add(shapeFilePath.replaceAll(".shp", ".shx"));
    return fileNames.toArray(new String[]{});
  }

  public static void copyFile(String srcFileName, String dstFileName) throws Exception
  {
    // Copies src file to dst file.
    // If the dst file does not exist, it is created
    InputStream in = new FileInputStream(srcFileName);
    OutputStream out = new FileOutputStream(dstFileName);

    // Transfer bytes from in to out
    byte[] buf = new byte[1024];
    int len;
    while ((len = in.read(buf)) > 0)
    {
      out.write(buf, 0, len);
    }
    in.close();
    out.close();
  }

  /**
   * Copies a set of files with a common extension to another location with
   * optional name variant.
   * 
   * @param srcFileName
   * @param dstFileName
   * @throws Exception
   */
  public static void copyFiles(String srcFileName, String dstFileName, String prefix)
      throws Exception
  {
    copyFilesImpl(srcFileName, dstFileName, prefix, false);
  }

  private static void copyFilesImpl(String srcFileName, String dstFileName, String prefix,
      boolean move) throws Exception
  {
    File f = new File(srcFileName);
    if (!f.exists() || !f.isFile())
      throw new Exception("Invalid file! [" + srcFileName + "]");
    File dir = f.getParentFile();
    String name = f.getName();
    int pos = name.indexOf(".");
    if (pos == -1)
    {

    }
    name = name.substring(0, pos);
    FilenameFilter filter = new BasenameFilter(name);
    File[] child = dir.listFiles(filter);

    File fnew = new File(dstFileName);
    File dir2 = fnew.getParentFile();
    name = fnew.getName();
    pos = name.indexOf(".");
    String newBasename = null;
    if (pos == -1)
    {
      newBasename = name;
    }
    else
    {
      newBasename = name.substring(0, pos);
    }

    for (int i = 0; i < child.length; i++)
    {
      name = child[i].getName();
      pos = name.indexOf(".");
      if (pos == -1)
      {

      }
      String ext = name.substring(pos + 1);

      if (prefix != null)
        System.out.println(prefix + child[i].getCanonicalPath() + " -> " + dir2 + "\\"
            + newBasename + "." + ext);
      copyFile(child[i].getCanonicalPath(), dir2 + "\\" + newBasename + "." + ext);
      if (move)
        child[i].delete();
    }
  }

  public static File createTextFile(String fileName, String raw) throws Exception
  {
    if (raw == null)
      return null;
    BufferedWriter out = new BufferedWriter(new FileWriter(fileName));
    String line = raw;
    out.write(line);
    out.newLine();
    out.close();
    out = null;
    return new File(fileName);
  }

  public static String getLength(long length)
  {
    if (length > (KB * KB * KB))
    {
      BigDecimal bd = new BigDecimal(length / (KB * KB * KB));
      bd = bd.setScale(1, BigDecimal.ROUND_HALF_EVEN);
      return formatter.format(bd.doubleValue()) + "GB";
    }
    else if (length > (KB * KB))
    {
      BigDecimal bd = new BigDecimal(length / (KB * KB));
      bd = bd.setScale(1, BigDecimal.ROUND_HALF_EVEN);
      return formatter.format(bd.doubleValue()) + "MB";
    }
    else if (length > (KB))
    {
      BigDecimal bd = new BigDecimal(length / (KB));
      bd = bd.setScale(1, BigDecimal.ROUND_HALF_EVEN);
      return formatter.format(bd.doubleValue()) + "KB";
    }
    else
    {
      return formatter.format(length) + "B";
    }
  }

  public static void insertTextFile(String fileName, BufferedWriter out) throws Exception
  {
    try
    {
      BufferedReader in = new BufferedReader(new FileReader(fileName));
      String str;
      while ((str = in.readLine()) != null)
      {
        out.write(str);
        out.newLine();
      }
      in.close();
    }
    catch (FileNotFoundException e)
    {
      throw new Exception("Insert File [" + fileName + "] not found!");
    }
    catch (Exception e)
    {
      throw e;
    }
  }

  public static void moveFiles(String srcFileName, String dstFileName, String prefix)
      throws Exception
  {
    copyFilesImpl(srcFileName, dstFileName, prefix, true);
  }

  public static String readTextFile(String fileName) throws Exception
  {
    BufferedReader in = new BufferedReader(new FileReader(fileName));
    String output = "";
    String str = null;
    String sep = System.getProperty("line.separator");
    while ((str = in.readLine()) != null)
    {
      output += str + sep;
    }
    in.close();
    return output;
  }
}
