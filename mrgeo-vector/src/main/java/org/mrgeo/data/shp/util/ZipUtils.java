/*
 * Copyright 2009-2014 DigitalGlobe, Inc.
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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.Vector;
import java.util.zip.*;

@SuppressWarnings("unchecked")
public class ZipUtils
{
  public static void createZipFile(File zipFile, String baseDir, String[] contents)
      throws Exception
      {
    // check
    String separator = System.getProperty("file.separator");
    if (zipFile == null)
      throw new Exception("ZipFile Is Null!  Cannot Create File!");
    ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(zipFile));
    try
    {
      if (baseDir == null)
      {
        baseDir = "";
      }
      else
      {
        if (!baseDir.endsWith(separator))
          baseDir += separator;
      }
      for (int i = 0; i < contents.length; i++)
      {
        // check
        if (contents[i] == null)
          throw new Exception("Null Contents Entry At " + i + "!  Stopping!");
        // create an entry
        ZipEntry ze = new ZipEntry(contents[i]);
        zos.setMethod(ZipOutputStream.DEFLATED); // indicate deflated
        zos.setLevel(Deflater.DEFAULT_COMPRESSION); // use default level
        // add to the zip
        zos.putNextEntry(ze);
        // loop to read the file and process to the zip
        String tempFileName = null;
        if (contents[i].startsWith(separator))
        {
          tempFileName = baseDir + contents[i].substring(1);
        }
        else
        {
          tempFileName = baseDir + contents[i];
        }
        File fin = new File(tempFileName);
        FileInputStream ins = new FileInputStream(fin);
        int bread;
        byte[] bin = new byte[4096];
        while ((bread = ins.read(bin, 0, 4096)) != -1)
        {
          zos.write(bin, 0, bread);
        }

        ins.close();

        zos.closeEntry();

      }
    }
    finally
    {
      zos.close();
    }
      }

  public static void createZipFile(File zipFile, String[] contents) throws Exception
  {
    createZipFile(zipFile, null, contents);
  }

  public static void createZipFile(String zipFileName, String baseDir, String[] contents)
      throws Exception
      {
    File zipFile = new File(zipFileName);
    createZipFile(zipFile, baseDir, contents);
      }

  public static void createZipFile(String zipFileName, String[] contents) throws Exception
  {
    File zipFile = new File(zipFileName);
    createZipFile(zipFile, null, contents);
  }

  public static void extractFromZipFile(File zipFile, String fileName, String baseDir)
      throws Exception
      {
    // check
    String separator = System.getProperty("file.separator");
    if (zipFile == null)
      throw new Exception("ZipFile Is Null!  Cannot Extract File!");
    if (fileName == null)
      throw new Exception("FileName Is Null!  Cannot Extract Unknown Zip File!");
    if (baseDir == null)
    {
      baseDir = "";
    }
    else
    {
      if (!baseDir.endsWith(separator))
        baseDir += separator;
    }
    // get zip object
    ZipFile zf = new ZipFile(zipFile);
    // get zip entry
    ZipEntry ze = zf.getEntry(fileName);
    // get an input stream for the entry
    if (ze == null)
      throw new Exception("FileName Not Found In ZipFile!  Cannot Extract File!");
    InputStream ins = zf.getInputStream(ze);
    // create output file stream
    String tempFileName = null;
    if (fileName.startsWith(separator))
    {
      tempFileName = baseDir + fileName.substring(1);
    }
    else
    {
      tempFileName = baseDir + fileName;
    }
    File tempFile = new File(tempFileName);
    File tempDir = tempFile.getParentFile();
    if (!tempDir.exists())
      tempDir.mkdir();
    FileOutputStream fos = new FileOutputStream(tempFile);
    int bread;
    // transfer buffer
    byte[] bin = new byte[4096];
    // loop through reading the zipped file entry and write it to the external
    // file
    while ((bread = ins.read(bin, 0, 4096)) > -1)
    {
      fos.write(bin, 0, bread);
    }
    fos.close();
    ins.close();
      }

  public static void extractFromZipFile(String zipFileName, String fileName, String baseDir)
      throws Exception
      {
    File zipFile = new File(zipFileName);
    extractFromZipFile(zipFile, fileName, baseDir);
      }

  public static void extractZipFile(File zipFile) throws Exception
  {
    extractZipFile(zipFile, null);
  }

  public static void extractZipFile(File zipFile, String baseDir) throws Exception
  {
    // check
    String separator = System.getProperty("file.separator");
    if (zipFile == null)
      throw new Exception("ZipFile Is Null!  Cannot Extract Zip File!");
    if (baseDir == null)
    {
      baseDir = "";
    }
    else
    {
      if (!baseDir.endsWith(separator))
        baseDir += separator;
    }
    // get zip object as stream
    ZipInputStream zis = new ZipInputStream(new FileInputStream(zipFile));
    ZipEntry ze;
    // process sequentially
    while ((ze = zis.getNextEntry()) != null)
    {
      // create output file stream
      String tempFileName = null;
      if (ze.getName().startsWith(separator))
      {
        tempFileName = baseDir + ze.getName().substring(1);
      }
      else
      {
        tempFileName = baseDir + ze.getName();
      }
      File tempFile = new File(tempFileName);
      File tempDir = tempFile.getParentFile();
      if (!tempDir.exists())
        tempDir.mkdir();
      FileOutputStream fos = new FileOutputStream(tempFile);
      int bread;
      // transfer buffer
      byte[] bin = new byte[4096];
      // loop through reading the zipped file entry and write it to the external
      // file
      while ((bread = zis.read(bin, 0, 4096)) > -1)
      {
        fos.write(bin, 0, bread);
      }
      fos.close();
    }
    zis.close();
  }

  public static void extractZipFile(String zipFileName) throws Exception
  {
    File zipFile = new File(zipFileName);
    extractZipFile(zipFile, null);
  }

  public static void extractZipFile(String zipFileName, String baseDir) throws Exception
  {
    File zipFile = new File(zipFileName);
    extractZipFile(zipFile, baseDir);
  }

  @SuppressWarnings("rawtypes")
  public static String[] getZipFileContents(File zipFile) throws Exception
  {
    // check
    if (zipFile == null)
      throw new Exception("ZipFile Is Null!  Cannot Extract File Contents!");
    // create a zipfile object
    ZipFile zf = new ZipFile(zipFile);
    // get enumeration of all entries
    Enumeration zipEnum = zf.entries();
    Vector v = new Vector();
    // loop through all entries
    while (zipEnum.hasMoreElements())
    {
      ZipEntry ze = (ZipEntry) zipEnum.nextElement(); // get next entry
      v.add(ze.getName());
    }
    zf.close();
    // change vector to String[] array
    String[] names = new String[v.size()];
    for (int i = 0; i < v.size(); i++)
      names[i] = (String) v.get(i);
    // return String[] array
    return names;
  }

  public static String[] getZipFileContents(String zipFileName) throws Exception
  {
    File zipFile = new File(zipFileName);
    return getZipFileContents(zipFile);
  }
}
