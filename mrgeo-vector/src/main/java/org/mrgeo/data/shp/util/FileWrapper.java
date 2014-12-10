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

import java.io.*;

public class FileWrapper implements Serializable
{
  private static final long serialVersionUID = 1L;

  public static void main(String[] args)
  {
    try
    {
      if (args.length == 0)
      {
        System.out.println("USAGE: FileWrapper <file>");
        System.out.println("Tests the FileWrapper class.");
        System.exit(1);
      }
      FileWrapper w = new FileWrapper(args[0]);
      System.out.println(w.toString());
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }

  private byte[] data; // file binary data array
  private File file; // file
  private String full; // file's full name/path
  private int length; // file length
  private String name; // file's name

  private String path; // file's path

  // constructors
  public FileWrapper()
  {
    reset();
  }

  public FileWrapper(File f) throws IOException
  {
    // load a file
    load(f);
  }

  public FileWrapper(String fileName) throws IOException
  {
    // load a file
    File f = new File(fileName);
    load(f);
  }

  public byte[] getByteArray()
  {
    return data;
  }

  public String getFull()
  {
    return full;
  }

  public int getLength()
  {
    return length;
  }

  public String getName()
  {
    return name;
  }

  public String getPath()
  {
    return path;
  }

  public String getSizeLabel()
  {
    return FileUtils.getLength(length);
  }

  public File getSourceFile()
  {
    return file;
  }

  // load (read) methods
  public int load(File f) throws IOException
  {
    // check
    if (f == null)
      throw new IOException("File Parameter Is Null!  Cannot Load!");

    // reset
    reset();

    // some convenience
    file = f;
    path = f.getPath();
    name = f.getName();
    full = f.toString();

    // open file stream
    FileInputStream fis = new FileInputStream(f);

    // read the file and allocate a byte array the size of the file.
    length = (int) f.length();
    data = new byte[length];

    // read the file into the byte array.
    int numread = fis.read(data, 0, length);

    // close
    fis.close();

    // some validation
    if (length != numread)
      throw new IOException("Integrity Check Not Passed! (Inconsistent Byte Lengths)");

    // return
    return this.length;
  }

  public int load(String fileName) throws IOException
  {
    File f = new File(fileName);
    load(f);
    return length;
  }

  // free resources
  private void reset()
  {
    file = null;
    name = null;
    path = null;
    full = null;
    length = 0;
    data = null;
  }

  public File save() throws IOException
  {
    // saves with original name
    if (name == null)
      throw new IOException("Name (Original) Is Null!  Cannot Save!");
    File f = new File(name);
    save(f, true);
    return f;
  }

  public File save(boolean release) throws IOException
  {
    // saves with original name
    if (name == null)
      throw new IOException("Name (Original) Is Null!  Cannot Save!");
    File f = new File(name);
    save(f, release);
    return f;
  }

  public void save(File f) throws IOException
  {
    if (f == null)
      throw new IOException("File Is Null!  Cannot Save!");
    save(f, true);
  }

  // save (write) methods
  public void save(File f, boolean release) throws IOException
  {
    // check
    if (f == null)
      throw new IOException("File Parameter Is Null!  Cannot Save!");
    if (f.exists())
      f.delete();

    // open file stream
    FileOutputStream fos = new FileOutputStream(f);

    // write the file
    fos.write(data);

    // close
    fos.close();

    // release (memory, etc...)
    // this offers better performance if you need to free resources sooner than
    // later
    if (release == true)
      reset();
  }

  public void save(String fileName) throws IOException
  {
    if (fileName == null)
      throw new IOException("Name Is Null!  Cannot Save!");
    File f = new File(fileName);
    save(f, true);
  }

  public void save(String fileName, boolean release) throws IOException
  {
    if (fileName == null)
      throw new IOException("Name Is Null!  Cannot Save!");
    File f = new File(fileName);
    save(f, release);
  }

  @Override
  public String toString()
  {
    if (file != null)
    {
      return full + " (" + getSizeLabel() + ")";
    }
    return null;
  }
}
