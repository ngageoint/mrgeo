/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.data.shp.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.Vector;

@SuppressWarnings("unchecked")
public class DelimitedFile
{
  @SuppressWarnings("rawtypes")
  private Vector data = null;
  private String delimeter = null;
  @SuppressWarnings("rawtypes")
  private HashMap fields = null;
  private String file = null;
  private boolean header;

  public DelimitedFile(String file) throws IOException
  {
    this(file, ",", true);
  }

  public DelimitedFile(String file, boolean header) throws IOException
  {
    this(file, ",", header);
  }

  public DelimitedFile(String file, String delimeter) throws IOException
  {
    this(file, delimeter, true);
  }

  public DelimitedFile(String file, String delimeter, boolean header) throws IOException
  {
    this.file = file;
    this.delimeter = delimeter;
    this.header = header;
    data = parse();
  }

  public int getColumn(String s)
  {
    if (!header)
      return -1;
    Integer i = (Integer) fields.get(s);
    if (i == null)
      return -1;
    return i.intValue();
  }

  @SuppressWarnings("rawtypes")
  public Vector getColumns()
  {
    if (!header)
      return null;
    Vector v = new Vector(fields.size());
    for (int i = 0; i < fields.size(); i++)
    {
      v.add(new Object());
    }
    Iterator i = fields.keySet().iterator();
    while (i.hasNext())
    {
      Object objKey = i.next();
      Object objPosition = fields.get(objKey);
      String key = (String) objKey;
      int position = ((Integer) objPosition).intValue();
      v.set(position, key);
    }
    return v;
  }

  public File getFile()
  {
    return new File(file);
  }

  @SuppressWarnings("rawtypes")
  public Vector getRow(int i)
  {
    return (Vector) data.get(i);
  }

  public int getRowCount()
  {
    return data.size();
  }

  @SuppressWarnings("rawtypes")
  private Vector parse() throws IOException
  {
    BufferedReader in = new BufferedReader(new FileReader(file));
    StringTokenizer st = null;
    int fieldcount = -1;
    int i;

    // header
    if (header)
    {
      String hdr = in.readLine();
      st = new StringTokenizer(hdr, delimeter);
      fieldcount = st.countTokens();
      fields = new HashMap(fieldcount);
      i = 0;
      while (st.hasMoreTokens())
      {
        fields.put(st.nextToken(), new Integer(i));
        i++;
      }
    }

    // data
    Vector d = new Vector();
    String str;
    int count;
    i = 1;
    while ((str = in.readLine()) != null)
    {
      i++;
      st = new StringTokenizer(str, delimeter);
      count = st.countTokens();
      if (header && count < fieldcount)
        System.out.println("WARNING: Row " + i + " has incorrect number of elements! (" + count
            + ")");
      Vector row = new Vector(count);
      while (st.hasMoreTokens())
      {
        row.add(st.nextToken());
      }
      d.add(row);
    }
    in.close();

    // return
    return d;
  }
}
