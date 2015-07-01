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

package org.mrgeo.data.csv;

import java.io.*;
import java.util.Iterator;

/**
 * It is assumed that all CSV files are in WGS84.
 * 
 * @author jason.surratt
 * 
 */
public class CsvInputStream implements Iterator<String[]>
{
  protected String delimiter = ",";

  private boolean hasHeader;
  private String[] header;
  private boolean checkHeader = true;

  private LineNumberReader reader;
  private InputStream stream = null;

  // this is a side effect of the header check
  protected String[] firstline = null;
  protected String[] line = null;

  public CsvInputStream(final InputStream is) throws IOException
  {
    open(is);
  }

  public CsvInputStream(final String fileName) throws IOException
  {
    final FileInputStream fis = new FileInputStream(fileName);
    open(fis);
  }

  protected CsvInputStream(final InputStream is, final String delimiter) throws IOException
  {
    this.delimiter = delimiter;
    open(is);
  }

  protected CsvInputStream(final String fileName, final String delimiter) throws IOException
  {
    this.delimiter = delimiter;
    final FileInputStream fis = new FileInputStream(fileName);
    open(fis);
  }

  public void close() throws IOException
  {
    if (reader != null)
    {
      reader.close();
      reader = null;
    }

    if (stream != null)
    {
      stream.close();
      stream = null;
    }
  }

  public String getDelimiter()
  {
    return delimiter;
  }

  public String[] getHeader()
  {
    if (checkHeader)
    {
      checkForHeader();
    }

    if (hasHeader)
    {
      return header;
    }

    return new String[] {};

  }

  public int getRecordNumber()
  {
    if (checkHeader)
    {
      checkForHeader();
    }

    if (hasHeader)
    {
      return reader.getLineNumber() - 1;
    }

    return reader.getLineNumber();
  }

  public boolean hasHeader()
  {
    if (checkHeader)
    {
      checkForHeader();
    }
    return hasHeader;
  }

  @Override
  public boolean hasNext()
  {
    try
    {
      if (checkHeader)
      {
        checkForHeader();
      }

      if (line != null || firstline != null)
      {
        return true;
      }

      if (!reader.ready())
      {
        return false;
      }

      final String str = reader.readLine();
      line = str.split(delimiter);

      return true;
    }
    catch (final IOException e)
    {
      line = null;

      try
      {
        if (reader != null)
        {
          reader.close();
          reader = null;
        }

        if (stream != null)
        {
          stream.close();
          stream = null;
        }
      }
      catch (final IOException e1)
      {
        e1.printStackTrace();
      }

      return false;
    }
  }

  @Override
  public String[] next()
  {
    if (checkHeader)
    {
      checkForHeader();
    }

    if (firstline != null)
    {
      String[] f = firstline;
      firstline = null;
      return f;
    }

    if (line != null)
    {
      String[] l = line;
      line = null;
      return l;
    }

    return new String[] {};
  }

  @Override
  public void remove()
  {
    throw new UnsupportedOperationException();
  }

  public void setDelimiter(final String delim)
  {
    delimiter = delim;
  }

  private void checkForHeader()
  {
    try
    {
      if (reader.ready())
      {
        checkHeader = false;
        hasHeader = true;

        String str = reader.readLine();
        line = str.split(delimiter);

        // 1st make sure the line contains no booleans or numbers
        for (String col: line)
        {
          Object o = parseType(col);
          if (o instanceof Boolean || o instanceof Double)
          {
            hasHeader = false;
            break;
          }
        }

        // if the 1st line is all strings, 
        // lets see if the 2nd line contains any numbers or booleans
        if (hasHeader)
        {
          hasHeader = false;
          firstline = line;
          if (reader.ready())
          {
            str = reader.readLine();
            line = str.split(delimiter);

            for (String col: line)
            {
              Object o = parseType(col);
              if (o instanceof Boolean || o instanceof Double)
              {
                hasHeader = true;
                break;
              }
            }

            if (hasHeader)
            {
              header = firstline;
              firstline = null;
            }
          }
        }
      }
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
  }

  public static Object parseType(String str)
  {
    // are we a boolean
    if (str.equalsIgnoreCase("true") || str.equalsIgnoreCase("false"))
    {
      return Boolean.parseBoolean(str);
    }
    try
    {
      // are we a number
      return Double.parseDouble(str);
    }
    catch (NumberFormatException e)
    {

    }

    // we are a string
    return str;
  }

  private void open(final InputStream is) throws IOException
  {
    this.stream = is;
    final InputStreamReader isr = new InputStreamReader(this.stream);
    reader = new LineNumberReader(isr);

  }
}
