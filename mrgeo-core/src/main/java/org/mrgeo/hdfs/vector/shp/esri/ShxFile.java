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

package org.mrgeo.hdfs.vector.shp.esri;

import org.mrgeo.hdfs.vector.shp.SeekableDataInput;
import org.mrgeo.hdfs.vector.shp.esri.geom.JShape;
import org.mrgeo.hdfs.vector.shp.util.Convert;

import java.io.IOException;


public class ShxFile extends java.lang.Object
{
  public final static int DEFAULT_CACHE_SIZE = 500;

  /**
   * Opens a SHX file as read-only.
   */
  protected static ShxFile open(SeekableDataInput in, boolean cachemode, int cachesize)
  {
    return new ShxFile(in, cachemode, cachesize);
  }

  protected boolean cachemode = false; // cache mode indicator flag
  protected int cachepos; // cache position (inclusive); dynamic access only
  protected int cachesize; // size of cache - represents all rows if not dynamic
  protected int[] contentLength = null;
  protected Header header = null;
  private SeekableDataInput in = null;
  protected boolean modData = false;
  protected int[] offset = null;

  protected int recordCount;

  
  public void close() throws IOException
  {
    if (in != null)
    {
      in.close();
    }
  }
  /**
   * Opens a SHX file as read-only. Made private for clarity's sake.
   */
  private ShxFile(SeekableDataInput in, boolean cachemode, int cachesize)
  {
    this.in = in;
    recordCount = 0;
    header = new Header();
    // initialize arrays
    offset = new int[0];
    contentLength = new int[0];
    this.cachemode = cachemode;
    this.cachesize = cachesize;
  }

  public void addRow(int i, JShape obj)
  {
    // i is # of rows, for verification purposes only
    // offset
    int[] temp = new int[offset.length + 1];
    System.arraycopy(offset, 0, temp, 0, offset.length);
    int[] prev = null;
    if (offset.length == 0)
    {
      if (cachepos == 0)
      {
        prev = new int[2];
        prev[0] = 46;
        prev[1] = 0;
      }
      else
      {
        try
        {
          prev = lookupRecordData(in, i - 2);
        }
        catch (Exception e)
        {
          throw new RuntimeException("Can't Lookup SHX Record!");
        }
      }
    }
    else
    {
      prev = new int[2];
      prev[0] = offset[offset.length - 1];
      prev[1] = contentLength[offset.length - 1];
    }
    temp[offset.length] = prev[0] + prev[1] + 4;
    if ((i - 1) != (offset.length + cachepos))
      throw new RuntimeException("ShxFile Add Row Inconsistency");
    offset = temp;
    // contentlength
    temp = new int[contentLength.length + 1];
    System.arraycopy(contentLength, 0, temp, 0, contentLength.length);
    temp[contentLength.length] = obj.getRecordLength() / 2;
    contentLength = temp;
  }

  @Override
  protected void finalize() throws IOException
  {
    if (modData)
      save();
    if (in != null)
      in.close();
  }

  public int getCachePos()
  {
    return cachepos;
  }

  public int getCacheSize()
  {
    return cachesize;
  }

  public int getContentLength(int i)
  {
    try
    {
      if (i < cachepos || i > (cachepos + contentLength.length - 1))
      {
        if (modData)
          saveRows(cachepos);
        loadData(i);
      }
      return contentLength[i - cachepos];
    }
    catch (Exception e)
    {
      throw new RuntimeException(e);
    }
  }

  public int getCurrentCacheSize()
  {
    return offset.length;
  }

  public int getNumRecords()
  {
    return recordCount;
  }

  public int getOffset(int i)
  {
    try
    {
      if (i < cachepos || i > (cachepos + offset.length - 1))
      {
        if (modData)
          saveRows(cachepos);
        loadData(i);
      }
      return offset[i - cachepos];
    }
    catch (Exception e)
    {
      throw new RuntimeException(e);
    }
  }

  public boolean isCached()
  {
    return cachemode;
  }

  public void load() throws IOException, FormatException
  {
    // read header
    in.seek(0);
    header.load(in);
    // calculate number of records
    int byteCount = (header.fileLength - 50) * 2;
    recordCount = byteCount / 8;
    // initialize
    cachepos = -1;
    if (!cachemode)
      cachesize = recordCount;
    // read data
    loadData(in, 0);
  }

  protected void loadData(int i) throws IOException
  {
    if (header == null)
      throw new IOException("Header never read.  Cannot load!");
    loadData(in, i);
  }

  private void loadData(SeekableDataInput is, int i) throws IOException
  {
    // new position
    cachepos = i;
    // reset row array
    int max = (((recordCount - i) > cachesize) ? cachesize : (recordCount - i));
    offset = new int[max];
    contentLength = new int[max];
    int pos = 100 + (8 * i);
    is.seek(pos);
    // read data
    for (int j = 0; j < offset.length; j++)
    {
      // initialize row data
      loadRecord(is, j);
    }
  }

  protected void loadRecord(SeekableDataInput is, int i) throws IOException
  {
    // read data
    byte[] record = new byte[8];
    is.readFully(record, 0, 8);
    offset[i] = Convert.getInteger(record, 0);
    contentLength[i] = Convert.getInteger(record, 4);
  }

  protected int[] lookupRecordData(SeekableDataInput is, int i) throws IOException
  {
    long current = is.getPos();
    int pos = 100 + (8 * i);
    is.seek(pos);
    // read data
    byte[] record = new byte[8];
    is.readFully(record, 0, 8);
    int[] temp = new int[2];
    temp[0] = Convert.getInteger(record, 0);
    temp[1] = Convert.getInteger(record, 4);
    is.seek(current);
    return temp;
  }

  public void save()
  {
    throw new UnsupportedOperationException();
  }

  public void saveRows(int i)
  {
    throw new UnsupportedOperationException();
  }

  public void setCacheSize(int size)
  {
    if (!cachemode)
      return;
    if (size < 1)
      size = 1;
    cachesize = size;
  }
}
