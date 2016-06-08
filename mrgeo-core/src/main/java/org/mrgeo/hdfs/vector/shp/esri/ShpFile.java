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
import java.io.RandomAccessFile;


/**
 * Internal class used by ESRILayer. If you're looking to read and write
 * shapefiles use ESRILayer instead.
 */
public class ShpFile
{
  protected ShpData data = null;
  //protected String fileName = null;
  protected Header header = null;
  private SeekableDataInput in = null;
  private ShxFile index = null;
  private ESRILayer parent;

  public void close() throws IOException
  {
    if (in != null)
    {
      in.close();
    }
  }
  
  /** Creates new ShpFile */
  protected ShpFile(ShxFile index, ESRILayer parent)
  {
    this.index = index;
    this.parent = parent;
    header = new Header();
  }

 
  protected void load(SeekableDataInput sdi) throws IOException, FormatException
  {
    // save raf reference
    in = sdi;
    
    // read header
    in.seek(0);
    header.load(in);
    // initialize data interface
    switch (header.shapeType)
    {
    case JShape.POINT:
      data = new ShpPoint(index.recordCount);
      break;
    case JShape.POLYLINE:
      data = new ShpPolyLine(index.recordCount);
      break;
    case JShape.POLYGON:
      data = new ShpPolygon(index.recordCount);
      break;
    case JShape.POINTZ:
      data = new ShpPointZ(index.recordCount);
      break;
    case JShape.POLYLINEZ:
      data = new ShpPolyLineZ(index.recordCount);
      break;
    case JShape.POLYGONZ:
      data = new ShpPolygonZ(index.recordCount);
      break;
    default:
      throw new FormatException("Unhandled Shape Type: " + header.shapeType);
    }
    // set parent
    data.setParent(parent);
    // read data
    loadData(in, 0);
  }

  protected void loadData(int i) throws IOException, FormatException
  {
    if (header == null)
      throw new IOException("Header never read.  Cannot load!");
    loadData(in, i);
  }

  private void loadData(SeekableDataInput is, int i) throws IOException, FormatException
  {
    //System.out.printf("cache size: %d", index.getCurrentCacheSize());
    // resize
    data.resizeCache(index.getCurrentCacheSize());
    int pos = index.getOffset(i);
    is.seek(pos * 2);
    // read data
    for (int j = 0; j < index.getCurrentCacheSize(); j++)
    {
      // initialize row data
      loadRecord(is, j);
    }
  }

  protected void loadRecord(SeekableDataInput is, int i) throws IOException, FormatException
  {
    // read record header
    byte[] recordHeader = new byte[8];
    is.readFully(recordHeader, 0, 8);
    int recordNumber = Convert.getInteger(recordHeader, 0);
    if ((i + index.getCachePos() + 1) != recordNumber)
      throw new FormatException("Unequal SHP Record Number Specification (" + recordNumber + ")");
    int contentLength = Convert.getInteger(recordHeader, 4);
    if (index.getContentLength(i + index.getCachePos()) != contentLength)
      throw new FormatException("Unequal SHP/SHX Content Length Specification (" + contentLength
          + ")");
    byte[] record = new byte[contentLength * 2];
    is.readFully(record, 0, contentLength * 2);
    // read geometry
    data.load(i, record);
  }

  public void save(RandomAccessFile out) throws IOException
  {
    // write data
    if (index.offset.length > 0)
      saveRows(out, index.getCachePos());
    // recalc header vars
    // calculate fileLength first (written in header)
    if (index.offset.length > 0)
    {
      int filePos = 0;
      if (index.cachepos == 0)
        filePos = 100;
      for (int j = 0; j < index.offset.length; j++)
      {
        // load geometry to get size
        JShape tempShape = data.getShape(j + index.cachepos);
        filePos += 8 + tempShape.getRecordLength();
      }
      header.fileLength += filePos / 2;
      out.setLength(2 * header.fileLength);
      // write header
      out.seek(0);
      header.save(out);
    }
  }

  private void saveRows(RandomAccessFile os, int i) throws IOException
  {
    // write data
    int filePos = 100;
    int pos = index.getOffset(i);
    os.seek(pos * 2);
    // loop
    for (int j = 0; j < index.getCurrentCacheSize(); j++)
    {
      // load geometry
      byte[] record = data.save(j);
      int contentLength = record.length / 2;

      // write record header
      byte[] recordHeader = new byte[8];
      Convert.setInteger(recordHeader, 0, j + 1 + index.getCachePos()); // recordNumber
      Convert.setInteger(recordHeader, 4, contentLength);
      os.write(recordHeader, 0, recordHeader.length);
      index.offset[j] = filePos / 2;
      index.contentLength[j] = contentLength;
      filePos = filePos + 8;

      // write geometry
      os.write(record, 0, record.length);
      filePos = filePos + contentLength * 2;
    }
  }
}
