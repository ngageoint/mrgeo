/*
 * Copyright 2009-2017. DigitalGlobe, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.mrgeo.hdfs.vector.shp.esri;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.mrgeo.hdfs.vector.shp.SeekableDataInput;
import org.mrgeo.hdfs.vector.shp.SeekableRaf;
import org.mrgeo.hdfs.vector.shp.dbase.DbaseException;
import org.mrgeo.hdfs.vector.shp.dbase.DbaseFile;
import org.mrgeo.hdfs.vector.shp.esri.geom.JExtent;
import org.mrgeo.hdfs.vector.shp.esri.geom.JShape;
import org.mrgeo.utils.GDALUtils;

import java.io.*;


/**
 * Provides basic reading of Shapefiles. There is some old code in place for
 * writing shapefiles and reading DBFs, but many changes have been made w/o
 * supporting or testing the code.
 * <p>
 * TODO Write unit tests and supporting DBFs and writing
 * <p>
 * This class was ported from the legacy Orion code and has no existing unit
 * tests so it should be questioned if you experience problems.
 */
public class ESRILayer
{
protected ShxFile index;
@SuppressFBWarnings(value = "URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD", justification = "used in subclasses")
protected String mode; // RAF mode
protected ShpFile shape;
protected DbaseFile table;
//private double maxScale = Long.MAX_VALUE;
//private double minScale;
private String projection = "";
//private boolean visible = true;

/**
 * Opens an existing shapefile for read-only.
 */
public ESRILayer(SeekableDataInput shp, SeekableDataInput shx, SeekableDataInput dbf,
    InputStream prj) throws FormatException, IOException
{
  internalLoader(shp, shx, dbf, prj, ShxFile.DEFAULT_CACHE_SIZE);
}

public static String getBaseName(String fileName)
{
  if (fileName.length() > 4 && fileName.toLowerCase().endsWith(".shp"))
  {
    return fileName.substring(0, fileName.length() - 4);
  }
  throw new IllegalArgumentException("Please specify shapefile name. E.g. 'MyShapefile.shp'");
}

/**
 * Opens a Shapefile as read only.
 *
 * @return
 * @throws DbaseException
 * @throws IOException
 * @throws FormatException
 */
public static ESRILayer open(SeekableDataInput shp, SeekableDataInput shx, SeekableDataInput dbf,
    InputStream prj) throws FormatException, IOException, DbaseException
{
  return new ESRILayer(shp, shx, dbf, prj);
}

/**
 * Opens a Shapefile as read only.
 *
 * @return
 * @throws DbaseException
 * @throws IOException
 * @throws FormatException
 */
@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "method only build valid shp file names")
public static ESRILayer open(String fileName) throws FormatException, IOException, DbaseException
{
  String fileBase = getBaseName(fileName);
  // load shx file
  RandomAccessFile shxRaf = new RandomAccessFile(fileBase + ".shx", "r");
  RandomAccessFile shpRaf = new RandomAccessFile(fileName, "r");
  RandomAccessFile dbfRaf = new RandomAccessFile(fileBase + ".dbf", "r");
  FileInputStream prjIs = null;
  if (new File(fileBase + ".prj").exists())
  {
    prjIs = new FileInputStream(fileBase + ".prj");
  }

  try
  {
    return open(new SeekableRaf(shpRaf), new SeekableRaf(shxRaf), new SeekableRaf(dbfRaf), prjIs);
  }
  finally
  {
    if (prjIs != null)
    {
      prjIs.close();
    }
  }

}

public void close() throws IOException
{
  if (shape != null)
  {
    shape.close();
  }
  if (index != null)
  {
    index.close();
  }
  if (table != null)
  {
    table.close();
  }
}

public int getCacheSize()
{
  return index.getCacheSize();
}

public int getColumn(String fieldName)
{
  return table.getColumn(fieldName);
}

/**
 * Returns the number of points in the current geometry -- odd.
 *
 * @return
 */
public int getCount()
{
  if (shape != null)
  {
    return shape.data.getCount();
  }
  return 0;
}

@SuppressWarnings("squid:S1166") // Exception caught and handled
public String[] getDataColumns()
{
  String[] temp = new String[table.getHeader().getFieldCount()];
  for (int i = 0; i < table.getHeader().getFieldCount(); i++)
  {
    try
    {
      temp[i] = table.getHeader().getField(i).name;
    }
    catch (DbaseException dbe)
    {
      temp[i] = null;
    }
  }
  return temp;
}

@SuppressWarnings("squid:S1166") // Exception caught and handled
public int[] getDataWidths()
{
  int[] temp = new int[table.getHeader().getFieldCount()];
  for (int i = 0; i < table.getHeader().getFieldCount(); i++)
  {
    try
    {
      temp[i] = table.getHeader().getField(i).length;
    }
    catch (DbaseException dbe)
    {
      temp[i] = 0;
    }
  }
  return temp;
}

//public double getMaxScale()
//{
//  return maxScale;
//}
//
//public double getMinScale()
//{
//  return minScale;
//}

public int getNumRecords()
{
  return index.recordCount;
}

/**
 * Returns the WKT representation of the projection as read from the .prj
 * file. If there is no .prj file or the file is empty, the WKT for WGS84 is
 * returned.
 */
public String getProjection()
{
  return projection;
}

@SuppressWarnings("squid:S00112")
// I didn't write this code, so I'm not sure why it throws the RuntimeException.  Keeping it
public JShape getShape(int i)
{
  if (shape != null)
  {
    try
    {
      return shape.data.getShape(i);
    }
    catch (Exception e)
    {
      throw new RuntimeException(e);
    }
  }
  return null;
}

public int getShapeType()
{
  if (shape != null)
  {
    return shape.header.shapeType;
  }
  return 0;
}

public DbaseFile getTable()
{
  return table;
}

//public boolean isLayerVisible()
//{
//  return visible;
//}

@SuppressWarnings("squid:S00112")
// I didn't write this code, so I'm not sure why it throws the RuntimeException.  Keeping it
public void save() throws Exception
{
  throw new UnsupportedOperationException();
}

public void updateExtent(boolean check) throws IOException
{
  JExtent groupExtent = null;
  JExtent tempExtent = null;
  JShape obj = null;
  if (shape != null)
  {
    synchronized (shape)
    {
      for (int i = 0; i < shape.data.getCount(); i++)
      {
        obj = shape.data.getShape(i);
        if (obj != null)
        {
          if (check)
          {
            obj.updateExtent();
          }

          tempExtent = obj.getExtent();
          if (groupExtent == null)
          {
            groupExtent = (JExtent) tempExtent.clone();
          }
          else
          {
            if (tempExtent != null)
            {
              JExtent.union(groupExtent, tempExtent, groupExtent);
            }
          }
        }
      }
    }
    // set extent
    if (shape.header.extent == null && groupExtent != null)
    {
      shape.header.extent = new JExtent();
    }
    shape.header.extent = groupExtent;
    index.header.extent = shape.header.extent;
  }
}

private void internalLoader(SeekableDataInput shp, SeekableDataInput shx, SeekableDataInput dbf,
    InputStream prj, int cachesize) throws FormatException, IOException
{
  mode = "r";
  // normal ESRI file set (unzipped)
  try
  {
    // load shx file
    index = ShxFile.open(shx, true, cachesize);
    index.load();
    // load shp file
    shape = new ShpFile(index, this);
    shape.load(shp);
    table = DbaseFile.open(dbf, true, cachesize, "r");
  }
  catch (IOException | FormatException e)
  {
    throw e;
  }
  catch (Exception e)
  {
    throw new IOException("Unknown Error Reading File!", e);
  }

  // read in the projection information from the .prj file
  projection = null;
  if (prj != null)
  {
    BufferedReader reader = new BufferedReader(new InputStreamReader(prj));
    String tmp = reader.readLine();
    if (tmp != null && tmp.trim().compareTo("") != 0)
    {
      projection = tmp;
    }
  }
  // given no other information default to WGS84
  if (projection == null)
  {
    projection = GDALUtils.EPSG4326();
  }
}
}
