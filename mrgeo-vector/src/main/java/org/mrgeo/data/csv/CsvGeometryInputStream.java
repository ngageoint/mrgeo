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

import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

import org.mrgeo.hdfs.vector.Column;
import org.mrgeo.hdfs.vector.ColumnDefinitionFile;
import org.mrgeo.hdfs.vector.WktGeometryUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Vector;

public class CsvGeometryInputStream extends CsvInputStream
{
  private String[] columnsHeader = null;
  private GeometryType geometryType = null;
  private int[] geometryIndex;

  private enum GeometryType
  {
    GEOMETRY,
    X_Y,
    LON_LAT
  }

  public CsvGeometryInputStream(InputStream is) throws IOException
  {
    super(is);
  }

  public CsvGeometryInputStream(InputStream is, InputStream columnStream) throws IOException
  {
    super(is);

    if (columnStream != null)
    {
      readColumns(columnStream);
      columnStream.close();
    }
  }

  private void readColumns(InputStream is) throws IOException
  {
    ColumnDefinitionFile cdf = new ColumnDefinitionFile(is);

    Vector<Column> cols = cdf.getColumns();
    columnsHeader = new String[cols.size()];

    int i = 0;
    for (final Column column : cols)
    {
      final String col = column.getName();
      columnsHeader[i++] = col;
    }
  }

  public CsvGeometryInputStream(String fileName) throws IOException
  {
    super(fileName);
  }

  public CsvGeometryInputStream(InputStream is, String delimiter) throws IOException
  {
    super(is, delimiter);
  }
  public CsvGeometryInputStream(InputStream is, String delimiter, InputStream columnStream) throws IOException
  {
    super(is, delimiter);

    if (columnStream != null)
    {
      readColumns(columnStream);
      columnStream.close();
    }
  }

  public CsvGeometryInputStream(String fileName, String delimiter) throws IOException
  {
    super(fileName, delimiter);
  }

  public boolean hasGeometry()
  {
    if (geometryType != null)
    {
      return true;
    }
    
    boolean hasGeometry = false;

    if (hasHeader())
    {
      hasGeometry = checkHeaderForGeometry();
    }
    if (!hasGeometry && hasNext())
    {
      if (firstline != null)
      {
        hasGeometry = checkLineForGeometry(firstline);
      }
      else if (line != null)
      {
        hasGeometry = checkLineForGeometry(line);
      }
    }

    return hasGeometry;
  }  

  @Override
  public boolean hasHeader()
  {
    if (columnsHeader != null)
    {
      return true;
    }

    return super.hasHeader();
  }


  @Override
  public String[] getHeader()
  {
    String[] header;
    if (columnsHeader != null)
    {
      header = columnsHeader;
    }
    else 
    {
      header = super.getHeader();
    }
    
    // add the geometry column name if it is derived
    if (hasGeometry() && geometryType != GeometryType.GEOMETRY)
    {
      header = Arrays.copyOf(header, header.length + 1);
      header[header.length - 1] = "geometry";
    }
    
    return header;
  }
  
  @Override
  public String[] next()
  {
    if (geometryType == null)
    {
      hasGeometry();
    }
    
    String[] cols = super.next();
    
    // add the geometry column if it is derived
    if (geometryType != GeometryType.GEOMETRY)
    {
      cols = Arrays.copyOf(cols, cols.length + 1);
      
      cols[cols.length - 1] = "POINT(" + cols[geometryIndex[0]] + " " + cols[geometryIndex[1]] + ")";
    }
    
    return cols;
  }


  private boolean checkHeaderForGeometry()
  {
    if (hasHeader())
    {
      String[] header;
      if (columnsHeader != null)
      {
        header = columnsHeader;
      }
      else 
      {
        // need the raw header here (hence the super)
        header = super.getHeader();
      }

      // if the header has a "geometry" field, or lat/latitude and lon/longitude, or x & y, we'll
      // assume that is geometry...
      for (int i = 0; i < header.length; i++)
      {        
        final String col = header[i];
        if (col.equalsIgnoreCase("geometry"))
        {
          geometryType = GeometryType.GEOMETRY;
          geometryIndex = new int[]{i};
          return true;
        }
        else if (col.equalsIgnoreCase("x"))
        {
          int x = i;
          for (int y = 0; y < header.length; y++)
          {
            final String col2 = header[y];
            if (col2.equalsIgnoreCase("y"))
            {
              geometryType = GeometryType.X_Y;
              geometryIndex = new int[]{x, y};
              return true;
            }
          }
        }
        else if (col.equalsIgnoreCase("lat") || col.equalsIgnoreCase("latitude"))
        {
          int lat = i;
          for (int lon = 0; lon < header.length; lon++)
          {
            final String col2 = header[lon];
            if (col2.equalsIgnoreCase("lon") || col2.equalsIgnoreCase("longitude"))
            {
              geometryType = GeometryType.LON_LAT;
              geometryIndex = new int[]{lon, lat};

              return true;
            }
          }
        }
      }
    }
    return false;
  }

  private boolean checkLineForGeometry(final String[] line)
  {
    // we can only check for WKT geometry...
    for (int i = 0; i < line.length; i++)
    {
      final String col = line[i];
      if (WktGeometryUtils.isValidWktGeometry(col))
      {
        geometryType = GeometryType.GEOMETRY;
        geometryIndex = new int[]{i};
        return true;
      }
    }

    return false;
  }

  public static Object parseType(String str)
  {
    Object o = CsvInputStream.parseType(str);
    if (o instanceof String)
    {
      if (WktGeometryUtils.isValidWktGeometry(str))
      {
        try
        {
          WKTReader reader = new WKTReader();
          // Despite the javadocs on the "read" function, it does sometimes
          // return a null value when it can't parse the input.
          Object g = reader.read(str);
          if (g != null)
          {
            o = g;
          }
        }
        catch (ParseException e)
        {
          // shouldn't get here, but make it a no-op
        }
      }
    }
    return o;
  }


}
