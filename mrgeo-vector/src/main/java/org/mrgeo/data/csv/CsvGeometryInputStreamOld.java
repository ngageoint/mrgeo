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

import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.ParseException;
import jj2000.j2k.NotImplementedError;
import org.geotools.geometry.jts.WKTReader2;
import org.mrgeo.geometry.GeometryFactory;
import org.mrgeo.geometry.*;
import org.mrgeo.data.GeometryInputStream;

import java.io.*;


/**
 * It is assumed that all CSV files are in WGS84.
 * 
 * @author jason.surratt
 * 
 */
public class CsvGeometryInputStreamOld implements GeometryInputStream
{
  public class BadFormatException extends RuntimeException
  {
    private static final long serialVersionUID = 1L;

    public BadFormatException(String message)
    {
      super(message);
    }
  }

  public class UnderlyingException extends RuntimeException
  {
    private static final long serialVersionUID = 1L;

    public UnderlyingException(String message, Throwable cause)
    {
      super(message, cause);
    }
  }

  protected String delimiter = ",";

  LineNumberReader reader;
  InputStream is = null;

  public CsvGeometryInputStreamOld(InputStream is) throws IOException
  {
    loadCsv(is);
  }
  protected CsvGeometryInputStreamOld(InputStream is, String delimiter) throws IOException
  {
    this.delimiter = delimiter;
    loadCsv(is);
  }


  /**
   * Convenience function similar to above.
   * 
   * @param fileName
   * @throws IOException
   */
  public CsvGeometryInputStreamOld(String fileName) throws IOException
  {
    FileInputStream fis = new FileInputStream(fileName);
    loadCsv(fis);
  }

  protected CsvGeometryInputStreamOld(String fileName, String delimiter) throws IOException
  {
    this.delimiter = delimiter;
    FileInputStream fis = new FileInputStream(fileName);
    loadCsv(fis);
  }

  @Override
  public String getProjection()
  {
    return WellKnownProjections.WGS84;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.spadac.Geometry.GeometryInputStream#hasNext()
   */
  @Override
  public boolean hasNext()
  {
    try
    {
      return reader.ready();
    }
    catch (IOException e)
    {
      try
      {
        if (reader != null)
        {
          reader.close();
        }

        if (is != null)
        {
          is.close();
        }
      }
      catch (IOException e1)
      {
        e1.printStackTrace();
      }
      return false;
    }
  }

  /**
   * @param is
   * @throws IOException
   */
  @SuppressWarnings("hiding")
  private void loadCsv(InputStream is) throws IOException
  {
    this.is = is;
    InputStreamReader isr = new InputStreamReader(this.is);
    reader = new LineNumberReader(isr);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.spadac.Geometry.GeometryInputStream#next()
   */
  @Override
  public WritableGeometry next()
  {
    WritablePoint result = null;
    boolean goodline = false;
    while (hasNext() && goodline == false)
    {
      String str;
      try
      {
        str = reader.readLine();
      }
      catch (IOException e)
      {
        throw new UnderlyingException("Error reading a line from CSV", e);
      }
      
      String[] splits = str.split(delimiter);
      
      WKTReader2 wkt = new WKTReader2();
      for (String split: splits)
      {
        try
        {
          Geometry geometry = wkt.read(split);
          return convertToGeometry(geometry);
        }
        catch (ParseException e)
        {
          // swallow the parse exception.  This column may not be a geometry
        }
      }
      
      if (splits.length < 2)
      {
        throw new BadFormatException(String
            .format("CSV File is not properly formatted, too few columns. (line: %d)", reader
                .getLineNumber()));
      }
      try
      {
        result = GeometryFactory.createPoint(Double.valueOf(splits[0]), Double.valueOf(splits[1]));
        goodline = true;
      }
      catch(NumberFormatException e)
      {
      }
    }
    return result;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.util.Iterator#remove()
   */
  @Override
  public void remove()
  {
    throw new UnsupportedOperationException();
  }


  @Override
  public void close() throws IOException
  {
    if (is != null)
    {
      is.close();
      is = null;
    }
  }
  
  private static WritableGeometry convertToGeometry(Geometry shape)
  {
    if (shape instanceof Point)
    {
      Point p = (Point)shape;
      return GeometryFactory.createPoint(p.getX(), p.getY());
    }
    else if (shape instanceof Polygon)
    {
      return convertToPolygon((Polygon)shape);
    }
    else if (shape instanceof LineString)
    {
      convertToLineString((LineString)shape);
    }
    else if (shape instanceof GeometryCollection)
    {
      throw new NotImplementedError("GeometryCollection not yet implemented!");
    }
        
    return null;
  }

  private static WritableLineString convertToLineString(LineString line)
  {
    WritableLineString result = GeometryFactory.createLineString();
    
    for (int i = 0; i < line.getNumPoints(); i++)
    {
      Coordinate c = line.getCoordinateN(i);
      result.addPoint(GeometryFactory.createPoint(c.x, c.y));
    }

    return result;
  }


  private static WritableGeometry convertToPolygon(Polygon polygon)
  {
    WritablePolygon result = GeometryFactory.createPolygon();
    
    WritableLinearRing exteriorRing = GeometryFactory.createLinearRing();
    
    LineString ring;
    ring = polygon.getExteriorRing();
    for (int p = 0; p < ring.getNumPoints(); p++)
    {
        Coordinate c = ring.getCoordinateN(p);
        
        exteriorRing.addPoint(GeometryFactory.createPoint(c.x, c.y));
    }
    result.setExteriorRing(exteriorRing);

    for (int r = 0; r < polygon.getNumInteriorRing(); r++)
    {
      WritableLinearRing interiorRing = GeometryFactory.createLinearRing();

      ring = polygon.getInteriorRingN(r);
      for (int p = 0; p < ring.getNumPoints(); p++)
      {
          Coordinate c = ring.getCoordinateN(p);
          
          interiorRing.addPoint(GeometryFactory.createPoint(c.x, c.y));
      }
      result.addInteriorRing(interiorRing);
    }
    
    return result;
  }


}
