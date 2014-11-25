/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */

package org.mrgeo.data.csv;

import org.mrgeo.geometry.Geometry;
import org.mrgeo.geometry.Point;
import org.mrgeo.data.GeometryOutputStream;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;


/**
 * It is assumed that all CSV files are in WGS84.
 * 
 * @author jason.surratt
 * 
 */
public class CsvGeometryOutputStreamOld implements GeometryOutputStream
{
  private OutputStream os;
  private PrintStream ps;
  
  protected String delimiter = "\t";
  
  public class InvalidGeometryException extends IOException
  {
    private static final long serialVersionUID = 1L;
    
    public InvalidGeometryException(String message)
    {
      super(message);
    }

    public InvalidGeometryException(String message, Throwable cause)
    {
      super(message, cause);
    }
  }

  public CsvGeometryOutputStreamOld(OutputStream os)
  {
    this.os = os;
    init();
  }

  private void init()
  {
    ps = new PrintStream(os);
    ps.println("x"+ delimiter + "y" + delimiter + "z");
  }
  
  public CsvGeometryOutputStreamOld(OutputStream os, String delimiter)
  {
    this.os = os;
    this.delimiter = delimiter;
    
    init();
  }

  @Override
  public void close() throws IOException
  {
    ps.close();
  }

  @Override
  public void flush() throws IOException
  {
    ps.flush();
  }

  @Override
  public void write(Geometry g) throws IOException
  {
    if (g instanceof Point)
    {
      Point p = (Point)g;
      ps.println(String.format("%g%s%g%s%g", p.getX(), delimiter, p.getY(), delimiter, p.getZ()));
    }
    else
    {
      throw new InvalidGeometryException("Only points are supported for writing to CSV");
    }
  }

}
