/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */

package org.mrgeo.data.tsv;

import org.mrgeo.data.csv.CsvGeometryInputStreamOld;

import java.io.IOException;
import java.io.InputStream;


/**
 * It is assumed that all TSV files are in WGS84.
 * 
 * @author jason.surratt
 * 
 */
public class TsvGeometryInputStreamOld extends CsvGeometryInputStreamOld
{

  public TsvGeometryInputStreamOld(InputStream is) throws IOException
  {
    super(is, "\t");
  }
  
  public TsvGeometryInputStreamOld(String fileName) throws IOException
  {
    super(fileName, "\t");
  }
}
