/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */

package org.mrgeo.data.tsv;

import org.mrgeo.data.csv.CsvInputStream;

import java.io.IOException;
import java.io.InputStream;


/**
 * It is assumed that all TSV files are in WGS84.
 * 
 * @author jason.surratt
 * 
 */
public class TsvInputStream extends CsvInputStream
{

  public TsvInputStream(InputStream is) throws IOException
  {
    super(is, "\t");
  }
  
  public TsvInputStream(String fileName) throws IOException
  {
    super(fileName, "\t");
  }
}
