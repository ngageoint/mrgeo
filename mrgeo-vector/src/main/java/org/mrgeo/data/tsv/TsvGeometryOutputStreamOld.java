/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */

package org.mrgeo.data.tsv;

import org.mrgeo.data.csv.CsvGeometryOutputStreamOld;

import java.io.OutputStream;


/**
 * It is assumed that all CSV files are in WGS84.
 * 
 * @author jason.surratt
 * 
 */
public class TsvGeometryOutputStreamOld extends CsvGeometryOutputStreamOld
{

  public TsvGeometryOutputStreamOld(OutputStream os)
  {
    super(os, "\t");
  }

}
