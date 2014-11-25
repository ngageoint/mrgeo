/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.format;


/**
 * Reads tab separated values as geometries.
 */

public class TsvInputFormat extends CsvInputFormat
{
  private static final long serialVersionUID = -1;
  
  static public class TsvRecordReader extends CsvInputFormat.CsvRecordReader
  {
    public TsvRecordReader()
    {
      super('\t');
    }

  }
  
  public TsvInputFormat()
  {
    super();
    
    _delimiter = '\t';
  }
}
