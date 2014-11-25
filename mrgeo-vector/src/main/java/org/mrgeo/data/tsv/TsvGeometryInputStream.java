package org.mrgeo.data.tsv;

import org.mrgeo.data.csv.CsvGeometryInputStream;

import java.io.IOException;
import java.io.InputStream;

public class TsvGeometryInputStream extends CsvGeometryInputStream
{

  public TsvGeometryInputStream(InputStream is) throws IOException
  {
    super(is, "\t");
  }
  
  public TsvGeometryInputStream(InputStream is, InputStream columnStream) throws IOException
  {
    super(is, "\t", columnStream);
  }

}
