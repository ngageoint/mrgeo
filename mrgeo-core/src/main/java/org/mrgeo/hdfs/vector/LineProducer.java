package org.mrgeo.hdfs.vector;

import java.io.Closeable;
import java.io.IOException;

public interface LineProducer extends Closeable
{
  /**
   * Returns the next available line from the source. If no more lines are
   * available, it returns null.
   * 
   * @return next available line or null when there are no more
   * @throws IOException 
   */
  public String nextLine() throws IOException;
}
