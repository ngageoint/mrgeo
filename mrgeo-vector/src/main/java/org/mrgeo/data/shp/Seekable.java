/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.data.shp;

import java.io.IOException;

public interface Seekable
{
  public void close() throws IOException;

  public long getPos() throws IOException;

  public void seek(long pos) throws IOException;
}
