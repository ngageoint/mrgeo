/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.mapreduce;

import org.apache.hadoop.io.ArrayWritable;

public class GeometryArrayWritable extends ArrayWritable
{
  public GeometryArrayWritable()
  {
    super(GeometryWritable.class);
  }
}