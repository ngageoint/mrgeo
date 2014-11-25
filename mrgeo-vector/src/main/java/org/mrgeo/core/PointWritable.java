/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */

package org.mrgeo.core;

import org.apache.hadoop.io.WritableComparable;
import org.mrgeo.geometry.GeometryFactory;
import org.mrgeo.geometry.Point;
import org.mrgeo.geometry.WritablePoint;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 */
public class PointWritable implements WritableComparable<PointWritable>
{
  private WritablePoint p;

  public PointWritable()
  {
  }

  public PointWritable(double x, double y)
  {
    p = GeometryFactory.createPoint(x, y);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Comparable#compareTo(java.lang.Object)
   */
  @Override
  public int compareTo(PointWritable o)
  {
    // check x's first
    int result = Double.compare(p.getX(), o.p.getX());
    // if the x's are equal, check the y's
    if (result == 0)
    {
      result = Double.compare(p.getY(), o.p.getY());
    }
    return result;
  }

  public final Point getPoint()
  {
    return p;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
   */
  @Override
  public void readFields(DataInput in) throws IOException
  {
    p = GeometryFactory.createPoint(in.readDouble(), in.readDouble(), in.readDouble());
  }

  /**
   * @param x
   * @param y
   */
  public void set(double x, double y)
  {
    if (p == null)
    {
      p = GeometryFactory.createPoint(x, y);
    }
    else
    {
      p.setX(x);
      p.setY(y);
    }
  }

  public void setPoint(Point p)
  {
    this.p = (WritablePoint) p.asWritable();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
   */
  @Override
  public void write(DataOutput out) throws IOException
  {
    out.writeDouble(p.getX());
    out.writeDouble(p.getY());
    out.writeDouble(p.getZ());
  }

}
