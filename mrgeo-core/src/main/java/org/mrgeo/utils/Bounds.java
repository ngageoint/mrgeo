/*
 * Copyright 2009-2015 DigitalGlobe, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.mrgeo.utils;

import com.vividsolutions.jts.geom.Envelope;
import org.apache.commons.lang3.NotImplementedException;
import org.codehaus.jackson.annotate.JsonIgnore;

import java.awt.geom.Rectangle2D;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author jason.surratt
 * 
 */

// NOTE: This class is json serialized, so there are @JsonIgnore annotations on the
//       getters/setters that should not be automatically serialized
//
// NOTE:  Because of the json serialization, we'll call individual setters on 
//        initialization.  That means we need to keep track of what setters have 
//        been called, and update the <set> value when all 4 are called.  That's
//        what the <individualParamSet> is for.
public class Bounds  implements Comparable<Bounds>, Externalizable
{
  private static final long serialVersionUID = 1L;

  public static final Bounds world = new Bounds(-180, -90, 180, 90);

  private double minX, minY, maxX, maxY;
  private boolean set = false;

  private int individualParamSet = 0;

  public Bounds()
  {
    clear();
  }

  public Bounds(double[] ary)
  {
    this.minX = ary[0];
    this.minY = ary[1];
    this.maxX = ary[2];
    this.maxY = ary[3];

    set = true;
  }

  public Bounds(double minX, double minY, double maxX, double maxY)
  {
    this.minX = minX;
    this.minY = minY;
    this.maxX = maxX;
    this.maxY = maxY;

    set = true;
  }

  public Bounds(Rectangle2D from)
  {
    minX = from.getMinX();
    minY = from.getMinY();
    maxX = from.getMaxX();
    maxY = from.getMaxY();

    set = true;
  }
  
  public Bounds(String boundsStr)
  {
    //expects comma delimited, no spaces: minX,minY,maxX,maxY; e.g. "-115,30,-95,50"
    String[] boundsParts = boundsStr.split(",");
    this.minX = Double.parseDouble(boundsParts[0]);
    this.minY = Double.parseDouble(boundsParts[1]);
    this.maxX = Double.parseDouble(boundsParts[2]);
    this.maxY = Double.parseDouble(boundsParts[3]);

    set = true;
  }

  @Override
  public Bounds clone()
  {
    return new Bounds(minX, minY, maxX, maxY);
  }

  @Override
  public int compareTo(Bounds o)
  {
    if (minX < o.minX)
    {
      return -1;
    }
    else if (minX == o.minX)
    {
      return Double.compare(minY, o.minY);
    }

    return 1;
  }

  @Override
  public boolean equals(Object obj)
  {
    boolean result = false;
    if (obj instanceof Bounds)
    {
      Bounds other = (Bounds) obj;
      result = minX == other.minX && minY == other.minY && maxX == other.maxX && maxY == other.maxY;
    }
    return result;
  }

  public void expand(Bounds b)
  {
    if (!set)
    {
      if (b.isValid())
      {
        minX = b.minX;
        minY = b.minY;
        maxX = b.maxX;
        maxY = b.maxY;

        set = true;
      }
    }
    else
    {
      minX = Math.min(b.minX, minX);
      minY = Math.min(b.minY, minY);
      maxX = Math.max(b.maxX, maxX);
      maxY = Math.max(b.maxY, maxY);
    }
  }
  
  public void expand(final double xMin, final double yMin, final double xMax, final double yMax)
  {
    if (!set)
    {
        minX = xMin;
        minY = yMin;
        maxX = xMax;
        maxY = yMax;

        set = true;
    }
    else
    {
      minX = Math.min(xMin, minX);
      minY = Math.min(yMin, minY);
      maxX = Math.max(xMax, maxX);
      maxY = Math.max(yMax, maxY);
    }
  }

  public void expand(double x, double y)
  {
    if (!set)
    {
      minX = x;
      minY = y;
      maxX = x;
      maxY = y;

      set = true;
    }
    else
    {
      minX = Math.min(x, minX);
      minY = Math.min(y, minY);
      maxX = Math.max(x, maxX);
      maxY = Math.max(y, maxY);
    }
  }
  @JsonIgnore 
  public TMSUtils.Bounds getTMSBounds()
  {
    return new TMSUtils.Bounds(minX, minY, maxX, maxY);
  }

  @JsonIgnore 
  public double getCenterX()
  {
    return (minX + maxX) / 2.0;
  }

  @JsonIgnore 
  public double getCenterY()
  {
    return (minY + maxY) / 2.0;
  }

  @JsonIgnore 
  public double getHeight()
  {
    return maxY - minY;
  }

  public double getMaxX()
  {
    return maxX;
  }

  public double getMaxY()
  {
    return maxY;
  }

  public double getMinX()
  {
    return minX;
  }

  public double getMinY()
  {
    return minY;
  }

  @JsonIgnore 
  public double getWidth()
  {
    return maxX - minX;
  }

  public void clear()
  {
    minX = Double.MAX_VALUE;
    minY = Double.MAX_VALUE;
    maxX = -Double.MAX_VALUE;
    maxY = -Double.MAX_VALUE;
  }

  @JsonIgnore 
  public boolean isValid()
  {
    return (set && (minX != Double.MAX_VALUE) && (minY != Double.MAX_VALUE)
        && (maxX != -Double.MAX_VALUE) && (maxY != -Double.MAX_VALUE));
  }

  public boolean intersects(final double srcMinX, final double srcMinY, final double srcMaxX, final double srcMaxY)
  {
    return intersects(new Bounds(srcMinX, srcMinY, srcMaxX, srcMaxY));
  }

  public boolean intersects(final Bounds b)
  {
    // formula:
    // x, y are center points of the rectangle...
    //
    //    if Math.abs(rectA.x - rectB.x) < (Math.abs(rectA.width + rectB.width) / 2) 
    //      && (Math.abs(rectA.y - rectB.y) < (Math.abs(rectA.height + rectB.height) / 2))
    //    then
    //        // A and B collide
    //    end if

    return (Math.abs(getCenterX() - b.getCenterX()) < Math.abs(getWidth() + b.getWidth()) / 2) &&
     (Math.abs(getCenterY() - b.getCenterY()) < Math.abs(getHeight() + b.getHeight()) / 2);
  }

  @SuppressWarnings({ "unused", "static-method" })
  public boolean intersectsLine(final double x1, final double y1, final double x2, final double y2)
  {
    throw new NotImplementedException("intersectsLine() not implemented");
  }

  public boolean containsPoint(final double x, final double y)
  {
    return (x >= getMinX() && x <= getMaxX() && y >= getMinY() && y <= getMaxY());
  }
  
  public boolean contains(Bounds b)
  {
    return contains(b, true);
  }
  
  public boolean contains(final Bounds b, final boolean includeAdjacent)
  {
    if (includeAdjacent)
    {
      return (b.minX >= minX && b.minY >= minY && b.maxX <= maxX && b.maxY <= maxY);
    }
    return (b.minX > minX && b.minY > minY && b.maxX < maxX && b.maxY < maxY);
  }

  /**
   * Return the intersection of this bounds with b.
   */
  public Bounds intersection(Bounds b)
  {
    return new Bounds(Math.max(minX, b.minX), Math.max(minY, b.minY), Math.min(maxX, b.maxX),
        Math.min(maxY, b.maxY));
  }

  public void setMaxX(double maxX)
  {
    individualParamSet |= 1;
    set = set | individualParamSet == 15;

    this.maxX = maxX;
  }

  public void setMaxY(double maxY)
  {
    individualParamSet |= 2;
    set = set | individualParamSet == 15;

    this.maxY = maxY;
  }

  public void setMinX(double minX)
  {
    individualParamSet |= 4;
    set = set | individualParamSet == 15;

    this.minX = minX;
  }

  public void setMinY(double minY)
  {
    individualParamSet |= 8;
    set = set | individualParamSet == 15;

    this.minY = minY;
  }

  public Envelope toEnvelope()
  {
    return new Envelope(minX, maxX, minY, maxY);
  }

  public Rectangle2D.Double toRectangle2D()
  {
    return new Rectangle2D.Double(minX, minY, getWidth(), getHeight());
  }

  @Override
  public String toString()
  {
    return "Bounds: lon/lat" + "(" + minX + "," + minY + ") (" + maxX + "," + maxY + ")";
  }

  @Override
  public int hashCode()
  {
    return super.hashCode();
  }

  public static Bounds fromDelimitedString(final String rect)
  {
    String[] args = rect.split(",");
    if (args.length != 4)
    {
      throw new IllegalArgumentException("Delimited Bounds should be in the format of \"minx,miny,maxx,maxy\" (delimited by \",\") ");
    }
    
    return new Bounds(Double.parseDouble(args[0]), Double.parseDouble(args[1]),Double.parseDouble(args[2]),Double.parseDouble(args[3]));
  }

  public String toDelimitedString()
  {
    // Don't use String.format - it's slow and loses precision apparently
    return "" + Double.valueOf(minX) + "," + Double.valueOf(minY) + "," + Double.valueOf(maxX) + "," + Double.valueOf(maxY);
  }

@Override
public void writeExternal(ObjectOutput out) throws IOException
{
  out.writeDouble(minX);
  out.writeDouble(minY);
  out.writeDouble(maxX);
  out.writeDouble(maxY);
}

@Override
public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
{
  minX = in.readDouble();
  minY = in.readDouble();
  maxX = in.readDouble();
  maxY = in.readDouble();
}
}