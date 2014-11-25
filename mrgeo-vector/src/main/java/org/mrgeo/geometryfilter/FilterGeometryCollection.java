/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.geometryfilter;

import org.mrgeo.geometry.Geometry;
import org.mrgeo.geometry.WritableGeometry;
import org.mrgeo.data.GeometryCollection;

import java.util.Iterator;

public class FilterGeometryCollection implements GeometryCollection
{
  static class LocalIterator implements Iterator<WritableGeometry>
  {
    private int currentIndex = 0;
    private final FilterGeometryCollection parent;

    public LocalIterator(final FilterGeometryCollection parent)
    {
      this.parent = parent;
    }

    @Override
    public boolean hasNext()
    {
      return currentIndex < parent.size();
    }

    @Override
    public WritableGeometry next()
    {
      return parent.get(currentIndex++);
    }

    @Override
    public void remove()
    {
      throw new UnsupportedOperationException();
    }
  }

  private static final long serialVersionUID = 1L;

  private final GeometryFilter filter;
  private final GeometryCollection src;

  public FilterGeometryCollection(final GeometryCollection src, final GeometryFilter filter)
  {
    this.src = src;
    this.filter = filter;
  }

  @Override
  public void close()
  {
    if (src != null)
    {
      src.close();
    }
  }

  @Override
  public WritableGeometry get(final int index)
  {
    return filter.filterInPlace(src.get(index));
  }

  @Override
  public String getProjection()
  {
    return src.getProjection();
  }

  @Override
  public Iterator<WritableGeometry> iterator()
  {
    return new LocalIterator(this);
  }

  @Override
  public int size()
  {
    return src.size();
  }
}
