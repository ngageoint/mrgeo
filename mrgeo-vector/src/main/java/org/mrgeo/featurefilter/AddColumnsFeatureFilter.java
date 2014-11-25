/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.featurefilter;

import org.mrgeo.geometry.Geometry;
import org.mrgeo.geometry.WritableGeometry;

import java.util.Collection;
import java.util.List;

public class AddColumnsFeatureFilter extends BaseFeatureFilter
{
  private static final long serialVersionUID = 1L;
  Collection<String> columns;




  /**
   * Use this constructor if the new columns are all the same type and their
   * ordering is not important.
   * 
   * @param columns
   */
  public AddColumnsFeatureFilter(Collection<String> columns)
  {
    this.columns = columns;
  }

  @Override
  public Geometry filterInPlace(Geometry input)
  {

    WritableGeometry result = input.createWritableClone();

    for (String attr: columns)
    {
      result.setAttribute(attr, null);
    }

    return result;
  }
}
