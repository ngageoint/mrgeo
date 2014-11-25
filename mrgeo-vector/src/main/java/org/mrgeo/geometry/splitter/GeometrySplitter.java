package org.mrgeo.geometry.splitter;

import org.mrgeo.geometry.Geometry;

import java.util.List;
import java.util.Map;

/**
 * This interface is used when we want to split Geometry objects
 * into categories. A specific use case is when we want to split
 * Geometry objects across multiple output directories based on
 * some criteria within the Geometry itself.
 * 
 * Implementors of this interface will be configured with enough
 * information to perform a split on any given Geometry and return
 * a list of categories to split it into.
 */
public interface GeometrySplitter
{
  public void initialize(Map<String, String> splitterProperties, final boolean uuidOutputNames,
      final String[] outputNames);
  public String[] getAllSplits();
  public List<String> splitGeometry(final Geometry geometry);
}
