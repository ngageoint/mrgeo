package org.mrgeo.featurefilter;

import org.mrgeo.data.FeatureIterator;
import org.mrgeo.geometry.Geometry;

import java.util.NoSuchElementException;

/**
 * Provides a slightly simpler version of the iterator interface.
 */
public abstract class SimplifiedFeatureIterator implements FeatureIterator
{
  Geometry current = null;
  boolean primed;

  @Override
  public boolean hasNext()
  {
    if (current == null)
    {
      current = simpleNext();
    }
    return current != null;
  }

  @Override
  public Geometry next()
  {
    if (hasNext())
    {
      Geometry result = current;
      current = null;
      return result;
    }
    throw new NoSuchElementException();
  }

  /**
   * Returns either the next feature or null if there are not features
   * remaining. This is a bit simpler to implement than the full Iterator
   * interface.
   * 
   * @return
   */
  public abstract Geometry simpleNext();

  @Override
  public void remove()
  {
    throw new UnsupportedOperationException();
  }
}
