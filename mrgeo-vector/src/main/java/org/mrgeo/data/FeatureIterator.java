package org.mrgeo.data;

import org.mrgeo.geometry.Geometry;

import java.io.IOException;
import java.util.Iterator;

/**
 * The features returned by this iterator are owned by the caller. This means
 * they may be modified w/o modifying the underlying data. If necessary the
 * implementor of this interface should call clone before returning the result.
 */
@Deprecated
public interface FeatureIterator extends Iterator<Geometry>
{

  public void close() throws IOException;
}
