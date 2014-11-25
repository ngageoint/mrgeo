package org.mrgeo.data;

import org.mrgeo.geometry.Geometry;

import java.io.Serializable;

@Deprecated
public interface FeatureProvider extends Iterable<Geometry>, Serializable
{
  @Override
  FeatureIterator iterator();
}
