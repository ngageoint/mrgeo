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

package org.mrgeo.data.datastore;

import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.mrgeo.data.csv.CsvGeometryInputStream;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.IOException;

@SuppressWarnings("unchecked")
public class CsvFeatureSource extends ContentFeatureSource
{
  CsvGeometryInputStream stream;

  public CsvFeatureSource(ContentEntry entry, CsvGeometryInputStream stream)
  {
    super(entry, Query.ALL);
    this.stream = stream;
  }

  @Override
  @SuppressWarnings("hiding")
  protected ReferencedEnvelope getBoundsInternal(Query query) throws IOException
  {
//    if (query.getFilter() != Filter.INCLUDE) 
//    {
//      return null;
//    }
    return null;
  }

  @Override
  protected boolean canFilter()
  {
    return false;
  }

  @Override
  protected int getCountInternal(Query query) throws IOException
  {
    return 0;
  }

  @Override
  @SuppressWarnings("hiding")
  protected FeatureReader<SimpleFeatureType, SimpleFeature> getReaderInternal(Query query)
      throws IOException
  {
    return new CsvFeatureReader(stream);
  }

  @Override
  protected SimpleFeatureType buildFeatureType() throws IOException
  {
    return (new CsvFeatureReader(stream)).featureType();
  }

}
