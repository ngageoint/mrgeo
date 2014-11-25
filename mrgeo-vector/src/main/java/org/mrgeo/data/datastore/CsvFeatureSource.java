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
