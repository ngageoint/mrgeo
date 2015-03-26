package org.mrgeo.data.vector.geowave;

import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import mil.nga.giat.geowave.store.CloseableIterator;
import mil.nga.giat.geowave.store.adapter.DataAdapter;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.query.Query;
import mil.nga.giat.geowave.store.query.SpatialQuery;
import mil.nga.giat.geowave.store.query.TemporalConstraints;
import mil.nga.giat.geowave.store.query.TemporalQuery;
import mil.nga.giat.geowave.vector.VectorDataStore;
import mil.nga.giat.geowave.vector.adapter.FeatureDataAdapter;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.hadoop.io.LongWritable;
import org.geotools.factory.CommonFactoryFinder;
import org.mrgeo.data.CloseableKVIterator;
import org.mrgeo.data.vector.VectorReader;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.utils.Bounds;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory;
import org.opengis.filter.identity.FeatureId;

//import com.vividsolutions.jts.geom.Geometry;
//import com.vividsolutions.jts.geom.GeometryFactory;

public class GeoWaveVectorReader implements VectorReader
{
  private VectorDataStore dataStore;
  private DataAdapter<?> adapter;
  private Index index;
  private Properties providerProperties;

  public GeoWaveVectorReader(VectorDataStore dataStore, DataAdapter<?> adapter,
      Index index, Properties providerProperties)
  {
    this.dataStore = dataStore;
    this.adapter = adapter;
    this.index = index;
    this.providerProperties = providerProperties;
  }

  @Override
  public void close()
  {
  }

  @Override
  public CloseableKVIterator<LongWritable, Geometry> get()
  {
    CloseableIterator<?> iter = dataStore.query(adapter, null);
    return new GeoWaveVectorIterator(iter);
  }

  @Override
  public boolean exists(LongWritable featureId)
  {
    return (get(featureId) != null);
  }

  @Override
  public Geometry get(LongWritable featureId)
  {
    FilterFactory ff = CommonFactoryFinder.getFilterFactory();
    Set<FeatureId> ids = new HashSet<FeatureId>();
    ids.add(ff.featureId(adapter.getAdapterId().getString() + "." + featureId));
    Filter filter = ff.id(ids);
    Query query = new TemporalQuery(new TemporalConstraints());
    Integer limit = null; // no limit
    CloseableIterator<?> iter = dataStore.query((FeatureDataAdapter)adapter, index,
        query, filter, limit);
    if (iter.hasNext())
    {
      Object value = iter.next();
      if (value instanceof SimpleFeature)
      {
        SimpleFeature sf = (SimpleFeature)value;
        return GeoWaveVectorIterator.convertToGeometry(sf);
      }
      throw new IllegalArgumentException("Expected SimpleFeature, but got " + value.getClass().getName());
    }
    return null;
  }

  @Override
  public CloseableKVIterator<LongWritable, Geometry> get(Bounds bounds)
  {
    com.vividsolutions.jts.geom.GeometryFactory gf = new com.vividsolutions.jts.geom.GeometryFactory();
    com.vividsolutions.jts.geom.Geometry queryGeometry = gf.toGeometry(bounds.toEnvelope());
    Query query = new SpatialQuery(queryGeometry);
    CloseableIterator<?> iter = dataStore.query(adapter, index, query);
    return new GeoWaveVectorIterator(iter);
  }

  @Override
  public long count() throws IOException
  {
    try
    {
      return GeoWaveVectorDataProvider.getAdapterCount(adapter.getAdapterId(),
          providerProperties);
    }
    catch(AccumuloSecurityException e)
    {
      throw new IOException(e);
    }
    catch(AccumuloException e)
    {
      throw new IOException(e);
    }
  }
}
