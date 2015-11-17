package org.mrgeo.data.vector.geowave;

import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.geotools.factory.CommonFactoryFinder;
import org.mrgeo.data.CloseableKVIterator;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.vector.FeatureIdWritable;
import org.mrgeo.data.vector.VectorReader;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.utils.Bounds;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory;
import org.opengis.filter.identity.FeatureId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class GeoWaveVectorReader implements VectorReader
{
  static final Logger log = LoggerFactory.getLogger(GeoWaveVectorReader.class);

  private String namespace;
  private DataStore dataStore;
  private DataAdapter<?> adapter;
  private Query query;
  private PrimaryIndex index;
  private Filter filter;
  private ProviderProperties providerProperties;

  public GeoWaveVectorReader(String namespace, DataStore dataStore, DataAdapter<?> adapter,
      Query query, PrimaryIndex index, Filter filter, ProviderProperties providerProperties)
  {
    this.namespace = namespace;
    this.dataStore = dataStore;
    this.adapter = adapter;
    this.query = query;
    this.index = index;
    this.filter = filter;
    this.providerProperties = providerProperties;
  }

  @Override
  public void close()
  {
  }

  @Override
  public CloseableKVIterator<FeatureIdWritable, Geometry> get()
  {
    Integer limit = null; // no limit
    QueryOptions queryOptions = new QueryOptions(adapter, index);
    queryOptions.setLimit(limit);
    CloseableIterator<?> iter = dataStore.query(queryOptions, query);
    return new GeoWaveVectorIterator(iter);
  }

  @Override
  public boolean exists(FeatureIdWritable featureId)
  {
    return (get(featureId) != null);
  }

  @Override
  public Geometry get(FeatureIdWritable featureId)
  {
    FilterFactory ff = CommonFactoryFinder.getFilterFactory();
    Set<FeatureId> ids = new HashSet<FeatureId>();
    ids.add(ff.featureId(adapter.getAdapterId().getString() + "." + featureId));
    Filter idFilter = ff.id(ids);
    Filter queryFilter = ff.and(idFilter, this.filter);
    Integer limit = null; // no limit
    QueryOptions queryOptions = new QueryOptions(adapter, index);
    queryOptions.setLimit(limit);
    CloseableIterator<?> iter = dataStore.query(queryOptions, query);
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
  public CloseableKVIterator<FeatureIdWritable, Geometry> get(Bounds bounds)
  {
    com.vividsolutions.jts.geom.GeometryFactory gf = new com.vividsolutions.jts.geom.GeometryFactory();
    com.vividsolutions.jts.geom.Geometry queryGeometry = gf.toGeometry(bounds.toEnvelope());
    Query query = new SpatialQuery(queryGeometry);
    Integer limit = null; // no limit
    QueryOptions queryOptions = new QueryOptions(adapter, index);
    queryOptions.setLimit(limit);
    CloseableIterator<?> iter = dataStore.query(queryOptions, query);
    return new GeoWaveVectorIterator(iter);
  }

  @Override
  public long count() throws IOException
  {
    try
    {
      if (filter == null)
      {
        return GeoWaveVectorDataProvider.getAdapterCount(adapter.getAdapterId(),
                                                         namespace,
                                                         providerProperties);
      }
      else
      {
        // We must iterate through the returned features to get the count
        long count = 0L;
        Integer limit = null; // no limit
        QueryOptions queryOptions = new QueryOptions(adapter, index);
        queryOptions.setLimit(limit);
        CloseableIterator<?> iter = dataStore.query(queryOptions, query);
        while (iter.hasNext())
        {
          count++;
          iter.next();
        }
        return count;
      }
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
