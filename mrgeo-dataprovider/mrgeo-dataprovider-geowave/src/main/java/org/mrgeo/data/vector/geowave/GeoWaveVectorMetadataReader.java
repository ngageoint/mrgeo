package org.mrgeo.data.vector.geowave;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.store.adapter.DataAdapter;
import mil.nga.giat.geowave.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.vector.adapter.FeatureDataAdapter;
import mil.nga.giat.geowave.vector.stats.FeatureBoundingBoxStatistics;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.hadoop.io.LongWritable;
import org.mrgeo.data.CloseableKVIterator;
import org.mrgeo.data.vector.VectorMetadata;
import org.mrgeo.data.vector.VectorMetadataReader;
import org.mrgeo.data.vector.VectorReader;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.utils.Bounds;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class GeoWaveVectorMetadataReader implements VectorMetadataReader
{
  private static Logger log = LoggerFactory.getLogger(VectorMetadataReader.class);
  private VectorMetadata metadata;
  private GeoWaveVectorDataProvider dataProvider;
  private FeatureDataAdapter adapter;

  public GeoWaveVectorMetadataReader(GeoWaveVectorDataProvider provider)
  {
    this.dataProvider = provider;
  }

  @Override
  public VectorMetadata read() throws IOException
  {
    if (dataProvider == null)
    {
      throw new IOException("DataProvider not set!");
    }

    String name = dataProvider.getResourceName();
    if (name == null || name.length() == 0)
    {
      throw new IOException("Can not load metadata, resource name is empty!");
    }

    if (metadata == null)
    {
      try
      {
        metadata = loadMetadata();
      }
      catch (AccumuloSecurityException e)
      {
        throw new IOException(e);
      }
      catch (AccumuloException e)
      {
        throw new IOException(e);
      }
    }

    return metadata;
  }

  @Override
  public VectorMetadata reload() throws IOException
  {
    // TODO Auto-generated method stub.
    // For MrsImage, this method was used to keep existing instances of 
    // metadata updated with the current values after a new metadata is
    // written. There might be an easier way to accomplish that? The writer
    // could pass its metadata instance to the reader as an argument to
    // reload, and it could copy the fields directly - it should be a
    // shallow clone.
    return null;
  }

  private VectorMetadata loadMetadata() throws AccumuloSecurityException, AccumuloException, IOException
  {
    VectorMetadata metadata = new VectorMetadata();
    if (adapter == null)
    {
      DataAdapter<?> localAdapter = dataProvider.getDataAdapter();
      adapter = (FeatureDataAdapter)localAdapter;
    }
    SimpleFeatureType sft = adapter.getType();
    for (int index=0; index < sft.getAttributeCount(); index++)
    {
      AttributeDescriptor desc = sft.getDescriptor(index);
      metadata.addAttribute(desc.getName().getLocalPart());
    }
    FeatureBoundingBoxStatistics boundsStats = null;
    String cqlFilter = dataProvider.getCqlFilter();
    // We only want to try to read the bounds stats if there is no CQL filter because
    // if there is a filter, the overall bounds for the data source will not be correct.
    if (cqlFilter == null || cqlFilter.isEmpty())
    {
      DataStatistics<?> stats = GeoWaveVectorDataProvider.getStatisticsStore().getDataStatistics(
              new ByteArrayId(dataProvider.getGeoWaveResourceName()),
              FeatureBoundingBoxStatistics.composeId("the_geom"));
      boundsStats = (FeatureBoundingBoxStatistics) stats;
    }
    Bounds bounds = null;
    if(boundsStats != null)
    {
    	bounds = new Bounds(boundsStats.getMinX(), boundsStats.getMinY(),
    			boundsStats.getMaxX(), boundsStats.getMaxY());
      log.info("Bounds for " + dataProvider.getGeoWaveResourceName() + " from GeoWave: " + bounds.toString());
    }
    else
    {
      VectorReader reader = dataProvider.getVectorReader();
      CloseableKVIterator<LongWritable, Geometry> iter = reader.get();
      double minX = Double.MAX_VALUE;
      double minY = Double.MAX_VALUE;
      double maxX = Double.MIN_VALUE;
      double maxY = Double.MIN_VALUE;
      while (iter.hasNext())
      {
        Geometry geom = iter.next();
        Bounds b = geom.getBounds();
        minX = Math.min(minX, b.getMinX());
        minY = Math.min(minY, b.getMinY());
        maxX = Math.max(maxX, b.getMaxX());
        maxY = Math.max(maxY, b.getMaxY());
      }
		  bounds = new Bounds(minX, minY, maxX, maxY);
      log.info("Bounds for " + dataProvider.getGeoWaveResourceName() + " were computed as: " + bounds.toString());
    }
    metadata.setBounds(bounds);
    return metadata;
  }
}
