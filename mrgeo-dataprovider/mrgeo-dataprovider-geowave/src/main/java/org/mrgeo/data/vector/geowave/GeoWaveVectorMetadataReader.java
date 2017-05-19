package org.mrgeo.data.vector.geowave;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.stats.FeatureBoundingBoxStatistics;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.mrgeo.data.CloseableKVIterator;
import org.mrgeo.data.vector.FeatureIdWritable;
import org.mrgeo.data.vector.VectorMetadata;
import org.mrgeo.data.vector.VectorMetadataReader;
import org.mrgeo.data.vector.VectorReader;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.utils.tms.Bounds;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class GeoWaveVectorMetadataReader implements VectorMetadataReader
{
private static Logger log = LoggerFactory.getLogger(GeoWaveVectorMetadataReader.class);
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
    adapter = (FeatureDataAdapter) localAdapter;
  }
  SimpleFeatureType sft = adapter.getType();
  for (int index = 0; index < sft.getAttributeCount(); index++)
  {
    AttributeDescriptor desc = sft.getDescriptor(index);
    metadata.addAttribute(desc.getName().getLocalPart());
  }
  FeatureBoundingBoxStatistics boundsStats = null;
  String geometryField = sft.getGeometryDescriptor().getLocalName();
  log.info("GeoWave geometry field is: " + geometryField);
  DataStatistics<?> stats = dataProvider.getStatisticsStore().getDataStatistics(
      new ByteArrayId(dataProvider.getGeoWaveResourceName()),
      FeatureBoundingBoxStatistics.composeId(geometryField));
  boundsStats = (FeatureBoundingBoxStatistics) stats;
  Bounds bounds = null;
  if (boundsStats != null)
  {
    bounds = new Bounds(boundsStats.getMinX(), boundsStats.getMinY(),
        boundsStats.getMaxX(), boundsStats.getMaxY());
    log.info("Bounds for " + dataProvider.getGeoWaveResourceName() + " from GeoWave: " + bounds.toString());
  }
  else
  {
    log.info("BBOX information was not found in GeoWave for field " + geometryField);
    // See if the GeoWave data provider is configured to force a bounding
    // box computation by iterating features.
    if (GeoWaveVectorDataProvider.getConnectionInfo().getForceBboxCompute())
    {
      log.info("Computing BBOX by iterating features");
      VectorReader reader = dataProvider.getVectorReader();
      CloseableKVIterator<FeatureIdWritable, Geometry> iter = reader.get();
      double minX = Double.MAX_VALUE;
      double minY = Double.MAX_VALUE;
      double maxX = Double.MIN_VALUE;
      double maxY = Double.MIN_VALUE;
      while (iter.hasNext())
      {
        Geometry geom = iter.next();
        Bounds b = geom.getBounds();
        minX = Math.min(minX, b.w);
        minY = Math.min(minY, b.s);
        maxX = Math.max(maxX, b.e);
        maxY = Math.max(maxY, b.n);
      }
      bounds = new Bounds(minX, minY, maxX, maxY);
      log.info("Bounds for " + dataProvider.getGeoWaveResourceName() + " were computed as: " + bounds.toString());
    }
    else
    {
      // Use world bounds, but log a warning
      bounds = Bounds.WORLD.clone();
      log.warn("Using world bounds for " + dataProvider.getGeoWaveResourceName() + ": " + bounds.toString());
    }
  }
  metadata.setBounds(bounds);
  return metadata;
}
}
