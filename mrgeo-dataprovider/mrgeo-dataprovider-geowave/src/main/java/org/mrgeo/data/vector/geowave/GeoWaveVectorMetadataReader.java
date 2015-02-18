package org.mrgeo.data.vector.geowave;

import java.io.IOException;

import mil.nga.giat.geowave.store.adapter.DataAdapter;
import mil.nga.giat.geowave.store.adapter.statistics.BoundingBoxDataStatistics;
import mil.nga.giat.geowave.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.vector.adapter.FeatureDataAdapter;
import mil.nga.giat.geowave.vector.stats.FeatureBoundingBoxStatistics;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.mrgeo.data.vector.VectorMetadata;
import org.mrgeo.data.vector.VectorMetadataReader;
import org.mrgeo.utils.Bounds;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

public class GeoWaveVectorMetadataReader implements VectorMetadataReader
{
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
    // TODO: The following gives back bad bounds for the adapter. I have a question
    // in to Rich to determine what I'm doing wrong. In the meantime, do not set
    // the bounds in the metadata. This will force a re-computation of the bounds
    // the hard way (in the caller).
    DataStatistics<SimpleFeature> stats = adapter.createDataStatistics(BoundingBoxDataStatistics.STATS_ID);
    FeatureBoundingBoxStatistics boundsStats = (FeatureBoundingBoxStatistics)stats;
    Bounds bounds = new Bounds(boundsStats.getMinX(), boundsStats.getMinY(),
        boundsStats.getMaxX(), boundsStats.getMaxY());
//    metadata.setBounds(bounds);
    return metadata;
  }
}
