package org.mrgeo.data.geowave.vector;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.vector.VectorDataProvider;
import org.mrgeo.data.vector.VectorMetadata;
import org.mrgeo.data.vector.VectorMetadataReader;

public class GeoWaveVectorDataProviderTest
{
  @Test
  public void testGetMetadataReader() throws IOException
  {
    VectorDataProvider vdp = DataProviderFactory.getVectorDataProvider("geowave:Af_Clip", AccessMode.READ);
    Assert.assertNotNull(vdp);
    VectorMetadataReader reader = vdp.getMetadataReader();
    Assert.assertNotNull(reader);
    VectorMetadata metadata = reader.read();
    Assert.assertNotNull(metadata);
  }
}
