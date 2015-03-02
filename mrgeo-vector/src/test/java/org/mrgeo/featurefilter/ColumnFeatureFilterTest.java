package org.mrgeo.featurefilter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.io.LongWritable;
import org.mrgeo.data.CloseableKVIterator;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.vector.VectorDataProvider;
import org.mrgeo.data.vector.VectorReader;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.test.TestUtils;

public abstract class ColumnFeatureFilterTest
{
  protected static List<Geometry> readFeatures(String fileName) throws IOException,
    InterruptedException
  {
    String testDir = TestUtils.composeInputDir(ColumnFeatureFilterTest.class);
    File testFile = new File(testDir, fileName);
    String resolvedFileName = testFile.toURI().toString();
    VectorDataProvider vdp = DataProviderFactory.getVectorDataProvider(resolvedFileName,
        AccessMode.READ, new Properties());
    VectorReader reader = vdp.getVectorReader();
    CloseableKVIterator<LongWritable, Geometry> iter = reader.get();
    try
    {
      List<Geometry> features = new ArrayList<>();
      while (iter.hasNext())
      {
        Geometry geom = iter.next();
        if (geom != null)
        {
          features.add(geom.createWritableClone());
        }
      }
      return features;
    }
    finally
    {
      iter.close();
    }
  } 
}
