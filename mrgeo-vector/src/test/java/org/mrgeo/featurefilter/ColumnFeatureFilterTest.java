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
