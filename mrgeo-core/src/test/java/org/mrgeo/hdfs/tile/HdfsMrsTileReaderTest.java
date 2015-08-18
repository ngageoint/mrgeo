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

package org.mrgeo.hdfs.tile;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.core.Defs;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.data.tile.MrsTileReader;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.junit.UnitTest;

import java.awt.image.Raster;
import java.io.IOException;


public class HdfsMrsTileReaderTest
{
  String image = Defs.CWD + "/" + Defs.INPUT + "all-ones";
  MrsTileReader<Raster> reader;
  private ProviderProperties providerProperties;

  @Before
  public void setUp() throws IOException
  {
    providerProperties = null;
    MrsImageDataProvider provider = DataProviderFactory.getMrsImageDataProvider(image,
        AccessMode.READ, providerProperties);
    reader = provider.getMrsTileReader(10);
  }

  @After
  public void tearDown()
  {
    reader.close();
  }

  @Test
  @Category(UnitTest.class)
  public void readFull()
  {
    int numEntries = read(208787, 211861);
    Assert.assertEquals(12, numEntries);
  }
  
  @Test
  @Category(UnitTest.class)
  public void readNonExistentStartEnd()
  {
    int numEntries = read(0, 209810);
    Assert.assertEquals(3, numEntries);		  
  }
  
  @Test
  @Category(UnitTest.class)
  public void readSkipPartition()
  {
    int numEntries = read(210838, Long.MAX_VALUE);
    Assert.assertEquals(3, numEntries);		  
  }
  
  @Test
  @Category(UnitTest.class)
  public void readWithNoRangeSpecified()
  {
    int numEntries = read(Long.MIN_VALUE, Long.MAX_VALUE);
    Assert.assertEquals(12, numEntries);		  
  }
  
  private int read(long start, long end) {
    TileIdWritable startKey = new TileIdWritable(start);
    TileIdWritable endKey = new TileIdWritable(end);
    int numEntries = 0;
    
    org.mrgeo.data.KVIterator<TileIdWritable, Raster> iter = reader.get(startKey, endKey);
        
    while (iter.hasNext())
    {
      numEntries++;
      iter.next();
    }
    
    return numEntries;
  }
}

