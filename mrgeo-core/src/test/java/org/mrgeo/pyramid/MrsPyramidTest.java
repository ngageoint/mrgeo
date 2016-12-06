/*
 * Copyright 2009-2016 DigitalGlobe, Inc.
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
 *
 */

package org.mrgeo.pyramid;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.core.Defs;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.image.MrsPyramid;
import org.mrgeo.image.MrsPyramidMetadata;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.utils.tms.Bounds;

import java.io.IOException;

@SuppressWarnings("all") // test code, not included in production
public class MrsPyramidTest
{
final String smallElevation = Defs.CWD + "/" + Defs.INPUT + "small-elevation-nopyramids";

MrsPyramid noPyramids = null;
MrsPyramidMetadata noPyramidsMeta = null;
ProviderProperties providerProperties;

private static boolean comparePyramids(final MrsPyramid expected, final MrsPyramid actual)
    throws IOException
{

  // compare pyramids by comparing metadata
  MrsPyramidMetadata m1 = expected.getMetadata();
  MrsPyramidMetadata m2 = actual.getMetadata();

  Assert.assertEquals("Pyramid names are different", m1.getPyramid(), m2.getPyramid());

  Assert.assertEquals("Max zoom levels are different", m1.getMaxZoomLevel(), m1.getMaxZoomLevel());

  Assert.assertEquals("Number of bands are different", m1.getBands(), m2.getBands());
  Assert.assertEquals("Bounds are different", m1.getBounds(), m2.getBounds());
  Assert.assertEquals("Tilesizes are different", m1.getTilesize(), m2.getTilesize());
  Assert.assertEquals("Tile types are different", m1.getTileType(), m2.getTileType());

  double[] d1 = m1.getDefaultValues();
  double[] d2 = m2.getDefaultValues();
  for (int i = 0; i < d1.length; i++)
  {
    Assert.assertEquals("Default values are different", d1[i], d2[i], 0.000001);
  }


  for (int i = 0; i < m1.getMaxZoomLevel(); i++)
  {
    Assert.assertEquals("Image names are different", m1.getName(i), m2.getName(i));
    Assert.assertEquals("Pixel bounds are different", m1.getPixelBounds(i), m2.getPixelBounds(i));
    Assert.assertEquals("Tile bounds are different", m1.getTileBounds(i), m2.getTileBounds(i));
  }

  return true;
}

@Before
public void setUp() throws Exception
{
  providerProperties = null;
  noPyramids = MrsPyramid.open(smallElevation, providerProperties);
  noPyramidsMeta = noPyramids.getMetadata();
}

@After
public void tearDown()
{
}

@Test
@Category(UnitTest.class)
public void openPyramid() throws Exception
{
  MrsPyramid p = MrsPyramid.open(smallElevation, providerProperties);
  comparePyramids(noPyramids, p);
}

@Test
@Category(UnitTest.class)
public void loadPyramid() throws Exception
{
  MrsPyramid p = MrsPyramid.loadPyramid(smallElevation, providerProperties);
  comparePyramids(noPyramids, p);
}

@Test(expected = IOException.class)
@Category(UnitTest.class)
public void missingPyramid() throws Exception
{
  // a bad pyramid
  MrsPyramid.open("/bad/path/to/file", providerProperties);
}

//  @Test
//  public void getImage() throws IOException
//  {
//    int level = noPyramidsMeta.getMaxZoomLevel();
//    
//    noPyramids.getImage(level);
//  }

@Test
@Category(UnitTest.class)
public void hasPyramids() throws Exception
{
  Assert.assertEquals("MrsPyramid should not have pyramids built", false, noPyramids.hasPyramids());
}

@Test
@Category(UnitTest.class)
public void getBounds()
{
  Bounds b1 = noPyramids.getBounds();
  Bounds b2 = noPyramidsMeta.getBounds();

  Assert.assertEquals("Mrs Pyramid bounds do not match those from the metadata", b2, b1);
}

@Test
@Category(UnitTest.class)
public void getNumLevels()
{
  Assert.assertEquals("Number of levels does not match the metadata", noPyramidsMeta.getMaxZoomLevel(),
      noPyramids.getNumLevels());
}

//  @Test
//  public ImageStats getStats()
//  {
//    return null;
//  }
//  
//  @Test
//  public String getScaleType()
//  {
//    return null;
//  }
//    
//  @Test
//  public MrsImagePyramidMetadata getMetadata()
//  {
//    return metadata;
//  }

@Test
@Category(UnitTest.class)
public void getMaximumLevel()
{
  Assert.assertEquals("Maximum zoom level does not match the metadata", noPyramidsMeta.getMaxZoomLevel(),
      noPyramids.getMaximumLevel());
}
}
