/*
 * Copyright 2009-2014 DigitalGlobe, Inc.
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

package org.mrgeo.image;

import org.codehaus.jackson.map.exc.UnrecognizedPropertyException;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.core.Defs;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.image.MrsImagePyramidMetadata.Classification;
import org.mrgeo.junit.UnitTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

import static org.junit.Assert.*;

@SuppressWarnings("static-method")
public class MrsImagePyramidMetadataTest
{
  private static final Logger log = LoggerFactory.getLogger(MrsImagePyramidMetadataTest.class);
  final double epsilon = 0.00000001;

  
  private static String allonesName = "all-ones/" ;
  private static String allOnes = Defs.INPUT + allonesName;

  @BeforeClass
  public static void init() throws Exception
  {
    File file = new File(allOnes);
    allOnes = "file://" + file.getAbsolutePath();
  }

  @Test
  @Category(UnitTest.class)
  public void testDeserializeMissingStats()
  {
    final String json = "{\"bounds\":{\"maxY\":41.5,\"maxX\":25,\"minX\":24,\"minY\":40.5},\"imageMetadata\":[{\"pixelBounds\":{\"maxY\":728,\"maxX\":728,\"minX\":0,\"minY\":0},\"tileBounds\":{\"maxY\":187,\"maxX\":291,\"minX\":290,\"minY\":185},\"name\":\"9\"}],\"bands\":1,\"defaultValues\":[-32768],\"tilesize\":512,\"maxZoomLevel\":3}";
    try
    {
      final InputStream is = new ByteArrayInputStream(json.getBytes());
      final MrsImagePyramidMetadata meta = MrsImagePyramidMetadata.load(is);
      assertEquals(meta.getBounds().getMaxY(), 41.5, epsilon);
      assertEquals(meta.getBounds().getMaxX(), 25, epsilon);
      assertEquals(meta.getBounds().getMinX(), 24, epsilon);
      assertEquals(meta.getBounds().getMinY(), 40.5, epsilon);
      assertEquals(meta.getTilesize(), MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT_INT);
      assertEquals(meta.getBands(), 1);
      assertEquals(meta.getDefaultValues()[0], -32768, epsilon);
    }
    catch (final Exception e)
    {
      log.error("Failed to load metadata", e);
      fail("Failed to load metadata");
    }
  }

  @Test(expected = UnrecognizedPropertyException.class)
  @Category(UnitTest.class)
  public void testDeserializeUnknownProperty() throws IOException
  {
    final String json = "{\"missing\":null,\"bounds\":{\"maxY\":41.5,\"maxX\":25,\"minX\":24,\"minY\":40.5},\"imageMetadata\":[{\"pixelBounds\":{\"maxY\":728,\"maxX\":728,\"minX\":0,\"minY\":0},\"tileBounds\":{\"maxY\":187,\"maxX\":291,\"minX\":290,\"minY\":185},\"name\":\"9\"}],\"bands\":1,\"defaultValues\":[-32768],\"maxZoomLevel\":3}";

    final InputStream is = new ByteArrayInputStream(json.getBytes());
    MrsImagePyramidMetadata.load(is);
  }

  @Test
  @Category(UnitTest.class)
  public void testLoadClassification()
  {
    final String json = "{\"bounds\":{\"maxY\":41.5,\"maxX\":25,\"minX\":24,\"minY\":40.5},\"classification\":\"Categorical\",\"imageMetadata\":[{\"stats\":null,\"pixelBounds\":null,\"tileBounds\":null,\"name\":null},{\"stats\":null,\"pixelBounds\":null,\"tileBounds\":null,\"name\":null},{\"stats\":null,\"pixelBounds\":null,\"tileBounds\":null,\"name\":null},{\"stats\":null,\"pixelBounds\":null,\"tileBounds\":null,\"name\":null},{\"stats\":null,\"pixelBounds\":null,\"tileBounds\":null,\"name\":null},{\"stats\":null,\"pixelBounds\":null,\"tileBounds\":null,\"name\":null},{\"stats\":null,\"pixelBounds\":null,\"tileBounds\":null,\"name\":null},{\"stats\":null,\"pixelBounds\":null,\"tileBounds\":null,\"name\":null},{\"stats\":null,\"pixelBounds\":null,\"tileBounds\":null,\"name\":null},{\"stats\":[{\"min\":-5,\"max\":2193,\"mean\":521.1451838574833}],\"pixelBounds\":{\"maxY\":728,\"maxX\":728,\"minX\":0,\"minY\":0},\"tileBounds\":{\"maxY\":187,\"maxX\":291,\"minX\":290,\"minY\":185},\"name\":\"9\"}],\"bands\":1,\"defaultValues\":[-32768],\"stats\":[{\"min\":-5,\"max\":2193,\"mean\":521.1451838574833}],\"tilesize\":512,\"maxZoomLevel\":9}";
    try
    {
      final InputStream is = new ByteArrayInputStream(json.getBytes());
      final MrsImagePyramidMetadata meta = MrsImagePyramidMetadata.load(is);
      assertNotNull(meta.getClassification());
      assertEquals("Categorical", meta.getClassification().name());
    }
    catch (final Exception e)
    {
      log.error("Failed to load metadata", e);
      fail("Failed to load metadata");
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testLoadDefaultValues()
  {
    final String json = "{\"bands\":1,\"tileType\":2,\"defaultValues\" : [ \"NaN\" ]}";
    try
    {
      final InputStream is = new ByteArrayInputStream(json.getBytes());
      final MrsImagePyramidMetadata meta = MrsImagePyramidMetadata.load(is);
      assertNotNull(meta.getDefaultValue(0));
      assertEquals(Double.NaN, meta.getDefaultValue(0), epsilon);
      assertEquals(0, meta.getDefaultValueInt(0));
      assertEquals((int) Double.NaN, 0);
    }
    catch (final Exception e)
    {
      log.error("Failed to load metadata", e);
      fail("Failed to load metadata");
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testLoadStats()
  {
    final String json = "{\"bounds\":{\"maxY\":41.5,\"maxX\":25,\"minX\":24,\"minY\":40.5},\"imageMetadata\":[{\"stats\":null,\"pixelBounds\":null,\"tileBounds\":null,\"name\":null},{\"stats\":null,\"pixelBounds\":null,\"tileBounds\":null,\"name\":null},{\"stats\":null,\"pixelBounds\":null,\"tileBounds\":null,\"name\":null},{\"stats\":null,\"pixelBounds\":null,\"tileBounds\":null,\"name\":null},{\"stats\":null,\"pixelBounds\":null,\"tileBounds\":null,\"name\":null},{\"stats\":null,\"pixelBounds\":null,\"tileBounds\":null,\"name\":null},{\"stats\":null,\"pixelBounds\":null,\"tileBounds\":null,\"name\":null},{\"stats\":null,\"pixelBounds\":null,\"tileBounds\":null,\"name\":null},{\"stats\":null,\"pixelBounds\":null,\"tileBounds\":null,\"name\":null},{\"stats\":[{\"min\":-5,\"max\":2193,\"mean\":521.1451838574833}],\"pixelBounds\":{\"maxY\":728,\"maxX\":728,\"minX\":0,\"minY\":0},\"tileBounds\":{\"maxY\":187,\"maxX\":291,\"minX\":290,\"minY\":185},\"name\":\"9\"}],\"bands\":1,\"defaultValues\":[-32768],\"stats\":[{\"min\":-5,\"max\":2193,\"mean\":521.1451838574833}],\"tilesize\":512,\"maxZoomLevel\":9}";
    try
    {
      final InputStream is = new ByteArrayInputStream(json.getBytes());
      final MrsImagePyramidMetadata meta = MrsImagePyramidMetadata.load(is);
      assertNotNull(meta.getStats());
      assertNotNull(meta.getStats(0));
    }
    catch (final Exception e)
    {
      log.error("Failed to load metadata", e);
      fail("Failed to load metadata");
    }
  }

  // Verify that we can load older format metadata that uses the "image" property
  // rather than "name". This was in place prior to changes for MrsVector support.
  @Test
  @Category(UnitTest.class)
  public void testLoadWithCompatibility()
  {
    final String json = "{\"bounds\":{\"maxY\":41.5,\"maxX\":25,\"minX\":24,\"minY\":40.5},\"imageMetadata\":[{\"stats\":null,\"pixelBounds\":null,\"tileBounds\":null,\"image\":null},{\"stats\":null,\"pixelBounds\":null,\"tileBounds\":null,\"image\":null},{\"stats\":null,\"pixelBounds\":null,\"tileBounds\":null,\"image\":null},{\"stats\":null,\"pixelBounds\":null,\"tileBounds\":null,\"image\":null},{\"stats\":null,\"pixelBounds\":null,\"tileBounds\":null,\"image\":null},{\"stats\":null,\"pixelBounds\":null,\"tileBounds\":null,\"image\":null},{\"stats\":null,\"pixelBounds\":null,\"tileBounds\":null,\"image\":null},{\"stats\":null,\"pixelBounds\":null,\"tileBounds\":null,\"image\":null},{\"stats\":null,\"pixelBounds\":null,\"tileBounds\":null,\"image\":null},{\"stats\":[{\"min\":-5,\"max\":2193,\"mean\":521.1451838574833}],\"pixelBounds\":{\"maxY\":728,\"maxX\":728,\"minX\":0,\"minY\":0},\"tileBounds\":{\"maxY\":187,\"maxX\":291,\"minX\":290,\"minY\":185},\"image\":\"9\"}],\"bands\":1,\"defaultValues\":[-32768],\"stats\":[{\"min\":-5,\"max\":2193,\"mean\":521.1451838574833}],\"tilesize\":512,\"maxZoomLevel\":9}";
    try
    {
      final InputStream is = new ByteArrayInputStream(json.getBytes());
      final MrsImagePyramidMetadata meta = MrsImagePyramidMetadata.load(is);
      assertNotNull(meta.getStats());
      assertNotNull(meta.getStats(0));
    }
    catch (final Exception e)
    {
      log.error("Failed to load metadata", e);
      fail("Failed to load metadata");
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testSerializeJsonDefaultValues()
  {
    try
    {
      final MrsImagePyramidMetadata metaIn = new MrsImagePyramidMetadata();
      final double[] defaultValues = new double[1];
      defaultValues[0] = Double.NaN;
      metaIn.setPyramid("foo");
      metaIn.setDefaultValues(defaultValues);
      metaIn.setTileType(2); // Set data buffer type to Short
      metaIn.setTag("foo", "bar");
      metaIn.setTag("bar", "foo");
      final ByteArrayOutputStream os = new ByteArrayOutputStream();
      metaIn.save(os);
      final byte[] jsonBytes = os.toByteArray();
      final InputStream is = new ByteArrayInputStream(jsonBytes);
      final MrsImagePyramidMetadata metaOut = MrsImagePyramidMetadata.load(is);
      assertEquals(metaIn.getDefaultValue(0), metaOut.getDefaultValue(0), epsilon);
      assertEquals(metaIn.getDefaultValueInt(0), metaOut.getDefaultValueInt(0));
    }
    catch (final Exception e)
    {
      log.error("Failed to load metadata", e);
      fail("Failed to load metadata");
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testSerializeJsonTags()
  {
    try
    {
      final MrsImagePyramidMetadata metaIn = new MrsImagePyramidMetadata();
      metaIn.setPyramid("foo");
      metaIn.setTag("foo", "bar");
      metaIn.setTag("bar", "foo");
      final ByteArrayOutputStream os = new ByteArrayOutputStream();
      metaIn.save(os);
      final byte[] jsonBytes = os.toByteArray();
      final InputStream is = new ByteArrayInputStream(jsonBytes);
      final MrsImagePyramidMetadata metaOut = MrsImagePyramidMetadata.load(is);
      assertEquals(metaIn.getTags().size(), metaOut.getTags().size());
      assertEquals(metaIn.getTags(), metaOut.getTags());
    }
    catch (final Exception e)
    {
      log.error("Failed to load metadata", e);
      fail("Failed to load metadata");
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testSetClassification()
  {
    final MrsImagePyramidMetadata metadata = new MrsImagePyramidMetadata();
    metadata.setClassification(Classification.Continuous);
    assertEquals(Classification.Continuous, metadata.getClassification());
    assertEquals("Continuous", metadata.getClassification().name());
    metadata.setClassification(Classification.Categorical);
    assertEquals(Classification.Categorical, metadata.getClassification());
    assertEquals("Categorical", metadata.getClassification().name());
  }

  @Test
  @Category(UnitTest.class)
  public void testSetMaxZoomLevel()
  {
    final MrsImagePyramidMetadata metadata = new MrsImagePyramidMetadata();
    // The +1 is because we want to be able to index ImageMetadata array by zoom level
    metadata.setMaxZoomLevel(10);
    assertEquals(10 + 1, metadata.getImageMetadata().length);
    metadata.setMaxZoomLevel(8);
    assertEquals(8 + 1, metadata.getImageMetadata().length);
    metadata.setMaxZoomLevel(11);
    assertEquals(11 + 1, metadata.getImageMetadata().length);
  }
  
}