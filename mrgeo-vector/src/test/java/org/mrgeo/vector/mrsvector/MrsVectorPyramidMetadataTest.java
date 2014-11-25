package org.mrgeo.vector.mrsvector;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.utils.Bounds;
import org.mrgeo.utils.LongRectangle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;

import static org.junit.Assert.*;

public class MrsVectorPyramidMetadataTest
{
  private static final Logger log = LoggerFactory.getLogger(MrsVectorPyramidMetadataTest.class);
  final double epsilon = 0.00000001;

  @Test
  @Category(UnitTest.class)
  public void testLoad()
  {
    final String json = "{\"bounds\":{\"maxY\":41.5,\"maxX\":25,\"minX\":24,\"minY\":40.5},\"vectorMetadata\":[{\"tileBounds\":null,\"name\":null},{\"tileBounds\":null,\"name\":null},{\"tileBounds\":null,\"name\":null},{\"tileBounds\":null,\"name\":null},{\"tileBounds\":null,\"name\":null},{\"tileBounds\":null,\"name\":null},{\"tileBounds\":null,\"name\":null},{\"tileBounds\":null,\"name\":null},{\"tileBounds\":null,\"name\":null},{\"tileBounds\":{\"maxY\":187,\"maxX\":291,\"minX\":290,\"minY\":185},\"name\":\"9\"}],\"tilesize\":512,\"maxZoomLevel\":9}";
    try {
      InputStream is = new ByteArrayInputStream(json.getBytes());
      MrsVectorPyramidMetadata meta = MrsVectorPyramidMetadata.load(is);
      assertNotNull(meta);
    } catch (Exception e) {
      log.error("Failed to load metadata", e);
      fail("Failed to load metadata");
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testSerializeJson()
  {
    try {
      MrsVectorPyramidMetadata metaIn = new MrsVectorPyramidMetadata();
      Bounds bounds = new Bounds(38.0, -140, 44.0, -135.0);
      metaIn.setBounds(bounds);
      int maxZoom = 10;
      metaIn.setMaxZoomLevel(maxZoom);
      metaIn.setName(maxZoom);
      int tileSize = 1024;
      metaIn.setTilesize(tileSize);
      LongRectangle tileBounds = new LongRectangle(1, 10, 5, 15);
      metaIn.setTileBounds(maxZoom, tileBounds);
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      metaIn.save(os);
      byte[] jsonBytes = os.toByteArray();
      InputStream is = new ByteArrayInputStream(jsonBytes);
      MrsVectorPyramidMetadata metaOut = MrsVectorPyramidMetadata.load(is);
      assertEquals(metaIn.getMaxZoomLevel(), metaOut.getMaxZoomLevel());
      MrsVectorPyramidMetadata.VectorMetadata[] outZoomData = metaOut.getVectorMetadata();
      assertNotNull(outZoomData);
      junit.framework.Assert.assertTrue(outZoomData.length == maxZoom + 1);
      assertEquals("" + maxZoom, outZoomData[maxZoom].name);
      assertEquals(tileBounds, metaOut.getTileBounds(maxZoom));
    } catch (Exception e) {
      log.error("Failed to load metadata", e);
      fail("Failed to load metadata");
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testSetMaxZoomLevel()
  {
    MrsVectorPyramidMetadata metadata = new MrsVectorPyramidMetadata();
    //The +1 is because we want to be able to index ImageMetadata array by zoom level
    metadata.setMaxZoomLevel(10);
    assertEquals(10+1, metadata.getVectorMetadata().length);
    metadata.setMaxZoomLevel(8);
    assertEquals(8+1, metadata.getVectorMetadata().length);
    metadata.setMaxZoomLevel(11);
    assertEquals(11+1, metadata.getVectorMetadata().length);
  }
  
  @Test
  @Category(UnitTest.class)
  public void testDeserializeUnknownProperty()
  {
    final String json = "{\"missing\":null,\"bounds\":{\"maxY\":41.5,\"maxX\":25,\"minX\":24,\"minY\":40.5},\"vectorMetadata\":[{\"tileBounds\":{\"maxY\":187,\"maxX\":291,\"minX\":290,\"minY\":185},\"name\":\"9\"}],\"maxZoomLevel\":3}";
    try {
      InputStream is = new ByteArrayInputStream(json.getBytes());
      @SuppressWarnings("unused")
      MrsVectorPyramidMetadata meta = MrsVectorPyramidMetadata.load(is);
      log.error("Should not have successfully loaded metadata");
      fail("Should not have successfully loaded metadata");
    } catch (Exception e) {
    }
  }
}
