package org.mrgeo.rasterops;

import java.awt.image.DataBuffer;
import java.util.HashSet;
import java.util.Properties;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.mrgeo.data.tile.TiledInputFormatContext;
import org.mrgeo.junit.IntegrationTest;
import org.mrgeo.utils.HadoopUtils;
import org.mrgeo.utils.TMSUtils;
import org.mrgeo.utils.TMSUtils.Bounds;
import org.mrgeo.utils.TMSUtils.TileBounds;

@SuppressWarnings("static-method")
public class CostDistanceWorkersConfigurationIntegrationTest
{
  private final static int zoomLevel = 12;
  private final static int tileSize = MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT_INT;
  private static Configuration conf;
  private static MrsImagePyramidMetadata metadata;
  
  
  @Before
  public void init()
  {
    // create a configuration
    conf = HadoopUtils.createConfiguration();
    
    // set this to a relatively high value - it will be bumped back down in testMoreWorkersThanMapSlots
    conf.setInt("mapred.tasktracker.map.tasks.maximum", 100);
    
    // create a metadata
    metadata = new MrsImagePyramidMetadata();    
    metadata.setTilesize(tileSize);
    metadata.setTileType(DataBuffer.TYPE_FLOAT);
  }
  
  @Test
  @Category(IntegrationTest.class)
  public void testGetHeapPerSlot() {
    {
      conf.set("mapred.child.java.opts", "-Xmx1024M");
      int heapPerSlot = CostDistanceWorkersConfiguration.getHeapPerSlot(conf);
      Assert.assertEquals(1024, heapPerSlot);
    }
    {
      conf.set("mapred.child.java.opts", "-Xmx2G");
      int heapPerSlot = CostDistanceWorkersConfiguration.getHeapPerSlot(conf);
      Assert.assertEquals(2048, heapPerSlot);
    }
    {
      conf.set("mapred.child.java.opts", "-Xmx1048064K");
      int heapPerSlot = CostDistanceWorkersConfiguration.getHeapPerSlot(conf);
      Assert.assertEquals(1023, heapPerSlot);
    }
  }
  
  @Test(expected = IllegalArgumentException.class)
  @Category(IntegrationTest.class)
  public void testMoreWorkersThanMapSlots() throws Exception
  {
    HadoopUtils.setupLocalRunner(conf);
    
    // find a large enough tile bounds 
    TileBounds tb = new TileBounds(2764, 1365, 2878, 1479);
    Bounds bounds = TMSUtils.tileToBounds(tb, zoomLevel, tileSize);
    TiledInputFormatContext ifContext = new TiledInputFormatContext(zoomLevel,
        tileSize,
        new HashSet<String>(), bounds.convertNewToOldBounds(),
        new Properties());
    ifContext.save(conf);
    
    CostDistanceWorkersConfiguration.getNumWorkers(metadata, conf);
  }
  
  @Test
  @Category(IntegrationTest.class)
  public void testHardCodedWorkers() throws Exception
  {
    final int numExpectedWorkers = 5;
    MrGeoProperties.getInstance().setProperty("giraph.workers", String.valueOf(numExpectedWorkers));
    
    // find a large enough tile bounds 
    TileBounds tb = new TileBounds(2764, 1365, 2878, 1479);
    Bounds bounds = TMSUtils.tileToBounds(tb, zoomLevel, tileSize);
    TiledInputFormatContext ifContext = new TiledInputFormatContext(zoomLevel,
        tileSize,
        new HashSet<String>(), bounds.convertNewToOldBounds(),
        new Properties());
    ifContext.save(conf);
    
    int numWorkers = CostDistanceWorkersConfiguration.getNumWorkers(metadata, conf);
    Assert.assertEquals(numExpectedWorkers, numWorkers);
    MrGeoProperties.getInstance().remove("giraph.workers");
  }
  
  @Test
  @Category(IntegrationTest.class)
  public void testGetOneWorker() throws Exception
  {
    // find a small enough tile bounds to 4 tiles 
    TileBounds tb = new TileBounds(2764, 1365, 2765, 1366);
    Bounds bounds = TMSUtils.tileToBounds(tb, zoomLevel, tileSize);
    TiledInputFormatContext ifContext = new TiledInputFormatContext(zoomLevel,
        tileSize,
        new HashSet<String>(), bounds.convertNewToOldBounds(),
        new Properties());
    ifContext.save(conf);
    
    int numWorkers = CostDistanceWorkersConfiguration.getNumWorkers(metadata, conf);
    Assert.assertEquals(1, numWorkers);
  }
  
  @Test
  @Category(IntegrationTest.class)
  public void testGetMultipleWorkers() throws Exception
  {
    conf.set("mapred.child.java.opts", "-Xmx2048M");
    conf.setInt("io.sort.mb", 100);
    
    // find a large enough tile bounds 
    TileBounds tb = new TileBounds(50, 50, 100, 100);
    Bounds bounds = TMSUtils.tileToBounds(tb, zoomLevel, tileSize);
    TiledInputFormatContext ifContext = new TiledInputFormatContext(zoomLevel,
        tileSize,
        new HashSet<String>(), bounds.convertNewToOldBounds(),
        new Properties());
    ifContext.save(conf);
    
    int numWorkers = CostDistanceWorkersConfiguration.getNumWorkers(metadata, conf,
                                                            false /* disable map slots check */);
    Assert.assertEquals(10, numWorkers);
  }
  
  // tests that numTiles is correctly computed - I had an off-by-one error due to assuming that 
  // boundsToTile returns tile boundaries where the upper right is exclusive instead of inclusive
  @Test
  @Category(IntegrationTest.class)
  public void testNumTiles() throws Exception
  {   
    conf.set("mapred.child.java.opts", "-Xmx2048M");
    conf.setInt("io.sort.mb", 100);
    
    // find a large enough tile bounds 
    TileBounds tb = new TileBounds(50, 100, 1000, 100);
    Bounds bounds = TMSUtils.tileToBounds(tb, zoomLevel, tileSize);
    TiledInputFormatContext ifContext = new TiledInputFormatContext(zoomLevel,
        tileSize,
        new HashSet<String>(), bounds.convertNewToOldBounds(),
        new Properties());
    ifContext.save(conf);
    
    int numWorkers = CostDistanceWorkersConfiguration.getNumWorkers(metadata, conf,
                                                            false /* disable map slots check */);
    Assert.assertEquals(4, numWorkers);
  }
}
