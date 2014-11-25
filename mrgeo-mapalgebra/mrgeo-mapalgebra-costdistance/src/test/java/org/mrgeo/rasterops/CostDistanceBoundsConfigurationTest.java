package org.mrgeo.rasterops;

import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.image.AllOnes;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.utils.HadoopUtils;
import org.mrgeo.utils.TMSUtils.Bounds;

import java.io.IOException;

public class CostDistanceBoundsConfigurationTest
{
  private static AllOnes ALL_ONES;
  private static Bounds rvBounds;
  private final static double minFriction = 0.7147477;
  private final static float maxCost = 30000;
  private Configuration conf;
  
  @BeforeClass
  public static void init() throws JsonGenerationException, JsonMappingException, IOException
  {
    ALL_ONES = new AllOnes();
    
    // doesn't have to be interior
    rvBounds = ALL_ONES.getBoundsInteriorRows();
  }
  
  // every test needs a brand new configuration
  @Before
  public void setup() {    
    conf = HadoopUtils.createConfiguration();  
    MrGeoProperties.getInstance().remove("giraph.boundsBuffer");
    MrGeoProperties.getInstance().remove("giraph.disableAutoBounds");
  }
  
  @Test
  @Category(UnitTest.class)
  public void testRVGetBoundsAutoBuffer() {  
    Bounds bounds = CostDistanceBoundsConfiguration.getBounds(rvBounds, maxCost, minFriction, conf);
    
    boolean isBigger = (bounds.w < rvBounds.w && 
                        bounds.e > rvBounds.e && 
                        bounds.s < rvBounds.s && 
                        bounds.n > rvBounds.n);
    Assert.assertEquals("Bounds are not bigger cost bounds:" + bounds.toString() + " rvbounds: " + rvBounds.toString() , true, isBigger);
    
    int scheme = conf.getInt(CostDistanceVertex.SCHEME_FOR_BOUNDS_CALCULATION, -1);
    Assert.assertEquals(scheme, 2);    
  }
  
  @Test
  @Category(UnitTest.class)
  public void testRVGetBoundsHardCoded() {
    final int buffer = 1;
    MrGeoProperties.getInstance().setProperty("giraph.boundsBuffer", String.valueOf(buffer));
    Bounds bounds = CostDistanceBoundsConfiguration.getBounds(rvBounds, maxCost, minFriction, conf);
    
    boolean isCorrect = (bounds.w == rvBounds.w - buffer && 
                          bounds.e == rvBounds.e + buffer && 
                          bounds.s == rvBounds.s - buffer &&
                          bounds.n == rvBounds.n + buffer);
    Assert.assertEquals(true, isCorrect);
    
    int scheme = conf.getInt(CostDistanceVertex.SCHEME_FOR_BOUNDS_CALCULATION, -1);
    Assert.assertEquals(scheme, 1);    
  }
  
  @Test
  @Category(UnitTest.class)
  public void testRVGetBoundsPassedIn() {
    final int buffer = -1;
    MrGeoProperties.getInstance().setProperty("giraph.boundsBuffer", String.valueOf(buffer));
    MrGeoProperties.getInstance().setProperty("giraph.disableAutoBounds", String.valueOf(true));

    Bounds bounds = CostDistanceBoundsConfiguration.getBounds(rvBounds, maxCost, minFriction, conf);
    
    Assert.assertEquals(bounds, rvBounds);
    
    int scheme = conf.getInt(CostDistanceVertex.SCHEME_FOR_BOUNDS_CALCULATION, -1);
    Assert.assertEquals(scheme, 3);    
  }
}
