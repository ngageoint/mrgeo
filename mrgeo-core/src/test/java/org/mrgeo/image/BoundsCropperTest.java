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

package org.mrgeo.image;

import junit.framework.Assert;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.pyramid.MrsPyramidMetadata;
import org.mrgeo.utils.LongRectangle;
import org.mrgeo.utils.TMSUtils;
import org.mrgeo.utils.TMSUtils.Bounds;
import org.mrgeo.utils.TMSUtils.TileBounds;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@SuppressWarnings("static-method")
public class BoundsCropperTest
{
  private static MrsPyramidMetadata imageMetadata;
  private static AllOnes ALL_ONES;
  
  @BeforeClass
  public static void init() throws JsonGenerationException, JsonMappingException, IOException
  {
    ALL_ONES = new AllOnes();
    imageMetadata = ALL_ONES.getMetadata();
  }

  @Test
  @Category(UnitTest.class)
  public void testCropSingleBounds() throws Exception
  {
    /*   
     * In the all-ones dataset, the tile boundaries slightly past the image boundaries on all  
     * four sides. Here, we construct a single boundary based on the middle two rows of tiles 
     * and check to see the resulting cropped boundary. We test both that cropping a subset of 
     * the image boundary works in the single boundary case, and that the bounds are appropriately
     * clipped to the image boundary 
     */  
    Bounds interiorRowsTileBounds = ALL_ONES.getBoundsInteriorRows();
    
    MrsPyramidMetadata croppedMetadata = BoundsCropper.getCroppedMetadata(imageMetadata,
        Collections.singletonList(interiorRowsTileBounds), imageMetadata.getMaxZoomLevel());

    // compare lat/lon bounds
    Bounds imageBounds = Bounds.convertOldToNewBounds(imageMetadata.getBounds());
    Bounds croppedBounds = Bounds.convertOldToNewBounds(croppedMetadata.getBounds());

    Assert.assertTrue(croppedBounds.w == imageBounds.w);
    Assert.assertTrue(croppedBounds.w > interiorRowsTileBounds.w);
    
    Assert.assertTrue(croppedBounds.e == imageBounds.e);
    Assert.assertTrue(croppedBounds.e < interiorRowsTileBounds.e);

    Assert.assertTrue(croppedBounds.n < imageBounds.n);
    Assert.assertTrue(croppedBounds.n == interiorRowsTileBounds.n);

    Assert.assertTrue(croppedBounds.s > imageBounds.s);
    Assert.assertTrue(croppedBounds.s == interiorRowsTileBounds.s);   
    
    // compare tile bounds
    LongRectangle tileBounds = croppedMetadata.getTileBounds(imageMetadata.getMaxZoomLevel());
    LongRectangle expectedTileBounds = TileBounds.convertToLongRectangle( 
                                            TMSUtils.boundsToTile(
                                                interiorRowsTileBounds, 
                                                imageMetadata.getMaxZoomLevel(), 
                                                imageMetadata.getTilesize())); 
    
    Assert.assertTrue(tileBounds.equals(expectedTileBounds));

  }
  
  @Test
  @Category(UnitTest.class)
  public void testCropMultipleBounds() throws Exception
  {
    /*   
     * In the all-ones dataset, the tile boundaries slightly past the image boundaries on all  
     * four sides. Here, we construct two boundaries based a subset of both the top and bottom rows 
     * of tiles, and check to see the resulting cropped boundary. In addition to testing everything 
     * tested by testCropSingleBounds, we also check that a proper envelope around both bounds is 
     * constructed 
     */  
    
    double epsilon = 0.005;
    
    Bounds topRowBounds = (ALL_ONES.getBounds(3, 0)).union(ALL_ONES.getBounds(3, 2));
    Bounds bottomRowBounds = (ALL_ONES.getBounds(0, 0)).union(ALL_ONES.getBounds(0, 2));
    
    Bounds subsetTopRowBounds = new Bounds(topRowBounds.w + epsilon, topRowBounds.s + epsilon,
                                            topRowBounds.e - epsilon, topRowBounds.n - epsilon);

    Bounds subsetBottomRowBounds = new Bounds(bottomRowBounds.w + epsilon, bottomRowBounds.s + epsilon,
                                               bottomRowBounds.e - epsilon, bottomRowBounds.n - epsilon);
   
    List<Bounds> userBounds = new ArrayList<Bounds>();
    userBounds.add(subsetTopRowBounds);
    userBounds.add(subsetBottomRowBounds);
   
    MrsPyramidMetadata croppedMetadata = BoundsCropper.getCroppedMetadata(imageMetadata,
                                                                          userBounds, imageMetadata.getMaxZoomLevel());

    Bounds imageBounds = Bounds.convertOldToNewBounds(imageMetadata.getBounds());
    Bounds croppedBounds = Bounds.convertOldToNewBounds(croppedMetadata.getBounds());

    Assert.assertTrue(croppedBounds.w == imageBounds.w);
    Assert.assertTrue(croppedBounds.w > subsetBottomRowBounds.w);
    
    Assert.assertTrue(croppedBounds.e == imageBounds.e);
    Assert.assertTrue(croppedBounds.e < subsetBottomRowBounds.e);

    // the fact that the n/s boundary of croppedBounds are equal to image's n/s boundary means  
    // that both subsetTopRowBounds and subsetBottomRowBounds were incorporated
    Assert.assertTrue(croppedBounds.n == imageBounds.n);
    Assert.assertTrue(croppedBounds.n < subsetTopRowBounds.n);

    Assert.assertTrue(croppedBounds.s == imageBounds.s);
    Assert.assertTrue(croppedBounds.s > subsetBottomRowBounds.s);  
  }

  @Test
  @Category(UnitTest.class)
  public void testWorldBoundsCroppedToImageBounds() throws Exception
  {
    /* 
     * Here we test that when given world bounds, it uses the image bounds 
     */
    MrsPyramidMetadata croppedMetadata = BoundsCropper.getCroppedMetadata(imageMetadata,
        Collections.singletonList(Bounds.WORLD), imageMetadata.getMaxZoomLevel());

    Bounds imageBounds = Bounds.convertOldToNewBounds(imageMetadata.getBounds());
    Bounds croppedBounds = Bounds.convertOldToNewBounds(croppedMetadata.getBounds());
   
    Assert.assertTrue(croppedBounds.equals(imageBounds));
  }
  
  @Test
  @Category(UnitTest.class)
  public void testRoundToNearestTileBounds() throws Exception
  {
    /* 
     * Here we test that the cropped boundaries are rounded up to the nearest tile boundary, 
     * when the tile boundary fits within the image boundary (the n/s boundary) and rounded 
     * up to the image boundary when the tile boundary does not fit within the image boundary 
     * (the e/w boundary)
     */
    double epsilon = 0.005;
    Bounds interiorRowBounds = (ALL_ONES.getBounds(1, 0)).union(ALL_ONES.getBounds(1, 2));
    Bounds subsetInteriorRowBounds = new Bounds(interiorRowBounds.w + epsilon, 
                                                interiorRowBounds.s + epsilon,
                                                interiorRowBounds.e - epsilon, 
                                                interiorRowBounds.n - epsilon);


    MrsPyramidMetadata croppedMetadata = BoundsCropper.getCroppedMetadata(imageMetadata,
        Collections.singletonList(subsetInteriorRowBounds), imageMetadata.getMaxZoomLevel());

    Bounds imageBounds = Bounds.convertOldToNewBounds(imageMetadata.getBounds());
    Bounds croppedBounds = Bounds.convertOldToNewBounds(croppedMetadata.getBounds());
   
    Assert.assertTrue(croppedBounds.n == interiorRowBounds.n);
    Assert.assertTrue(croppedBounds.s == interiorRowBounds.s);
    
    Assert.assertTrue(croppedBounds.w == imageBounds.w);    
    Assert.assertTrue(croppedBounds.e == imageBounds.e);
  }
  
  
  @Test
  @Category(UnitTest.class)
  public void testZoomLevelLessThanMaximum() throws Exception
  {
    final int zoomLevel = imageMetadata.getMaxZoomLevel() - 1;
    Bounds interiorRowsTileBounds = ALL_ONES.getBoundsInteriorRows();
    
    MrsPyramidMetadata croppedMetadata = BoundsCropper.getCroppedMetadata(imageMetadata,
        Collections.singletonList(interiorRowsTileBounds), zoomLevel);
    
    Assert.assertEquals(croppedMetadata.getMaxZoomLevel(), zoomLevel);    
  }
}
