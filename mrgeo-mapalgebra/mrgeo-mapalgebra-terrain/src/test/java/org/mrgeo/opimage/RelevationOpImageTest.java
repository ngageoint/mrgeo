/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.opimage;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.rasterops.OpImageRegistrar;
import org.mrgeo.junit.UnitTest;

@Ignore
@SuppressWarnings("static-method")
public class RelevationOpImageTest 
{
  @Before public void setUp()
  {
    OpImageRegistrar.registerMrGeoOps();
  }
  
  @Test
  @Category(UnitTest.class)
  public void testBasics()
  {
//    String inputDir = TestUtils.composeInputDir(RelevationOpImageTest.class);
//
//    double sigma = 1000;
//
//    RenderedImage input = GeoTiffDescriptor.create(Defs.INPUT + "/greece.tif", null);
//
//    input = ConvertToFloatDescriptor.create(input);
//
//    input = ReplaceNullDescriptor.create(input, Double.NaN, null);
//
//    // TODO: This test is worthless until we've updated it to work with
//    // a MrsImagePyramid v2 input and we can use real zoom and tileSize.
//    RenderedImage blur = FastGaussianKernelDescriptor.create(input, sigma, 1);
//
//    RenderedImage relevation = RawBinaryMathDescriptor.create(input, blur, "-");
//
//    RenderedImage cache = TileCacheDescriptor.create(relevation);
//
//    // clear this in case another test set it.
//    JAI.getDefaultInstance().getRenderingHints().remove(JAI.KEY_BORDER_EXTENDER);
//
//    RenderedImage baseline = GeoTiffDescriptor.create(inputDir + "testBasicsOutput.tif", null);
//
//    TestUtils.compareRenderedImages(baseline, cache);
  }

}
