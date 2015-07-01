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
