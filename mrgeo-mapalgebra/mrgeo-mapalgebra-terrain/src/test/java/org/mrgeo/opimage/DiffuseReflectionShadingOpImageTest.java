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
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.core.Defs;
import org.mrgeo.rasterops.ColorScale;
import org.mrgeo.rasterops.OpImageRegistrar;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.test.LocalRunnerTest;
import org.mrgeo.test.MapOpTestUtils;

import java.awt.*;
import java.io.File;

@SuppressWarnings("static-method")
public class DiffuseReflectionShadingOpImageTest extends LocalRunnerTest
{
  private static MapOpTestUtils testUtils;

  private static String islandsElevation = "IslandsElevation.tif";
  
  private static ColorScale colorScale;
  private static ColorScale csXML;
  
  @BeforeClass
  public static void init()
  {
    try
    {
      testUtils = new MapOpTestUtils(DiffuseReflectionShadingOpImageTest.class);
      
      File f = new File(Defs.INPUT + islandsElevation);
      islandsElevation = f.getCanonicalPath();
      
      
//      colorScale = ColorScale.empty();
//      colorScale.put(-12, new Color(255, 0, 0));
//      colorScale.put(182, new Color(255, 255, 0));
//      colorScale.put(375, new Color(0, 255, 0));
//      colorScale.put(568, new Color(0, 255, 255));
//      colorScale.put(761, new Color(0, 0, 255));
//      colorScale.put(954, new Color(255, 0, 255));
//      colorScale.setForceValuesIntoRange(true);
//
//      csXML = ColorScale.loadFromXML(testUtils.getInputLocal() + "InterpolateReliefMinMax.xml");
//      csXML.setScaleRange(-12, 954);
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }
  
  @Before public void setUp()
  {
    OpImageRegistrar.registerMrGeoOps();
  }

  @SuppressWarnings("unused")
  private static void checkSettings(ColorScale scale, String testName)
  {
//    RenderedImage input = GeoTiffDescriptor.create(islandsElevation, null);
//    RenderedImage floatRop = ConvertToFloatDescriptor.create(input);
//
//    RenderedImage cso = ColorScaleDescriptor.create(floatRop, scale);
//
//    RenderedImage normals = HornNormalDescriptor.create(floatRop, null);
//    DiffuseReflectionShadingOpImage rso = DiffuseReflectionShadingOpImage.create(normals,
//        new Vector3d(1, 1, -1), null);
//
//    RenderedImage multiplied = RawMultiplyDescriptor.create(rso, cso,
//        RawMultiplyOpImage.BandTreatment.ONE_BY_RGB);
//
//    //RenderedImage cache = TileCacheDescriptor.create(multiplied);
//    //ImageIO.write(cache, "png", new File(testName + "output.png"));
//
//    File f = new File(testUtils.getInputLocal() + testName + ".png");
//    BufferedImage baseline = ImageIO.read(f);
//    
//    TestUtils.compareRenderedImages(multiplied, baseline);
  }

  @Ignore
  @Test
  @Category(UnitTest.class)
  public void testInterpolateRelief() throws Exception
  {
    checkSettings(colorScale, "InterpolateRelief");
  }

  @Ignore
  @Test
  @Category(UnitTest.class)
  public void testInterpolateReliefMinMax() throws Exception
  {
    checkSettings(csXML, "InterpolateReliefMinMax");
  }
}
