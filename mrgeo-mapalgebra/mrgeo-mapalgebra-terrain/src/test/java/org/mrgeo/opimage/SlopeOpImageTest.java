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

/**
 * @author jason.surratt
 * 
 */
@Ignore
@SuppressWarnings("static-method")
public class SlopeOpImageTest
{
  @Before public void setUp()
  {
    OpImageRegistrar.registerMrGeoOps();
  }

  @Ignore
  @Test
  @Category(UnitTest.class)
  public void testDegrees() throws Exception
  {
//    RenderedImage input = GeoTiffDescriptor.create("testFiles/IslandsElevation.tif", null);
//    RenderedImage normal = HornNormalDescriptor.create(input, null);
//
//    RenderedImage slope = SlopeFromNormalOpImage.create(normal, SlopeFromNormalOpImage.DEGREES,
//        null);
//
//    ColorScale colorScale = new ColorScale();
//    colorScale.put(-1, new Color(0, 0, 0));
//    colorScale.put(0, new Color(255, 0, 0));
//    colorScale.put(90, new Color(255, 255, 255));
//
//    RenderedOp cso = ColorScaleDescriptor.create(slope, colorScale);
//
//    BufferedImage baseline = ImageIO.read(new File(Defs.INPUT
//        + "org.mrgeo.opimage/SlopeFromNormalDegreesTestBasline.png"));
//    TestUtils.compareRenderedImages(baseline, cso);
  }

  @Ignore
  @Test
  @Category(UnitTest.class)
  public void testPercent() throws Exception
  {
//    RenderedImage input = GeoTiffDescriptor.create("testFiles/IslandsElevation.tif", null);
//
//    RenderedImage normal = HornNormalDescriptor.create(input, null);
//
//    RenderedImage slope = SlopeFromNormalOpImage.create(normal, SlopeFromNormalOpImage.PERCENT,
//        null);
//
//    ColorScale colorScale = new ColorScale();
//    colorScale.put(-1, new Color(0, 0, 0));
//    colorScale.put(0, new Color(255, 0, 0));
//    colorScale.put(90, new Color(255, 255, 255));
//    colorScale.setForceValuesIntoRange(true);
//
//    RenderedImage cso = ColorScaleDescriptor.create(slope, colorScale);
//
//    BufferedImage baseline = ImageIO.read(new File(Defs.INPUT
//        + "org.mrgeo.opimage/SlopeFromNormalPercentTestBasline.png"));
//    TestUtils.compareRenderedImages(baseline, cso);
  }

  @Ignore
  @Test
  @Category(UnitTest.class)
  public void testGradient() throws Exception
  {
//    RenderedImage input = GeoTiffDescriptor.create("testFiles/IslandsElevation.tif", null);
//
//    RenderedImage normal = HornNormalDescriptor.create(input, null);
//
//    RenderedImage slope = SlopeFromNormalOpImage.create(normal, SlopeFromNormalOpImage.SLOPE,
//        null);
//
//    ColorScale colorScale = new ColorScale();
//    colorScale.put(-1, new Color(0, 0, 0));
//    colorScale.put(0, new Color(255, 0, 0));
//    colorScale.put(2, new Color(255, 255, 255));
//
//    RenderedImage cso = ColorScaleDescriptor.create(slope, colorScale);
//
//    BufferedImage baseline = ImageIO.read(new File(Defs.INPUT
//        + "org.mrgeo.opimage/SlopeFromNormalGradientTestBasline.png"));
//    TestUtils.compareRenderedImages(baseline, cso);
  }
}
