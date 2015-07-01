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
import org.mrgeo.junit.IntegrationTest;
import org.mrgeo.test.LocalRunnerTest;

import javax.media.jai.FloatDoubleColorModel;
import java.awt.*;
import java.awt.color.ColorSpace;
import java.awt.image.BufferedImage;
import java.awt.image.DataBuffer;
import java.awt.image.WritableRaster;
import java.util.Hashtable;

/**
 * @author jason.surratt
 * 
 */
@Ignore
@SuppressWarnings("static-method")
public class TimeIntersectOpImageIntegrationTest extends LocalRunnerTest
{
  int _width, _height;

  @Before 
  public void setUp()
  {
    OpImageRegistrar.registerMrGeoOps();
  }

  public BufferedImage createCost(int px, int py)
  {
    FloatDoubleColorModel colorModel = new FloatDoubleColorModel(ColorSpace
        .getInstance(ColorSpace.CS_GRAY), false, false, Transparency.OPAQUE,
        DataBuffer.TYPE_FLOAT);

    WritableRaster cost = colorModel.createCompatibleWritableRaster(_width, _height);
    // default cost to -1
    for (int x = 0; x < cost.getWidth(); x++)
    {
      for (int y = 0; y < cost.getHeight(); y++)
      {
        cost.setSample(x, y, 0, -1);
      }
    }
    // set about middle corner (depending on projection) to 0 cost
    cost.setSample(px, py, 0, 0);
    
    BufferedImage result = new BufferedImage(colorModel, cost, false, new Hashtable<Object, Object>());

    return result;
  }

  // TODO This seems a little hosed.
  @Ignore
  @Test
  @Category(IntegrationTest.class)  
  public void testTimeIntersect() throws Exception
  {
//    RenderedImage friction = GeoTiffOpImage.create(Defs.GREECE);
//    friction = PixelSizeMultiplierOpImage.create(friction, null);
//    _width = friction.getWidth();
//    _height = friction.getHeight();
//
//    CostSurfaceCalculator uut = new CostSurfaceCalculator();
//    uut.setFriction(friction.getData());
//
//    BufferedImage a1c1 = createCost(15, 15);
//    uut.updateCostSurface(a1c1.getRaster());
//
//    BufferedImage a1c2 = createCost(185, 15);
//    uut.updateCostSurface(a1c2.getRaster());
//
//    BufferedImage a2c1 = createCost(90, 185);
//    uut.updateCostSurface(a2c1.getRaster());
//
//    BufferedImage a2c2 = createCost(110, 185);
//    uut.updateCostSurface(a2c2.getRaster());
//
//    @SuppressWarnings("rawtypes")
//    Vector sources = new Vector();
//    Vector<Date> originTimes = new Vector<Date>();
//    Vector<Date> destinationTimes = new Vector<Date>();
//
//    sources.add(a1c1);
//    sources.add(a1c2);
//    sources.add(a2c1);
//    sources.add(a2c2);
//
//    // set the start and end times for actor 1.
//    Calendar c;
//    c = Calendar.getInstance();
//    c.set(2000, 1, 1, 7, 00);
//    originTimes.add(c.getTime());
//
//    c = Calendar.getInstance();
//    c.set(2000, 1, 1, 12, 00);
//    destinationTimes.add(c.getTime());
//
//    // set the start and end times for actor 2.
//    c = Calendar.getInstance();
//    c.set(2000, 1, 1, 7, 00);
//    originTimes.add(c.getTime());
//
//    c = Calendar.getInstance();
//    c.set(2000, 1, 1, 12, 00);
//    destinationTimes.add(c.getTime());
//
//    RenderingHints hints = (RenderingHints) JAI.getDefaultInstance().getRenderingHints().clone();
//    hints
//        .put(JAI.KEY_BORDER_EXTENDER, BorderExtender.createInstance(BorderExtender.BORDER_REFLECT));
//    TimeIntersectOpImage so = TimeIntersectOpImage.create(sources, originTimes, destinationTimes,
//        hints);
//
////    ImageIO.write(so, "TIF", new File("output.tif"));
////
////    ImageIO.write(a1c1, "TIF", new File("output_a1c1.tif"));
////    ImageIO.write(a1c2, "TIF", new File("output_a1c2.tif"));
////    ImageIO.write(a2c1, "TIF", new File("output_a2c1.tif"));
////    ImageIO.write(a2c2, "TIF", new File("output_a2c2.tif"));
  }
}
