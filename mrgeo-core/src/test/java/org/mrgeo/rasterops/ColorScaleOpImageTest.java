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

package org.mrgeo.rasterops;

import junit.framework.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.core.Defs;
import org.mrgeo.data.raster.RasterUtils;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.test.TestUtils;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.*;
import java.io.File;

/**
 * @author jason.surratt
 * 
 */
@SuppressWarnings("static-method")
public class ColorScaleOpImageTest
{

  private final String smallElevation = Defs.INPUT + "SmallElevation.tif";

  private static BufferedImage numberedDouble;

  private static String input = TestUtils.composeInputDir(ColorScaleOpImageTest.class);
  @Before public void setUp()
  {
    OpImageRegistrar.registerMrGeoOps();

  }

  @BeforeClass
  public static void init()
  {
    int width = 17;
    int height = 17;
    SampleModel smd = new BandedSampleModel(DataBuffer.TYPE_DOUBLE, width, height, 1);

    WritableRaster raster = Raster.createWritableRaster(smd, new Point(0, 0));
    for (int x=0; x < width; x++)
    {
      for (int y=0; y < height; y++)
      {
        raster.setSample(x, y, 0, y+1);
      }
    }

    numberedDouble = RasterUtils.makeBufferedImage(raster);
  }

  @Test
  @Category(UnitTest.class)
  public void testDefault() throws Exception
  {
    BufferedImage bi = ImageIO.read(new File(smallElevation));

    ColorScale colorScale = ColorScale.empty();
    colorScale.put(1548, new Color(255, 0, 0));
    colorScale.put(1598.4, new Color(255, 255, 0));
    colorScale.put(1648.8, new Color(0, 255, 0));
    colorScale.put(1699.2, new Color(0, 255, 255));
    colorScale.put(1749.6, new Color(0, 0, 255));
    colorScale.put(1800, new Color(255, 0, 255));
    ColorScaleOpImage cso = ColorScaleOpImage.create(bi, colorScale);

    BufferedImage baseline = ImageIO.read(new File(input, "ColorScaleBaseline.png"));
    TestUtils.compareRenderedImages(cso, baseline);
  }

  @Test
  @Category(UnitTest.class)
  public void testXmlInterpolate() throws Exception
  {
    BufferedImage bi = ImageIO.read(new File(smallElevation));

    ColorScale colorScale = ColorScale.loadFromXML(new File(input, "Default.xml").getCanonicalPath());
    colorScale.setScaleRange(1548, 1800);
    ColorScaleOpImage cso = ColorScaleOpImage.create(bi, colorScale);

    BufferedImage baseline = ImageIO.read(new File(input, "ColorScaleBaseline.png"));

    TestUtils.compareRenderedImages(cso, baseline);
  }

  @Test
  @Category(UnitTest.class)
  public void testXmlNoInterpolate() throws Exception
  {
    BufferedImage bi = ImageIO.read(new File(smallElevation));

    ColorScale colorScale = ColorScale.loadFromXML(new File(input, "DefaultNoInterpolate.xml").getCanonicalPath());

    colorScale.setScaleRange(1548, 1800);
    ColorScaleOpImage cso = ColorScaleOpImage.create(bi, colorScale);

    BufferedImage baseline = ImageIO.read(new File(input, "BaselineNoInterpolate.png"));

    TestUtils.compareRenderedImages(cso, baseline);
  }

  @Test
  @Category(UnitTest.class)
  public void testXmlBrewer() throws Exception
  {
    BufferedImage bi = ImageIO.read(new File(smallElevation));

    ColorScale colorScale = ColorScale.loadFromXML(new File(input, "ColorBrewerBuGn6Class.xml").getCanonicalPath());

    colorScale.setScaleRange(1548, 1800);
    ColorScaleOpImage cso = ColorScaleOpImage.create(bi, colorScale);

    BufferedImage baseline = ImageIO.read(new File(input, "BaselineBrewer.png"));

    TestUtils.compareRenderedImages(cso, baseline);
  }

  @Test
  @Category(UnitTest.class)
  public void testAbsoluteColorScale() throws Exception
  {
    ColorScale colorScale = ColorScale.loadFromXML(new File(input, "LandCover.xml").getCanonicalPath());

    ColorScaleOpImage cso = ColorScaleOpImage.create(numberedDouble, colorScale);
    compare(numberedDouble.getData(), cso.getData(), colorScale);
  }

  private void compare(Raster orig, Raster raster, ColorScale cs)
  {    
    for (int x = 0; x < raster.getWidth(); x++)
    {
      for (int y = 0; y < raster.getHeight(); y++)
      {
        int color[] =  cs.lookup(orig.getSampleDouble(x, y, 0));

       for (int band = 0; band < raster.getNumBands(); band++)
       {
          int v = raster.getSample(x, y, band);

          Assert.assertEquals("Pixel value mismatch: px: " + x + " py: " +  y 
              + " b: " + band + " orig: " + color[band] + " v2: " + v,  color[band], v);
        }
      }
    }
  }
}
