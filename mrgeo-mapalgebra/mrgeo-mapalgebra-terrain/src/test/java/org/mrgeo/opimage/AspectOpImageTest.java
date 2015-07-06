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

import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.io.IOException;
import java.util.LinkedList;

import javax.media.jai.PlanarImage;
import javax.media.jai.RenderedOp;
import javax.media.jai.TiledImage;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.rasterops.OpImageRegistrar;
import org.mrgeo.test.LocalRunnerTest;
import org.mrgeo.test.OpImageTestUtils;
import org.opengis.referencing.FactoryException;

/**
 * @author jason.surratt
 *
 */
@SuppressWarnings("static-method")
public class AspectOpImageTest extends LocalRunnerTest
{
  @Rule
  public TestName testname = new TestName();

  private static OpImageTestUtils testUtils;

  private static boolean GEN_BASELINE_DATA_ONLY = false;

  @BeforeClass
  public static void init() throws IOException
  {
    testUtils = new OpImageTestUtils(AspectOpImageTest.class);
  }

  @Before public void setUp()
  {
    OpImageRegistrar.registerMrGeoOps();
  }

  private static RenderedImage runAspect(TiledImage arg) throws IOException, FactoryException
  {
    return runAspect(arg, "deg");
  }
  private static RenderedImage runAspect(TiledImage arg, String units) throws IOException, FactoryException
  {
    RenderedImage op = AspectDescriptor.create(arg, units, null);

    // Force the OpImage to be created - invokes create method on the descriptor for the op
    op.getMinX();

    java.util.List<TileLocator> tiles = new LinkedList<>();
    walkOpTree(op, tiles);

    for (TileLocator tl: tiles)
    {
      tl.setTileInfo(testUtils.tx, testUtils.ty, testUtils.zoom, testUtils.tileWidth);
    }

    return op;
  }

  private static void walkOpTree(RenderedImage op, java.util.List<TileLocator> tl)
  {
    if (op.getSources() != null)
    {
      for (Object obj: op.getSources())
      {
        if (obj instanceof RenderedImage)
        {
          walkOpTree((RenderedImage)obj, tl);
        }
      }
    }
    if (op instanceof RenderedOp)
    {
      RenderedOp ro = (RenderedOp)op;
      PlanarImage image = ro.getCurrentRendering();

      if (image instanceof TileLocator)
      {
        tl.add((TileLocator)image);
      }
      walkOpTree(image, tl);
    }
  }



  @Test
  @Category(UnitTest.class)
  public void aspect() throws Exception
  {
    RenderedImage aspect = runAspect(testUtils.numbered);

    Raster r = aspect.getData(testUtils.destRect);
    if (GEN_BASELINE_DATA_ONLY)
    {
      testUtils.generateBaselineTif(testname.getMethodName(), r);
    }
    else
    {
      testUtils.compareRasters(testname.getMethodName(), r);
    }
  }

  @Test
  @Category(UnitTest.class)
  public void aspectRad() throws Exception
  {
    RenderedImage aspect = runAspect(testUtils.numbered, "rad");

    Raster r = aspect.getData(testUtils.destRect);
    if (GEN_BASELINE_DATA_ONLY)
    {
      testUtils.generateBaselineTif(testname.getMethodName(), r);
    }
    else
    {
      testUtils.compareRasters(testname.getMethodName(), r);
    }
  }

  @Test
  @Category(UnitTest.class)
  public void aspectNaN() throws Exception
  {
    RenderedImage aspect = runAspect(testUtils.numberedWithNanNoData);

    Raster r = aspect.getData(testUtils.destRect);
    if (GEN_BASELINE_DATA_ONLY)
    {
      testUtils.generateBaselineTif(testname.getMethodName(), r);
    }
    else
    {
      testUtils.compareRasters(testname.getMethodName(), r);
    }
  }

  @Test
  @Category(UnitTest.class)
  public void aspectNodata() throws Exception
  {
    RenderedImage aspect = runAspect(testUtils.numberedWithNoData);

    Raster r = aspect.getData(testUtils.destRect);
    if (GEN_BASELINE_DATA_ONLY)
    {
      testUtils.generateBaselineTif(testname.getMethodName(), r);
    }
    else
    {
      testUtils.compareRasters(testname.getMethodName(), r);
    }
  }

  @Test
  @Category(UnitTest.class)
  public void aspectflat() throws Exception
  {
    RenderedImage aspect = runAspect(testUtils.twos);

    Raster r = aspect.getData(testUtils.destRect);
    if (GEN_BASELINE_DATA_ONLY)
    {
      testUtils.generateBaselineTif(testname.getMethodName(), r);
    }
    else
    {
      testUtils.compareRasters(testname.getMethodName(), r);
    }
  }
  @Test
  @Category(UnitTest.class)
  public void aspectflatNaN() throws Exception
  {
    RenderedImage aspect = runAspect(testUtils.twosWithNanNoData);

    Raster r = aspect.getData(testUtils.destRect);
    if (GEN_BASELINE_DATA_ONLY)
    {
      testUtils.generateBaselineTif(testname.getMethodName(), r);
    }
    else
    {
      testUtils.compareRasters(testname.getMethodName(), r);
    }
  }
  @Test
  @Category(UnitTest.class)
  public void aspectflatNodata() throws Exception
  {
    RenderedImage aspect = runAspect(testUtils.twosWithNoData);

    Raster r = aspect.getData(testUtils.destRect);
    if (GEN_BASELINE_DATA_ONLY)
    {
      testUtils.generateBaselineTif(testname.getMethodName(), r);
    }
    else
    {
      testUtils.compareRasters(testname.getMethodName(), r);
    }
  }
}
