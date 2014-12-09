/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */

package org.mrgeo.opimage;

import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mrgeo.core.Defs;
import org.mrgeo.image.geotools.GeotoolsRasterUtils;
import org.mrgeo.rasterops.OpImageRegistrar;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.test.LocalRunnerTest;
import org.mrgeo.test.MapOpTestUtils;

import javax.imageio.ImageIO;
import javax.media.jai.PlanarImage;
import java.awt.image.RenderedImage;
import java.io.File;
import java.io.IOException;

/**
 * @author jason.surratt
 * 
 */
@SuppressWarnings("static-method")
public class AspectOpImageTest extends LocalRunnerTest
{
  @Rule
  public TestName testname = new TestName();

  private static MapOpTestUtils testUtils;

  private static String hornnormal = "horn-normal.tif";
  private static String hornnormalflat = "horn-normal-flat.tif";
//  private static ColorScale colorScale;

  private static boolean GEN_BASELINE_DATA_ONLY = true;

  @BeforeClass
  public static void init() throws IOException
  {
    testUtils = new MapOpTestUtils(AspectOpImageTest.class);

    File f = new File(Defs.INPUT + hornnormal);
    hornnormal = f.getCanonicalFile().toURI().toString();

    f = new File(Defs.INPUT + hornnormalflat);
    hornnormalflat = f.getCanonicalFile().toURI().toString();
  }

  @Before public void setUp()
  {
    OpImageRegistrar.registerMrGeoOps();
  }

  @Test
  @Category(UnitTest.class)
  public void aspect() throws Exception
  {
    PlanarImage normal = (PlanarImage)GeotoolsRasterUtils.openImage(hornnormal).read(null).getRenderedImage();

    RenderedImage aspect = AspectDescriptor.create(normal, null);

    if (GEN_BASELINE_DATA_ONLY)
    {
      testUtils.generateBaselineTif(conf, testname.getMethodName(), aspect);
    }
    else
    {
      testUtils.compareRenderedImages(testname.getMethodName(), aspect);
    }
  }

  @Test
  @Category(UnitTest.class)
  public void aspectflat() throws Exception
  {
    PlanarImage normal = (PlanarImage)GeotoolsRasterUtils.openImage(hornnormalflat).read(null).getRenderedImage();

    RenderedImage aspect = AspectDescriptor.create(normal, null);

    if (GEN_BASELINE_DATA_ONLY)
    {
      testUtils.generateBaselineTif(conf, testname.getMethodName(), aspect);
    }
    else
    {
      testUtils.compareRenderedImages(testname.getMethodName(), aspect);
    }
  }
}
