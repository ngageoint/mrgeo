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

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.rasterops.OpImageRegistrar;
import org.mrgeo.rasterops.OpImageUtils;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.test.TestUtils;
import org.mrgeo.utils.HadoopUtils;

import java.io.File;
import java.io.IOException;

@SuppressWarnings("static-method")
public class MrsPyramidOpImageTest
{
  private static final double EPSILON = 1e-8;

  @Before
  public void setUp()
  {
    OpImageRegistrar.registerMrGeoOps();
  }

  @Test
  @Category(UnitTest.class)
  public void testLoadingNoData() throws IOException
  {
    File file = new File(TestUtils.composeInputDir(MrsPyramidOpImageTest.class), "IslandsElevation");

    MrsImageDataProvider dp = DataProviderFactory.getMrsImageDataProvider(file.getCanonicalFile().toURI().toString(),
        AccessMode.READ, HadoopUtils.createConfiguration());
    java.awt.image.RenderedImage ri =
        MrsPyramidDescriptor.create(dp);
    double noData = OpImageUtils.getNoData(ri, -10000);
    Assert.assertEquals(-32768.0, noData, EPSILON);

  }

}
