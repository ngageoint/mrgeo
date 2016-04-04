/*
 * Copyright 2009-2016 DigitalGlobe, Inc.
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
 *
 */

package org.mrgeo.services.utils;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.utils.tms.Bounds;

public class RequestUtilsTest {

    @Test
    @Category(UnitTest.class)
    public void testBoundsFromParamGood() {
        String param = "66.047475576957,33.021709619141,68.594930410941,34.068157373047";
        Bounds bounds = RequestUtils.boundsFromParam(param);
        Assert.assertEquals("Bounds are bad", bounds, new Bounds(66.047475576957,33.021709619141,68.594930410941,34.068157373047));
    }

    @Test(expected = IllegalArgumentException.class)
    @Category(UnitTest.class)
    public void testBoundsFromParamBad() {
        String param = "66.047475576957,33.021709619141,68.594930410941";
        RequestUtils.boundsFromParam(param);
    }

    @Test
    @Category(UnitTest.class)
    public void testReprojectBounds4326()  {
        String epsg = "EPSG:4326";
        Bounds bounds = new Bounds(66.047475576957,33.021709619141,68.594930410941,34.068157373047);
        Bounds prjBounds = RequestUtils.reprojectBounds(bounds, epsg);
        // no reprojection, should be the same as input
        Assert.assertEquals("Bounds are bad", bounds, prjBounds);
    }

    @Test
    @Category(UnitTest.class)
    public void testReprojectBounds3857()  {
        String epsg = "EPSG:3857";
        Bounds bounds = new Bounds(66.047475576957,33.021709619141,68.594930410941,34.068157373047);
        Bounds prjBounds = RequestUtils.reprojectBounds(bounds, epsg);

        Assert.assertEquals("Bad bounds west", 7352371, prjBounds.w, 1.0);
        Assert.assertEquals("Bad bounds south", 3898185, prjBounds.s, 1.0);
        Assert.assertEquals("Bad bounds east", 7635952, prjBounds.e, 1.0);
        Assert.assertEquals("Bad bounds north", 4037957, prjBounds.n, 1.0);
    }

}
