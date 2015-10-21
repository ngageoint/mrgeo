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

package org.mrgeo.services.utils;

import com.vividsolutions.jts.util.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.utils.Bounds;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.operation.TransformException;

public class RequestUtilsTest {

    @Test
    @Category(UnitTest.class)
    public void testBoundsFromParamGood() {
        String param = "66.047475576957,33.021709619141,68.594930410941,34.068157373047";
        Bounds bounds = RequestUtils.boundsFromParam(param);
        Assert.equals(bounds, new Bounds(66.047475576957,33.021709619141,68.594930410941,34.068157373047));
    }

    @Test(expected = IllegalArgumentException.class)
    @Category(UnitTest.class)
    public void testBoundsFromParamBad() {
        String param = "66.047475576957,33.021709619141,68.594930410941";
        RequestUtils.boundsFromParam(param);
    }

    @Test
    @Category(UnitTest.class)
    public void testReprojectBounds4326() throws NoSuchAuthorityCodeException, TransformException, FactoryException {
        String epsg = "EPSG:4326";
        Bounds bounds = new Bounds(66.047475576957,33.021709619141,68.594930410941,34.068157373047);
        Bounds prjBounds = RequestUtils.reprojectBounds(bounds, epsg);
        Assert.equals(bounds, prjBounds);
    }

    @Test
    @Category(UnitTest.class)
    public void testReprojectBounds3857() throws NoSuchAuthorityCodeException, TransformException, FactoryException {
        String epsg = "EPSG:3857";
        Bounds bounds = new Bounds(7395870.7107807,3914591.5868208,7466766.0545053,3948376.7533182);
        Bounds prjBounds = RequestUtils.reprojectBounds(bounds, epsg);
        Assert.equals(prjBounds, new Bounds(66.43823698866211,33.14519138631994,67.07510069706927,33.3989375016381));
    }

}
