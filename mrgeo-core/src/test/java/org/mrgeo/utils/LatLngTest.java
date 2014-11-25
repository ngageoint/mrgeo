/*
 * Copyright 2009-2014 DigitalGlobe, Inc.
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

package org.mrgeo.utils;

import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.junit.UnitTest;

@SuppressWarnings("static-method")
public class LatLngTest 
{

  @Test
  @Category(UnitTest.class)
  public void testBasics() throws Exception
  {
    LatLng p1, p2;
    
    p1 = new LatLng(0, 0);
    p2 = new LatLng(0, 1);
    Assert.assertEquals(111319.49079326246, LatLng.calculateGreatCircleDistance(p1, p2));

    p1 = new LatLng(0, 0);
    p2 = new LatLng(1, 0);
    Assert.assertEquals(111319.49079326246, LatLng.calculateGreatCircleDistance(p1, p2));
  }
}
