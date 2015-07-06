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

package org.mrgeo.featurefilter;

import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.geometry.GeometryFactory;
import org.mrgeo.geometry.WritableGeometry;
import org.mrgeo.junit.UnitTest;

/**
 * @author jason.surratt
 * 
 */
@SuppressWarnings("static-method")
public class DateConvertFilterTest 
{
  @Test 
  @Category(UnitTest.class)  
  public void testBasics() throws Exception
  {
    try
    {
      DateConvertFilter filter = new DateConvertFilter("ddMMMyyyy:HH:mm:ss",
          "yyyy-MM-dd'T'HH:mm:ss", "date");

      WritableGeometry f = GeometryFactory.createEmptyGeometry();

      f.setAttribute("date", "03OCT2009:17:03:44.260000");
      f.setAttribute("1", "1");
      f.setAttribute("2", "2.0");

      Geometry conv = filter.filterInPlace(f);
      Assert.assertEquals(conv.getAttribute("date"), "2009-10-03T17:03:44");

      f.setAttribute("date", "22OCT2009:21:35:59.390000");
      f.setAttribute("1", "3");
      f.setAttribute("2", "4.0");
      conv = filter.filterInPlace(f);

      Assert.assertEquals(conv.getAttribute("date"), "2009-10-22T21:35:59");
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }
  }
}
