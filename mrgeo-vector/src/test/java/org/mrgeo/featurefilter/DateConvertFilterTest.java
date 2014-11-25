/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
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
