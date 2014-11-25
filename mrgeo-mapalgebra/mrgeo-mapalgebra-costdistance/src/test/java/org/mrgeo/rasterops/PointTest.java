package org.mrgeo.rasterops;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.junit.UnitTest;

import java.util.HashMap;

public class PointTest {
	@Test
	@Category(UnitTest.class)
	public void testHashCode() throws Exception {
		short x = 0, y = 0;
		float v = 0f;
		Point p1 = new PointWithFriction(x,y,v);
		Point p2 = new Point(x,y,++v);
		Point p3 = new Point(++x,++y,++v);

		HashMap<Point,String> pointMap = new HashMap<Point,String>();
		
		pointMap.put(p1, "val1");
		pointMap.put(p2, "val2");
		
    // val1 should have been overwritten by val2 
		Assert.assertEquals(pointMap.get(p1), "val2");
		
		// new point
		pointMap.put(p3, "val3");
		
    Assert.assertEquals(pointMap.get(p1), "val2");
    Assert.assertEquals(pointMap.get(p2), "val2");
    Assert.assertEquals(pointMap.get(p3), "val3");

	}

}

