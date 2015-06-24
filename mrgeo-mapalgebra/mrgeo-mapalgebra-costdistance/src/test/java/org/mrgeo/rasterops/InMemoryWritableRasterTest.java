package org.mrgeo.rasterops;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.junit.UnitTest;

import javax.media.jai.operator.ConstantDescriptor;
import java.awt.image.Raster;

@SuppressWarnings("static-method")
public class InMemoryWritableRasterTest {
	@Test
	@Category(UnitTest.class)
	public void printThirdBandRasterConversion() {
		final float RASTER_SIZE = MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT_INT;
		float PIXEL_VALUE = 1.0f;
		Raster parent = ConstantDescriptor.create(RASTER_SIZE, RASTER_SIZE, new Float[]{PIXEL_VALUE,PIXEL_VALUE+1,PIXEL_VALUE+2}, null).getData();
		Raster thirdBandRaster = parent.createChild
				(parent.getMinX(),
				parent.getMinY(), 
				parent.getWidth(), 
				parent.getHeight(),
				parent.getMinX(),
				parent.getMinY(),
				new int[]{2});
		Assert.assertEquals(1,thirdBandRaster.getNumBands());
		
		for(int py=0; py < parent.getHeight(); py++) {
			for(int px=0; px < parent.getWidth(); px++) {
				float pParent = parent.getSampleFloat(px, py, 2);
				float pChild = thirdBandRaster.getSampleFloat(px, py, 0);
				Assert.assertEquals(pParent, pChild, 0);
				Assert.assertEquals(3.0f,pChild,0);
			}			
		}
	}
}
