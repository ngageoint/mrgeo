package org.mrgeo.rasterops;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.core.Defs;
import org.mrgeo.image.MrsImage;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.test.TestUtils;
import org.mrgeo.utils.TMSUtils;

import java.awt.image.Raster;
import java.awt.image.WritableRaster;
import java.io.IOException;

public class PixelSizeMultiplierTest {
	String pyramidPath = Defs.CWD + "/" + Defs.INPUT + "all-ones";
	int zoom = 10;
	  MrsImage image;
	  PixelSizeMultiplier psm;
	  long start,end;
	  @Before
	  public void setUp() throws IOException
	  {
		  start = System.currentTimeMillis();
		  image = MrsImage.open(pyramidPath, zoom, null);
		  psm = new PixelSizeMultiplier(image.getMetadata());
	  }

	  @After
	  public void tearDown()
	  {
		  image.close();
	  }

	  @Test
	  @Category(UnitTest.class)
	  public void verifyRasterToWritableRaster() throws IOException {		  
		  for(long ty=image.getMinTileY(); ty <= image.getMaxTileY(); ty++) {
				for(long tx=image.getMinTileX(); tx <= image.getMaxTileX(); tx++) {
					Raster raster = image.getTile(tx,ty);
					Assert.assertNotNull(raster);		
					WritableRaster writableRaster 
						= psm.toWritableRaster(
								TMSUtils.tileid(tx, ty, image.getMaxZoomlevel()), raster);					
					Assert.assertNotNull(writableRaster);
					Assert.assertEquals(raster.getHeight(),writableRaster.getHeight());
					Assert.assertEquals(raster.getWidth(),writableRaster.getWidth());
					Assert.assertEquals(raster.getNumBands()+2, writableRaster.getNumBands());
					
					//TODO verify that friction values are assigned correctly in light of the raster UL being
										
					RasterWritable writable = RasterWritable.toWritable(writableRaster);
					Raster writableRasterNew = RasterWritable.toRaster(writable);					
					Assert.assertEquals(writableRaster.getHeight(),writableRasterNew.getHeight());
					Assert.assertEquals(writableRaster.getWidth(),writableRasterNew.getWidth());
					Assert.assertEquals(writableRaster.getNumBands(), writableRasterNew.getNumBands());
					TestUtils.compareRasters(writableRaster,writableRasterNew);
				}		
			}
		  end = System.currentTimeMillis();
		  System.out.println("Test took " + (end-start) + " ms.");
	  }
}
