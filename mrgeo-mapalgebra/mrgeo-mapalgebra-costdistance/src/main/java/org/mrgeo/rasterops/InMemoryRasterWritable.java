package org.mrgeo.rasterops;

import org.mrgeo.data.raster.RasterWritable;

import java.awt.Point;
import java.awt.image.Raster;
import java.awt.image.WritableRaster;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

/**
 * InMemoryRasterWritable is a variant of RasterWritable, which can keep the deserialized version of
 * the underlying raster (as a WritableRaster) in memory. This is useful in applications like 
 * CostDistance where the underlying raster is both long-living and changing (as it goes through 
 * the algorithm), but we would still like to back it up with a Writable. 
 * 
 * InMemoryRasterWritable is designed to be one of two states: initialized or not. Initialized means
 * raster is deserialized as writableRaster, and the backing byte[] referenced by super.get() is set
 * to null - see readFields() and setWritableRaster below. That reduces the in-memory footprint, 
 * since the class does not need to keep a deserialized Raster as well as a serialized byte[] in 
 * memory. 
 *
 */
public class InMemoryRasterWritable extends RasterWritable {
	private boolean isInitialized;
	private WritableRaster writableRaster;
	
	public InMemoryRasterWritable() {
		super();
	}

	public WritableRaster getWritableRaster() {
		return writableRaster;
	}

	public void setWritableRaster(WritableRaster writableRaster) {
		this.writableRaster = writableRaster;
		super.set(new byte[0], 0, 0);
		isInitialized = true;
	}

	/* copy constructor */
	public static InMemoryRasterWritable toInMemoryRasterWritable(final RasterWritable rw) {
	  byte[] copy = Arrays.copyOf(rw.getBytes(), rw.getLength());
		InMemoryRasterWritable imrw = new InMemoryRasterWritable(copy);
		imrw.isInitialized = false;
		return imrw;
	}
	
  private InMemoryRasterWritable(byte[] bytes) {
    super(bytes);
  }
  
	public Raster getThirdBandRaster() {
	  assert(isInitialized);
	  
	  WritableRaster parent = getWritableRaster();
    Raster thirdBandRaster = parent.createChild
                  (parent.getMinX(),
                  parent.getMinY(), 
                  parent.getWidth(), 
                  parent.getHeight(),
                  parent.getMinX(),
                  parent.getMinY(),
                  new int[]{2});
    return thirdBandRaster;
	}
	
	public boolean isInitialized() {
		return isInitialized;
	}

	/* 
	 * Deserializer. It first deserializes the backing byte[], and then based on whether it was 
	 * initialized or not, it deserializes the byte[] into a raster. 
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		isInitialized = in.readBoolean();
				
		super.readFields(in);
		
		if(isInitialized) {
			setWritableRaster(makeRasterWritable(RasterWritable.toRaster(this)));
		}
	}

	
	/* 
	 * Serializer. We piggy-back on super's write(). If we were initialized, then we have to make 
	 * sure that super.set is called with the byte[], so that super.write would serialize that byte[]
	 * out
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeBoolean(isInitialized);
		if(isInitialized) {
			super.set(RasterWritable.toWritable(writableRaster));
		}
		super.write(out);
	}
	

	private WritableRaster makeRasterWritable(Raster ras) {
        WritableRaster ret = Raster.createWritableRaster
                        (ras.getSampleModel(), 
                         ras.getDataBuffer(), 
                         new Point(0,0));
//        ret = ret.createWritableChild
//                        (ras.getMinX(), 
//                         ras.getMinY(), 
//                         ras.getWidth(), 
//                         ras.getHeight(),
//                         ras.getMinX(), 
//                         ras.getMinY(),
//                         null);
        return ret;
	}

}
