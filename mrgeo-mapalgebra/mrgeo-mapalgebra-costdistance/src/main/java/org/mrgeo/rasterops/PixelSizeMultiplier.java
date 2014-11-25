package org.mrgeo.rasterops;

import org.apache.log4j.Logger;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.mrgeo.utils.LatLng;
import org.mrgeo.utils.TMSUtils;
import org.mrgeo.utils.TMSUtils.Tile;

import javax.media.jai.ComponentSampleModelJAI;
import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;

public class PixelSizeMultiplier {
  
  /*
   *  CostDistance uses a three band raster to store in-memory state 
   *  band 0 - friction to traverse the pixel horizontally (unit = seconds)
   *  band 1 - friction to traverse the pixel vertically (unit = seconds)
   *  band 2 - cumulative cost to reach the pixel (unit = seconds)
   *  
   *  See toWritableRaster below for how the first two are computed. 
   *  The third is maintained by Point/PointWithFriction, and changes 
   *  as the algorithm iterates.
   *  
   */
  public final static int NUM_BANDS = 3;

  // Pixel height in meters
  float  pixelHeightM;
  // Pixel width in longitude
  double pixelWidthL;
  int    zoomLevel;
  int 	 tileSize;
  float  nodata;
	
  private static final Logger LOG = Logger.getLogger(PixelSizeMultiplier.class);

	
  double res;
  SampleModel sampleModel;

  public PixelSizeMultiplier(MrsImagePyramidMetadata metadata) {
    LatLng o = new LatLng(0, 0);
    System.out.println("new lat: " + metadata.getPixelHeight(metadata.getMaxZoomLevel()));
    LatLng n = new LatLng(metadata.getPixelHeight(metadata.getMaxZoomLevel()), 0);
    this.pixelHeightM = (float)LatLng.calculateGreatCircleDistance(o, n);
    this.pixelWidthL = metadata.getPixelWidth(metadata.getMaxZoomLevel());
    this.zoomLevel = metadata.getMaxZoomLevel();
    this.tileSize = metadata.getTilesize();
    this.res = TMSUtils.resolution(zoomLevel, tileSize);
    this.nodata = metadata.getDefaultValueFloat(0); // band 0
    System.out.println("pixel height = " + this.pixelHeightM);
    System.out.println("resolution = " + res);
    
    sampleModel = new ComponentSampleModelJAI(DataBuffer.TYPE_FLOAT, 
        tileSize, tileSize, NUM_BANDS, tileSize * NUM_BANDS, new int[] { 0, 1, 2 });
  }


  public WritableRaster toWritableRaster(long tileId, Raster raster) {

    WritableRaster out = Raster.createWritableRaster(sampleModel, null);

    Tile tile = TMSUtils.tileid(tileId, zoomLevel);

    final double startPx = tile.tx * tileSize;
    // since Rasters have their UL as 0,0, just tile.ty * tileSize does not work
    final double startPy = (tile.ty * tileSize) + tileSize - 1;

    final double lonStart = startPx * res - 180.0;    
    final double latStart = startPy * res - 90.0;
    
    final double lonNext = lonStart + res;    
    final double latNext = latStart + res;
    final LatLng o = new LatLng(latStart, lonStart);
    final LatLng n = new LatLng(latNext, lonNext);
    float[] v = new float[NUM_BANDS];

    for (int py = 0; py < raster.getHeight(); py++)
    {
      // since Rasters have their UL as 0,0, but since startPy is based on 0,0 being LL,  
      // we have to do startPy - py instead of startPy + py
      final double lat = (startPy - py) * res - 90.0;
      
      o.setLat(lat);	    	
      n.setLat(lat);
      final float pixelWidthM = (float)LatLng.calculateGreatCircleDistance(o, n);
      for (int px = 0; px < raster.getWidth(); px++)
      {
        final float s = raster.getSampleFloat(px, py, 0);
        v[0] = s * pixelWidthM;  
        v[1] = s * pixelHeightM;  
        v[2] = Float.NaN;
        
        // if nodata is not NaN and raw friction is nodata, set converted friction to NaN
        // if nodata is NaN, the above multiplication will ensure that converted friction is NaN
        if(!Float.isNaN(nodata) && s == nodata) {
        	v[0] = v[1] = Float.NaN;
        }
        if ((px == 510 || px == 511) && (py == 510 || py == 511))
        {
          System.out.println("friction surface at (" + px + ", " + py + ") is " + s);
          System.out.println("pixelWidthM = " + pixelWidthM);
          System.out.println("pixelHeightM = " + pixelHeightM);
          System.out.println("Set cost surface (" + px + ", " + py + ") to width " + v[0] + ", height " + v[1]);
        }
        out.setPixel(px, py, v);
      }
    }
    return out;
  }
}
