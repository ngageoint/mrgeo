package org.mrgeo.rasterops;

import org.mrgeo.rasterops.CostDistanceVertex.Position;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.image.DataBufferFloat;
import java.awt.image.WritableRaster;
import java.util.*;

/**
 * 
 */
class CostSurfaceCalculator
{
	static final Logger LOG = LoggerFactory.getLogger(CostSurfaceCalculator.class);	

	final float sqrt2 = (float) (Math.sqrt(2.0) / 2.0);
	final float[] dist2 = { sqrt2, .5f, sqrt2, .5f, sqrt2, .5f, sqrt2, .5f };
	final short[] dx = { -1, 0, 1, 1, 1, 0, -1, -1 };
	final short[] dy = { 1, 1, 1, 0, -1, -1, -1, 0 };
	//Raster friction;
	PriorityQueue<Point> heap;

	float maxCost = -1;

	float[] rawCost;

	short height, width;


	
	public static class ChangedPixelsMap {
		private HashMap<Position,ArrayList<PointWithFriction>> map;
		
		public ChangedPixelsMap() {
			map = new  HashMap<Position,ArrayList<PointWithFriction>>();
		}
		public void add(final Position side, final PointWithFriction point) {
			ArrayList<PointWithFriction> points = map.get(side);
			if(points == null) { 
				points = new ArrayList<PointWithFriction>();
				map.put(side, points);
			}
			
			if(LOG.isDebugEnabled()) {
			  LOG.debug(String.format("Adding point %d,%d with cost %.2f to direction %s", point.x,point.y,point.v,side));
			}
			points.add(point);
		}
		public Map<Position,ArrayList<PointWithFriction>> getMap() {
			return Collections.unmodifiableMap(map);
		}
		@Override
    public String toString() {
			String str = "";
			Map<Position,ArrayList<PointWithFriction>> readOnlyMap = getMap();
			for(Position i : readOnlyMap.keySet()) {
				List<PointWithFriction> points = readOnlyMap.get(i);
				str += "key=" + i + " and list = " + points + "\n";
			}
			return str;
		}
		public void printStats() {
			String str = "ChangedPixelsMap: ";
			Map<Position,ArrayList<PointWithFriction>> readOnlyMap = getMap();
			
			if(readOnlyMap.isEmpty())
				str += "empty";
			
			// Print a list of positions, and for each position, the total number of points 
			// and the number of duplicates. 
			HashMap<Point,Integer> existsMap = new HashMap<Point,Integer>();
			for(Position i : readOnlyMap.keySet()) {				
				List<PointWithFriction> points = readOnlyMap.get(i);
				
				existsMap.clear();
				for(Point point : points) {
					Integer count = existsMap.get(point);
					if(count == null)
						existsMap.put(point, 1);
					else
						existsMap.put(point, ++count);
				}
				int duplicates = 0;
				for(Point point : existsMap.keySet()) {
					duplicates += (existsMap.get(point) - 1);
				}
				str += String.format("[%s(%d,%d)],",i,duplicates,points.size());				
			}
			System.out.println(str);
			
			// For positions with at most 10 points, print the points themselves
			for(Position i : readOnlyMap.keySet()) {				
				List<PointWithFriction> points = readOnlyMap.get(i);
				
				if(points.size() <= 10) {
					System.out.println(String.format("Details: [%s: %s]", i, points));					
				}
			}
	
			
			
		}
		public void addPointIfOnBorder(PointWithFriction point, CostSurfaceCalculator csc) {
			if(point.x < 0 || point.x > csc.width-1 || point.y < 0 || point.y > csc.height-1)
				throw new RuntimeException(String.format("ChangedPixelsMap.addPoint: Illegal point"  
														+ "(%d,%d) with raster width %d height %d" 
														,point.x,point.y,csc.width,csc.height));
			
			// if the point is not on the border, it falls through all the checks and returns 			
	    
			// We have to be cognizant of rasters having their 0,0 on the upper left, so a point 
			// with py = 0 is actually on the top border, whereas py = width-1 is on the bottom border
			if(point.y == 0) {
        add(Position.TOP, point);
        if(point.x == 0)
          add(Position.TOPLEFT, point);
        else if(point.x == csc.width-1)
          add(Position.TOPRIGHT, point);
      }
      if(point.x == csc.width-1) {
        add(Position.RIGHT, point);
        if(point.y == csc.width-1)
          add(Position.BOTTOMRIGHT, point);       
      }
      if(point.y == csc.width-1) {
        add(Position.BOTTOM, point);
        if(point.x == 0)
          add(Position.BOTTOMLEFT, point);            
      }
      if(point.x == 0)
        add(Position.LEFT, point);			
			
		}
	}
	public final ChangedPixelsMap updateCostSurface(WritableRaster costSurface,
													Iterable<CostDistanceMessage> messages,
													@SuppressWarnings("hiding") final float maxCost) 
												throws IllegalArgumentException
	{
		// TODO - not sure if we need to do this
		costSurface = costSurface.createWritableTranslatedChild(0, 0);

		if (costSurface.getNumBands() != PixelSizeMultiplier.NUM_BANDS)
		{
			throw new IllegalArgumentException(String.format("Incorrect number of bands in cost surface: Expected %d, Got %d", PixelSizeMultiplier.NUM_BANDS, costSurface.getNumBands()));
		}

		this.maxCost = maxCost;
		this.width = (short)costSurface.getWidth();
		this.height = (short)costSurface.getHeight();

		DataBufferFloat dbf = (DataBufferFloat) costSurface.getDataBuffer();
		rawCost = dbf.getData();

		if (rawCost.length != width * height * PixelSizeMultiplier.NUM_BANDS)
		{
			String s = String.format("(reported: %d rawCost: %d)", width * height * PixelSizeMultiplier.NUM_BANDS, rawCost.length);
			throw new IllegalArgumentException("Image raw size doesn't match image size. " + s);
		}

		heap = new PriorityQueue<Point>();
		
		// first push points from any neighbors
		for(CostDistanceMessage message : messages) {
			for(PointWithFriction point : message.getPoints()) {
				// important to set position
				point.position = message.getPosition();			
				point.pushMyself(this);
			}
		}
		
		// then push my own points
		for (short y = 0; y < costSurface.getHeight(); y++)
		{
			for (short x = 0; x < costSurface.getWidth(); x++)
			{
				int index = PixelSizeMultiplier.NUM_BANDS * (x + y * width);
				float f = rawCost[index];        
				float v = rawCost[index + 2];

				// if the cost and friction value is non-negative and
				// the cost isn't greater than maxCost
				if (v >= 0 && f >= 0 && (v < maxCost || maxCost < 0) || Float.isNaN(v) == false)
				{          
					Point p = new Point(x, y, v);
					p.pushMyself(this);
				}
			}
		}

//    enable logging while debugging
//		LOG.info("heap size is: " + heap.size());
		
		// Runtime rt = Runtime.getRuntime();
		
		ChangedPixelsMap changedPixels = new ChangedPixelsMap();
//		int i = 0;
		// long start = new Date().getTime();
		while (!heap.isEmpty())
		{
//			if (i % 10000 == 0)
//			{
//	    enable logging while debugging
//				String s = String.format("memory: %.3fG / %.3fG free: %.3fG",
//						(double) (rt.totalMemory() - rt.freeMemory()) / 1e9, 
//						(double) (rt.maxMemory()) / 1e9,
//						(double) (rt.freeMemory() / 1e9));
//				LOG.info("i: {} heap size: {} {}", new Object[] { i, heap.size(), s });
//			}
//			i++;
			Point p = heap.poll();
			p.pushNeighbors(this, changedPixels);
		}
		
		// long elapsed = new Date().getTime() - start;
//  enable logging while debugging
//		LOG.info(String.format("all surrounds elapsed %dms", elapsed));
		return changedPixels;
	}
}

