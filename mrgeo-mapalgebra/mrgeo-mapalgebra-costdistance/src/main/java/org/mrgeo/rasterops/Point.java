package org.mrgeo.rasterops;

import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;
import org.mrgeo.rasterops.CostSurfaceCalculator.ChangedPixelsMap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// Package Visible
class Point implements Comparable<Point>, Writable
{
	private static final Logger LOG = Logger.getLogger(Point.class);

	float v;	
	short x, y;
	
	public static Point createPoint(short x, short y, float v) {
		return new Point(x, y, v);
	}

	Point() {v=Float.NaN; x=y=0;}
	
	Point(short x, short y, float v) 
	{
		this.x = x;
		this.y = y;
		this.v = v;
	}

	void pushMyself(CostSurfaceCalculator csc) {
		final int index = PixelSizeMultiplier.NUM_BANDS * (x + y * csc.width); 
		float f1 = csc.rawCost[index];
		float v1 = csc.rawCost[index + 2];
		
		for (int i = 0; i < 8; i++)
		{
			final short xNeighbor = (short)(x + csc.dx[i]);
			final short yNeighbor = (short)(y + csc.dy[i]);
			if (xNeighbor >= 0 && xNeighbor < csc.width && yNeighbor >= 0 && yNeighbor < csc.height)
			{
				final int neighborIndex = PixelSizeMultiplier.NUM_BANDS * (xNeighbor + yNeighbor * csc.width);
				final float f2 = csc.rawCost[neighborIndex];
				final float v2 = csc.rawCost[neighborIndex + 2];

				if (f2 < 0 || Float.isNaN(f2))
				{
					continue;
				}
				float newCost = v1 + ((f1 + f2) * csc.dist2[i]);
				if (newCost < v2 || v2 < 0.0 || Float.isNaN(v2))
				{
					csc.heap.add(this);
					return;
				}
			}
		}
	}
	
	void pushNeighbors(CostSurfaceCalculator csc, ChangedPixelsMap changedPixels) {		
		final int index = PixelSizeMultiplier.NUM_BANDS * (x + y * csc.width); 
		final float fw1 = csc.rawCost[index];
		final float fh1 = csc.rawCost[index + 1];
		final float v1 = csc.rawCost[index + 2];

		for (int i = 0; i < 8; i++)
		{
			final short xNeighbor = (short)(x + csc.dx[i]);
			final short yNeighbor = (short)(y + csc.dy[i]);
			if (xNeighbor >= 0 && xNeighbor < csc.width && yNeighbor >= 0 && yNeighbor < csc.height)
			{
				final int neighborIndex = PixelSizeMultiplier.NUM_BANDS * (xNeighbor + yNeighbor * csc.width);
				final float fw2 = csc.rawCost[neighborIndex];
				final float fh2 = csc.rawCost[neighborIndex + 1];
				final float v2 = csc.rawCost[neighborIndex + 2];

				if (fw2 < 0 || fh2 < 0 || Float.isNaN(fw2) || Float.isNaN(fh2))
				{
					continue;
				}
				
				float newCost;
				if (csc.dx[i] == 0)
				{
					newCost = v1 + ((fh1 + fh2) * 0.5f);
				}
				else if (csc.dy[i] == 0)
				{
					newCost = v1 + ((fw1 + fw2) * 0.5f);
				}
				else
				{
					final float fws = fw1 + fw2;
					final float fhs = fh1 + fh2;
					final float f = (float) Math.sqrt(fws * fws + fhs * fhs);
					newCost = v1 + (f * 0.5f);
				}

				if ((newCost < v2 || v2 < 0.0 || (Float.isNaN(v2) && !Float.isNaN(newCost))) && (newCost < csc.maxCost || csc.maxCost < 0))
				{
					/* TODO - figure a way to reduce duplicate objects here - 
					 * the issue is that the changedPixelsMap needs to have the friction values in  
					 * there since that's the one shipped out, however, we don't want the heap to
					 * have PointWithFriction, because the majority of pixels are not border pixels
					 * So for border pixels, we end up creating duplicate objects - one of type 
					 * Point and another of type PointWithFriction
					 */
					Point neighbor = new Point(xNeighbor, yNeighbor, newCost);
					csc.heap.add(neighbor);
					//System.out.println("Adding point " + neighbor + ", ik=" + ik);
					
					if(LOG.isDebugEnabled()) {
					  LOG.debug(String.format(
					      "While inspecting Point %d,%d, updating neighbor %d,%d with " +
					          "newCost %.2f oldCost %.2f maxCost %.2f",
					          x,y,xNeighbor, yNeighbor, newCost, 
					          csc.rawCost[neighborIndex + 2], csc.maxCost));
					}
					
					csc.rawCost[neighborIndex + 2] = newCost;
					
					// don't bother doing this stuff if neighbor is not a border pixel
					if(xNeighbor == 0 
							|| xNeighbor == csc.width-1 
							|| yNeighbor == 0 
							|| yNeighbor == csc.height-1) {
						PointWithFriction neighborWithFriction 
						= new PointWithFriction(xNeighbor, yNeighbor, newCost, fh2, fw2); 					
						changedPixels.addPointIfOnBorder(neighborWithFriction, csc);
					}
				}
			}
		}		
	}	
	
	@Override
	public int compareTo(Point o)
	{
		if (v < o.v)
			return -1;
		if (v > o.v)
			return 1;
		return 0;
	}

	@Override
  public boolean equals(Object obj)
	{
		Point p = (Point) obj;

		return p != null && x == p.x && y == p.y;
	}

	@Override
  public int hashCode()
	{
		return x + y * 7;
	}

	@Override
	public String toString() {
		return String.format("%.2f,%d,%d",v,x,y);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeFloat(v);
		out.writeShort(x);
		out.writeShort(y);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		v = in.readFloat();
		x = in.readShort();
		y = in.readShort();
	}
	
}
