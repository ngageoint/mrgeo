package org.mrgeo.rasterops;

import org.apache.log4j.Logger;
import org.mrgeo.rasterops.CostDistanceVertex.Position;
import org.mrgeo.rasterops.CostSurfaceCalculator.ChangedPixelsMap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


//Package Visible
class PointWithFriction extends Point {
	private static final Logger LOG = Logger.getLogger(PointWithFriction.class);

	float fw,fh; 
	Position position;
	
	PointWithFriction() {
		super();
		fw=fh=Float.NaN;
		position = Position.SELF;
	}

	PointWithFriction(short x, short y, float v) {
		super(x, y, v);
	}
	
	PointWithFriction(short x, short y, float v, float fh, float fw) {
		super(x, y, v);
		this.fw = fw;
		this.fh = fh;
	}
	
	PointWithFriction(short x, short y, float v, float fh, float fw, Position position) {
		this(x, y, v, fh, fw);
		this.position = position;
	}	
	
	@Override
  void pushMyself(CostSurfaceCalculator csc) {
		/* take this out once algorithm is tested */
		if(position == Position.SELF)
			throw new IllegalStateException("Oops - position is not initialized in pushMyself");
		csc.heap.add(this);
	}

	@Override
  void pushNeighbors(CostSurfaceCalculator csc, ChangedPixelsMap changedPixels) {
		/* take this out once algorithm is tested */
		if(position == Position.SELF)
			throw new IllegalStateException("Oops - position is not initialized in pushNeighbors");
		

		short xNeighbors[];
		short yNeighbors[];
		final short widthMinusOne = (short)(csc.width - 1);
		final short xMinusOne = (short)(x - 1);
		final short xPlusOne = (short)(x + 1);
		final short yMinusOne = (short)(y - 1);
		final short yPlusOne = (short)(y + 1);
		
    /* We have to be cognizant of rasters having their 0,0 on the upper left, so a point 
     * with py = 0 is actually on the top border, whereas py = 511 is on the bottom border,
		 * so neighbors of py=0 would have yNeighbors=width-1 if current tile (tile containing
		 * yNeighbor is to the TOP/TOPLEFT/TOPRIGHT of tile from where x/y were sent
		 */  
		switch(position) {
		case TOPLEFT:
			assert(x == 0 && y == 0);
			xNeighbors = new short[] {widthMinusOne};
			yNeighbors = new short[] {widthMinusOne};
			break;
		case TOP:
			assert(x >= 0 && x <= widthMinusOne && y == 0);
			xNeighbors = new short[] {xMinusOne, x, xPlusOne};
			yNeighbors = new short[] {widthMinusOne};		
			break;
		case TOPRIGHT:
			assert(x == widthMinusOne && y == 0);
			xNeighbors = new short[] {0};
			yNeighbors = new short[] {widthMinusOne};
			break;
		case RIGHT:
			assert(x == widthMinusOne && y >= 0 && y <= widthMinusOne);
			xNeighbors = new short[] {0};
			yNeighbors = new short[] {yMinusOne, y, yPlusOne};
			break;
		case BOTTOMRIGHT:
			assert(x == widthMinusOne && y == widthMinusOne);
			xNeighbors = new short[] {0};
			yNeighbors = new short[] {0};
			break;
		case BOTTOM:			
			assert(x >= 0 && x <= widthMinusOne && y == widthMinusOne);
			xNeighbors = new short[] {xMinusOne, x, xPlusOne};
			yNeighbors = new short[] {0};		
			break;
		case BOTTOMLEFT:
			assert(x == 0 && y == widthMinusOne);
			xNeighbors = new short[] {widthMinusOne};
			yNeighbors = new short[] {0};
			break;		
		case LEFT:
			assert(x == 0 && y >= 0 && y <= widthMinusOne);
			xNeighbors = new short[] {widthMinusOne};
			yNeighbors = new short[] {yMinusOne, y, yPlusOne};
			break;
		default:
			throw new IllegalStateException("Illegal position in pushNeighbors - " + position);
		}
		
		for(short xNeighbor : xNeighbors) {
			for(short yNeighbor : yNeighbors) {
				if(xNeighbor < 0 || xNeighbor > widthMinusOne || 
						yNeighbor < 0 || yNeighbor > widthMinusOne)
					continue;
				final int neighborIndex = PixelSizeMultiplier.NUM_BANDS * (xNeighbor + yNeighbor * csc.width);
				
				assert(neighborIndex >=0 && neighborIndex < csc.rawCost.length);

				final float fw2 = csc.rawCost[neighborIndex];
				final float fh2 = csc.rawCost[neighborIndex + 1];
				final float v2 = csc.rawCost[neighborIndex + 2];

				if (fw2 < 0 || fh2 < 0 || Float.isNaN(fw2) || Float.isNaN(fh2)) {
					continue;
				}
				float newCost;

				if (xNeighbor == x) {
					newCost = v + ((fh + fh2) * 0.5f);
				}
				else if (yNeighbor == y) {
					newCost = v + ((fw + fw2) * 0.5f);
				}
				else {
					final float fws = fw + fw2;
					final float fhs = fh + fh2;
					final float f = (float) Math.sqrt(fws * fws + fhs * fhs);
					newCost = v + (f * 0.5f);
				}

				if (	(newCost < v2 || v2 < 0.0 || Float.isNaN(v2)) && 
						(newCost < csc.maxCost || csc.maxCost < 0)) {
					Point neighbor = new Point(xNeighbor, yNeighbor, newCost);
					csc.heap.add(neighbor);
					
					if(LOG.isDebugEnabled()) {
					  LOG.debug(String.format(
					      "While inspecting PointWithFriction %d,%d, updating neighbor %d,%d " +
					          "with newCost %.2f oldCost %.2f maxCost %.2f",
					          x, y, xNeighbor, yNeighbor, newCost, 
					          csc.rawCost[neighborIndex + 2], csc.maxCost));
					}
					
					csc.rawCost[neighborIndex + 2] = newCost;

					// At this point, we know that we have to update the ChangedPixelMap, since
					// a) the point in context here is a PointWithFriction sent from another tile 
					// b) the border pixel (neighbor) cost has changed, it needs to be added to map
					PointWithFriction neighborWithFriction 
						= new PointWithFriction(xNeighbor, yNeighbor, newCost, fh2, fw2); 					
					changedPixels.addPointIfOnBorder(neighborWithFriction, csc);
				}
			}
		}
	}

	public static PointWithFriction createPointWithFriction(short x, short y, float v, float fh, float fw) {
		return new PointWithFriction(x, y, v, fh, fw);
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeFloat(fw);
		out.writeFloat(fh);
		super.write(out);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		fw = in.readFloat();
		fh = in.readFloat();
		super.readFields(in);
	}
	@Override
  public String toString() {
		return String.format("[%.2f,%.2f,%s]",fw,fh,super.toString());
	}
}
