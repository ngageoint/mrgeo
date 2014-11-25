package org.mrgeo.rasterops;

import org.mrgeo.rasterops.CostDistanceVertex.Position;
import org.mrgeo.utils.TMSUtils;

import java.util.ArrayList;
import java.util.List;

public class EdgeBuilder {
	TMSUtils.Tile tileMin; 
	TMSUtils.Tile tileMax;
	int zoomlevel;
	
	public static class PositionEdge {
		Position position;
		long targetVertexId;
				
		public PositionEdge(Position position, long targetVertexId) {
			super();
			this.position = position;
			this.targetVertexId = targetVertexId;
		}
		@Override
    public String toString() {
			return String.format("[%s: %d]",position,targetVertexId);
		}
	}
	
	public EdgeBuilder(long minTx, long minTy, long maxTx, long maxTy, int zoomlevel) {
		this.zoomlevel = zoomlevel;
		
		tileMin = new TMSUtils.Tile(minTx, minTy);
		tileMax = new TMSUtils.Tile(maxTx, maxTy);
	}
	public EdgeBuilder(long tileIdMin, long tileIdMax, int zoomlevel) {
		this.zoomlevel = zoomlevel;
		
		tileMin = TMSUtils.tileid(tileIdMin, zoomlevel);
		tileMax = TMSUtils.tileid(tileIdMax, zoomlevel);
	}
	public List<PositionEdge> getNeighbors(long tileId) {
		TMSUtils.Tile tile = TMSUtils.tileid(tileId, zoomlevel);
		
		long[] offsets = {-1,0,1};
		

		List<PositionEdge> neighbors = new ArrayList<PositionEdge>();
		for(long dy : offsets) {
			for(long dx : offsets) {
				
				if(dx == 0 && dy == 0)
					continue;
				
//				if(excludedOffsets(dx,dy))
//					continue;
				long neighborTx = tile.tx + dx;
				long neighborTy = tile.ty + dy;

				/*
				 * TODO - currently there is a bug in our bounds calculation, which sets the 
				 * max tx/ty to 1 + true max. So our indices stop at tileMax.tx - 1
				 * 
				 * The above message is no longer relevant but I'm keeping it there to be reminded
				 * if we run into bounds calculation issues in the future - for now, I'm taking my 
				 * indices all the way to tileMax.tx and tileMax.ty
				 */
				if(neighborTx >= tileMin.tx && neighborTx <= tileMax.tx 
						&& neighborTy >= tileMin.ty && neighborTy <= tileMax.ty) {
					long neighborId = TMSUtils.tileid(neighborTx, neighborTy, zoomlevel);
					
					Position position = Position.SELF;
					if(dx == -1 && dy == 1)
						position = Position.TOPLEFT;
					else if(dx == 0 && dy == 1)
						position = Position.TOP;
					else if(dx == 1 && dy == 1)
						position = Position.TOPRIGHT;
					else if(dx == 1 && dy == 0)
						position = Position.RIGHT;
					else if(dx == 1 && dy == -1)
						position = Position.BOTTOMRIGHT;
					else if(dx == 0 && dy == -1)
						position = Position.BOTTOM;
					else if(dx == -1 && dy == -1)
						position = Position.BOTTOMLEFT;
					else if(dx == -1 && dy == 0)
						position = Position.LEFT;
					else 
						throw new IllegalStateException(
									String.format("Unexpected dx/dy in EdgeBuilder %d/%d",dx,dy));			

					neighbors.add(new PositionEdge(position,neighborId));
				}
			}
		}
		if(neighbors.isEmpty())
			return null;
		return neighbors;
	}
	boolean excludedOffsets(long dx, long dy) {
		if( (dx == 0 && dy == 0) )
				return true;
		return false;

//				|| (dx == -1 && dy == -1)
//				|| (dx == -1 && dy == 1)
//				|| (dx == 1 && dy == 1)
//				|| (dx == 1 && dy == -1))
	}
}
