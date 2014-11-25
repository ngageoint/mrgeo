package org.mrgeo.rasterops;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

public class CostDistanceMessage implements Writable {
	// relative position of receiving tile to sending tile, so if position==BOTTOM and message 
	// sent from tile 1 to tile 2, then tile 2 is below tile 1
	CostDistanceVertex.Position position;
	ArrayList<PointWithFriction> points;
	
	public CostDistanceMessage() {
		points = new ArrayList<PointWithFriction>();
	}
	
	public CostDistanceMessage(CostDistanceVertex.Position position,
			ArrayList<PointWithFriction> points) {
		super();
		this.position = position;
		this.points = points;
	}

	public CostDistanceVertex.Position getPosition() {
		return position;
	}

	public void setPosition(CostDistanceVertex.Position position) {
		this.position = position;
	}

	public ArrayList<PointWithFriction> getPoints() {
		return points;
	}

	public void setPoints(ArrayList<PointWithFriction> points) {
		this.points = points;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		CostDistanceVertex.Position.write(position, out);
	    int numValues = points.size();
	    out.writeInt(numValues);                 // write number of values
	    for (int i = 0; i < numValues; i++) {
	      points.get(i).write(out);
	    }
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		position = CostDistanceVertex.Position.read(in);

	    int numValues = in.readInt();  
	    points.clear();
	    points.ensureCapacity(numValues);
	    for (int i = 0; i < numValues; i++) {
	      PointWithFriction point = new PointWithFriction();
	      point.readFields(in);                
	      points.add(point);                          
	    }		
	}
	@Override
  public String toString() {
		String str = "";
		str = position.toString() + ":";
		for(int i=0; i < points.size(); i++)
			str += points.get(i).toString();
		return str;
	}

}
