package org.mrgeo.cmd.findholes.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.utils.TMSUtils;

public class FindHolesMapper extends Mapper<TileIdWritable, RasterWritable, LongWritable, LongWritable>{

	private int zoomlevel = -1;
	
	public void setup(Context context) throws IOException, InterruptedException{
		// get zoom level and bounds(tile space)
	} // end setup
	
	public void map(TileIdWritable key, RasterWritable value, Context context) throws IOException, InterruptedException{
		
		TMSUtils.Tile tile = TMSUtils.tileid(key.get(), zoomlevel);
		context.write(new LongWritable(tile.ty), new LongWritable(tile.tx));

	} // end map
	
} // end FindHolesMapper
