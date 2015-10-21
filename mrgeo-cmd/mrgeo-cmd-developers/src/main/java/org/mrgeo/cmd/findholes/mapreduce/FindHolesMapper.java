/*
 * Copyright 2009-2015 DigitalGlobe, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.mrgeo.cmd.findholes.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.utils.TMSUtils;

import java.io.IOException;

public class FindHolesMapper extends Mapper<TileIdWritable, RasterWritable, LongWritable, LongWritable>{

	private int zoomlevel = -1;	
	

	public void setup(Context context) throws IOException, InterruptedException{

		// get zoom level and bounds(tile space)
		Configuration conf = context.getConfiguration();
		zoomlevel = conf.getInt("zoom", -1);
		
	} // end setup

	
	public void map(TileIdWritable key, RasterWritable value, Context context) throws IOException, InterruptedException{
		
		TMSUtils.Tile tile = TMSUtils.tileid(key.get(), zoomlevel);
		context.write(new LongWritable(tile.ty), new LongWritable(tile.tx));

	} // end map
	
	
} // end FindHolesMapper
