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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.adhoc.AdHocDataProvider;
import org.mrgeo.utils.LongRectangle;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;


public class FindHolesReducer extends Reducer<LongWritable, LongWritable, Text, Text>{

	private LongRectangle lr = null;
	private long minX, minY, maxX, maxY;
	private long width;
	
	private OutputStream os = null;
	private PrintWriter pw = null;
	
	
	public void setup(Context context) throws IOException, InterruptedException{
		// get zoom level and bounds(tile space)
		Configuration conf = context.getConfiguration();
		String bounds = conf.get("bounds");
		String[] vals = bounds.split(",");
		if(vals.length == 4){
			minX = Long.parseLong(vals[0]);
			minY = Long.parseLong(vals[1]);
			maxX = Long.parseLong(vals[2]);
			maxY = Long.parseLong(vals[3]);
		}
		
		width = maxX - minX + 1;

		String adhoc = conf.get("adhoc.provider");
		AdHocDataProvider provider = DataProviderFactory.getAdHocDataProvider(adhoc,
		        AccessMode.WRITE, context.getConfiguration());
		os = provider.add();
		pw = new PrintWriter(os);
		
		
	} // end setup

	
	public void cleanup(Context context) throws IOException, InterruptedException{
		
		pw.close();
		os.close();
		
	} // cleanup
	
	
	/**
	 * output of this reducer is:
	 * y: x x x x x x x x
	 */
	public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException{
		
		int[] valid = new int[(int)width];
		pw.print(key.toString() + ":");
		for(LongWritable v : values){
			int indx = (int)(v.get() - minX);
			valid[indx]++;
			pw.print(" " + Long.toString(v.get()));
			
		}
		pw.println();
		
	} // end reduce

	
} // end FindHolesReducer
