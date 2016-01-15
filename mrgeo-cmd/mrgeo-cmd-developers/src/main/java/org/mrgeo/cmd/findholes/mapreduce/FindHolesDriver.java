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
import org.apache.hadoop.mapreduce.Job;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.adhoc.AdHocDataProvider;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.data.image.MrsImageInputFormatProvider;
import org.mrgeo.data.tile.TiledInputFormatContext;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.mrgeo.utils.HadoopUtils;
import org.mrgeo.utils.LongRectangle;

import java.io.*;
import java.util.HashSet;
import java.util.Set;

public class FindHolesDriver {

	//private Configuration conf = null;

	public FindHolesDriver(){} // end constructor
	
	
	
	public boolean runJob(String input, String output, int zoom, ProviderProperties props, Configuration conf) throws Exception{

	    System.out.println("Input:     " + input);
	    System.out.println("Output:    " + output);
	    System.out.println("ZoomLevel: " + zoom);
		
		conf.set("zoom", Integer.toString(zoom));
		DataProviderFactory.saveProviderPropertiesToConfig(props, conf);

		MrsImageDataProvider midp = DataProviderFactory.getMrsImageDataProvider(input, AccessMode.READ, conf); 
		MrsImagePyramidMetadata mipm = midp.getMetadataReader().read();

		System.out.println("DP = " + midp.getClass().getCanonicalName());
		System.out.println("DP resource = " + midp.getResourceName());
		
		LongRectangle lr = mipm.getTileBounds(zoom);
		conf.set("bounds", lr.toDelimitedString());
		
		AdHocDataProvider ahdp = DataProviderFactory.createAdHocDataProvider(conf);
		conf.set("adhoc.provider", ahdp.getResourceName());		
		
		Job job = new Job(conf, "Find holes for " + input + " at zoom level " + zoom);
		conf = job.getConfiguration();

		// how to fake out loading core dependencies
		HadoopUtils.setJar(job, FindHolesDriver.class);
		
		job.setMapperClass(FindHolesMapper.class);
		job.setReducerClass(FindHolesReducer.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		//Properties props = new Properties();

		TiledInputFormatContext tifc = new TiledInputFormatContext(zoom, mipm.getTilesize(), input, props);
		MrsImageInputFormatProvider miifp = midp.getTiledInputFormatProvider(tifc);

		// this is key for setting up the input
		job.setInputFormatClass(miifp.getInputFormat(input).getClass());

		miifp.setupJob(job, null);
		
		ahdp.setupJob(job);
		
		// now set output
		AdHocDataProvider dummy = DataProviderFactory.createAdHocDataProvider(conf);

	    // mimic FileOutputFormat.setOutputPath(job, path);
	    conf.set("mapred.output.dir", dummy.getResourceName());
		
	    job.submit();
	    	    
	    boolean success = job.waitForCompletion(true);

	    dummy.delete();
	    
	    if(success){
	    	miifp.teardown(job);
	    	
	    	boolean[][] valid = new boolean[(int)lr.getHeight()][(int)lr.getWidth()];
	    	for(int y = 0; y < (int)lr.getHeight(); y++){
	    		for(int x = 0; x < (int)lr.getWidth(); x++){
	    			valid[y][x] = false;
	    		}
	    	}
	    	
	    	final int size = ahdp.size();
	        for (int i = 0; i < size; i++)
	        {
	          final InputStream stream = ahdp.get(i);
	          BufferedReader br = new BufferedReader(new InputStreamReader(stream));
	          // read values out of stream
	          String line;
	          while((line = br.readLine()) != null){

		          // format is "y: x x x x"
	        	  String[] vals = line.split(":");
	        	  int y = Integer.parseInt(vals[0]);
	        	  
	        	  if(vals.length == 1){
	        		  continue;
	        	  }
	        	  vals = vals[1].trim().split(" ");
	        	  for(String v : vals){
	        		  valid[y - (int)lr.getMinY()][Integer.parseInt(v) - (int)lr.getMinX()] = true;
	        	  }	        	  
	          }
	          
	          br.close();
	          stream.close();
	        }
	        ahdp.delete();
	        File outFile = new File(output);
	        PrintWriter pw = new PrintWriter(outFile);
	        StringBuffer sbMissing = new StringBuffer();
	        for(int y = 0; y < lr.getHeight(); y++){
	        	// y + lr.getMinY()
	        	boolean m = false;
	        	for(int x = 0; x < lr.getWidth(); x++){
	        		// x + lr.getMinX()
	        		if(valid[y][x]){
	        			pw.write("+");
	        		} else {
	        			m = true;
	        			sbMissing.append("(" + (x+lr.getMinX()) + "," + (y+lr.getMinY()) + ") ");
	        			pw.write("-");
	        		}
	        		
	        	}
	        	pw.write("\n");
	        	if(m){
	        		sbMissing.append("\n");
	        	}
	        }
	        if(sbMissing.length() > 0){
	        	pw.write("\n\n");
	        	pw.write(sbMissing.toString() + "\n");
	        }
	        pw.close();
	        return true;
	    }
	    
		return false;
	} // end runJob
	
} // end FindHolesDriver
