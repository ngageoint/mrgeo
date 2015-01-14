package org.mrgeo.cmd.findholes.mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.adhoc.AdHocDataProvider;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.data.image.MrsImageInputFormatProvider;
import org.mrgeo.data.tile.TiledInputFormatContext;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.mrgeo.utils.DependencyLoader;
import org.mrgeo.utils.HadoopUtils;
import org.mrgeo.utils.LongRectangle;

public class FindHolesDriver {

	//private Configuration conf = null;

	public FindHolesDriver(){} // end constructor
	
	
	
	public boolean runJob(String input, String output, int zoom, Properties props, Configuration conf) throws Exception{

	    System.out.println("Input:     " + input);
	    System.out.println("Output:    " + output);
	    System.out.println("ZoomLevel: " + zoom);
		
		conf.set("zoom", Integer.toString(zoom));
		if(props != null && props.getProperty(DataProviderFactory.PROVIDER_PROPERTY_USER_ROLES) != null){
			conf.set(DataProviderFactory.PROVIDER_PROPERTY_USER_ROLES, props.getProperty(DataProviderFactory.PROVIDER_PROPERTY_USER_ROLES));
		}
		
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
		//HadoopUtils.setJar(job, FindHolesDriver.class);
		HadoopUtils.setJar(job, HadoopUtils.class);
		//DependencyLoader.getDependencies(FindHolesMapper.class);
		//DependencyLoader.getDependencies(FindHolesReducer.class);
		
		job.setMapperClass(FindHolesMapper.class);
		job.setReducerClass(FindHolesReducer.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		//Properties props = new Properties();
		Set<String> inputs = new HashSet<String>();
		inputs.add(input);
		
		TiledInputFormatContext tifc = new TiledInputFormatContext(zoom, mipm.getTilesize(), inputs, props);
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
	    	
	    	final int size = ahdp.size();
	        for (int i = 0; i < size; i++)
	        {
	          final InputStream stream = ahdp.get(i);
	          BufferedReader br = new BufferedReader(new InputStreamReader(stream));
	          // read values out of stream
	          String line;
	          while((line = br.readLine()) != null){
	        	  String[] vals = line.split(":");
	        	  int y = Integer.parseInt(vals[0]);
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
	        for(int y = 0; y < lr.getHeight(); y++){
	        	// y + lr.getMinY()	        	
	        	for(int x = 0; x < lr.getWidth(); x++){
	        		// x + lr.getMinX()
	        		if(!valid[y][x]){
	        			pw.write("(" + (x+lr.getMinX()) + "," + (y+lr.getMinY()) + ") " );
	        		}
	        	}
	        }
	        pw.close();
	        return true;
	    }
	    
	    
		return false;
	} // end runJob
	
	
} // end FindHolesDriver
