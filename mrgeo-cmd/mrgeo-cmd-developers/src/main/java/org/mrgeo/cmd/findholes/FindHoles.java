package org.mrgeo.cmd.findholes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.mrgeo.cmd.Command;
import org.mrgeo.cmd.findholes.mapreduce.FindHolesDriver;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.utils.HadoopUtils;
import org.mrgeo.utils.LoggingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * options are output directior and zoom level
 * 
 * @author andrew
 *
 */
public class FindHoles extends Command{

	private Options options;
	private static Logger log = LoggerFactory.getLogger(FindHoles.class);
	
	private int zoomLevel = -1;
	private String out = null;
	private String inputImage = null;
	
	public FindHoles(){
		
		options = createOptions();
		
	} // end constructor

	
	public static Options createOptions(){
		Options retOpt = new Options();

		Option output = new Option("o", "output", true, "MrsImagePyramid image name");
	    output.setRequired(true);
	    retOpt.addOption(output);
		
	    Option zoomLevel = new Option("z", "zoomlevel", true, "Zoom Level to check for the image");
	    zoomLevel.setRequired(true);
	    retOpt.addOption(zoomLevel);

	    Option lcl = new Option("l", "local-runner", false, "Use Hadoop's local runner (used for debugging)");
	    lcl.setRequired(false);
	    retOpt.addOption(lcl);

	    Option roles = new Option("r", "roles", true, "User roles used for access to data.");
	    roles.setRequired(false);
	    retOpt.addOption(roles);
	    
	    retOpt.addOption(new Option("v", "verbose", false, "Verbose logging"));
	    retOpt.addOption(new Option("d", "debug", false, "Debug (very verbose) logging"));
	    
		return retOpt;
	} // end createOptions
	
	@Override
	public int run(String[] args, Configuration conf,
			Properties providerProperties) {

		CommandLine line = null;
		try
		{
			CommandLineParser parser = new GnuParser();
	        line = parser.parse(options, args);
		}
		catch (ParseException e)
		{
			System.out.println(e.getMessage());
	        new HelpFormatter().printHelp("findholes <options> <input>", options);
	        return -1;
		}
		
		
		if (line != null)
		{
			if (line.hasOption("v"))
	        {
				LoggingUtils.setDefaultLogLevel(LoggingUtils.INFO);
	        }
	        if (line.hasOption("d"))
	        {
	        	LoggingUtils.setDefaultLogLevel(LoggingUtils.DEBUG);
	        }
		
		
			if (line.hasOption("l"))
	        {
	          System.out.println("Using local runner");
	          try{
	        	  HadoopUtils.setupLocalRunner(conf);
	          } catch(IOException ioe){
	        	  ioe.printStackTrace();
	        	  return -1;
	          }
	        }
	
	
			String tmp = line.getOptionValue("z");
			zoomLevel = Integer.parseInt(tmp);
			out = line.getOptionValue("o");

			// DataProviderFactory.PROVIDER_PROPERTY_USER_ROLES
			Properties props = new Properties();
			if(line.hasOption("r")){
				props.setProperty(DataProviderFactory.PROVIDER_PROPERTY_USER_ROLES, line.getOptionValue("r"));
			}
			
			
			List<String> al = line.getArgList();
			System.out.print("Input:     ");
			for(String a: al){
				System.out.print(a + " ");
			}
			System.out.println();
			System.out.println("Output:    " + out);
			System.out.println("ZoomLevel: " + zoomLevel);
			
			System.out.println();
			
			FindHolesDriver fhd = new FindHolesDriver();
			try{
				fhd.runJob(al.get(0), out, zoomLevel, props, conf);
			} catch(Exception e){
				e.printStackTrace();
				return -1;
			}
			
			return 0;
		}
		return -1;
	} // end main

}  // end PrintKeys
