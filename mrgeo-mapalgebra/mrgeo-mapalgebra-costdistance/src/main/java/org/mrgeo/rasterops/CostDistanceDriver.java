package org.mrgeo.rasterops;

import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.image.ImageStats;
import org.mrgeo.image.MrsImage;
import org.mrgeo.image.MrsImagePyramid;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.adhoc.AdHocDataProvider;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.utils.HadoopUtils;
import org.mrgeo.utils.TMSUtils;
import org.mrgeo.utils.TMSUtils.Bounds;

import java.awt.image.Raster;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Cost Distance driver
 */
public class CostDistanceDriver implements Tool {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(CostDistanceDriver.class);
  /** Configuration */
  private Configuration conf;
  private Properties providerProperties;
 
  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public final int run(final String[] args) throws Exception {	  
	  LOG.debug("CostDistanceDriver args = " );
	  for(String arg : args)
		  LOG.debug(arg);
	  
	  SimpleOptionsParser parser = new SimpleOptionsParser(args);
	  
	  String argInputImage = parser.getOptionValue("inputpyramid");
	  String argInputZoomLevel = parser.getOptionValue("inputzoomlevel");
    int zoomLevel = Integer.valueOf(argInputZoomLevel);
	  String argOutputPyramid = parser.getOptionValue("outputpyramid");
  
	  float maxCost = -1;
	  if(parser.isOptionProvided("maxcost"))
		  maxCost = Float.valueOf(parser.getOptionValue("maxcost"));
	  
	  int argSupersteps = 30;
	  if(parser.isOptionProvided("supersteps"))
		  argSupersteps = Integer.valueOf(parser.getOptionValue("supersteps"));    
	  
	  // TODO - generate a more descriptive name based on source point and max cost
	  GiraphJob job = new GiraphJob(getConf(), "CostDistance");

	  HadoopUtils.setJar(job.getInternalJob(), this.getClass());
	  //job.getInternalJob().setJarByClass(CostDistanceDriver.class);

    // set zoo servers
	  setZooServersIfNecessary(job);

	  job.getConfiguration().setInt(CostDistanceVertex.MAX_SUPERSTEPS, argSupersteps);

	  // set max cost
	  job.getConfiguration().setFloat(CostDistanceVertex.MAX_COST, maxCost); 

	  job.getConfiguration().setWorkerContextClass(CostDistanceWorkerContext.class);

	  job.getConfiguration().setVertexClass(CostDistanceVertex.class);

	  LOG.debug("Using class " + job.getConfiguration().getVertexClass());
	  
	  job.getConfiguration().setVertexInputFormatClass(MrsImageVertexInputFormat.class);
	  
	  MrsImagePyramidMetadata meta = MrsImagePyramid.open(argInputImage.toString(),
	      providerProperties).getMetadata();
	  double minFriction = meta.getImageStats(zoomLevel, 0).min;

	  Bounds bounds = null;
	  if(parser.isOptionProvided("sourcepoints")) {
	    // set source points
	    String sourcePoints = parser.getOptionValue("sourcepoints");
	    job.getConfiguration().set(CostDistanceVertex.SOURCE_POINTS, sourcePoints); 

	    bounds = CostDistanceBoundsConfiguration.getBounds(sourcePoints, 
	                                                       maxCost, minFriction, 
	                                                       job.getConfiguration());
	  } 
	  else {
	    // TODO: This is a hack to keep from changing the parameters yet. The legacy inputs
	    // includes the zoom level at the end of the path, but it should be a separate
	    // parameter. So, at the moment, I just truncate the path to get to the pyramid path.
	    String rvName = parser.getOptionValue("rvpyramid");
	    String argRvZoomLevel = parser.getOptionValue("inputzoomlevel");
	    int rvZoomLevel = Integer.valueOf(argRvZoomLevel);
	    RasterizeVectorReader rvr = new RasterizeVectorReader();
	    rvr.read(rvName, rvZoomLevel, providerProperties);
	    String str = "";
	    for(long tileId : rvr.getTileIds()) {
	      str += (String.valueOf(tileId) + ",");
	    }
	    job.getConfiguration().set(CostDistanceVertex.RV_TILE_IDS, str);
	    job.getConfiguration().set(CostDistanceVertex.RV_PYRAMID, rvName);
      job.getConfiguration().setInt(CostDistanceVertex.RV_ZOOM_LEVEL, rvZoomLevel);
	    
	    String inputBoundsStr = parser.getOptionValue("inputbounds");
	    org.mrgeo.utils.Bounds oldBounds = org.mrgeo.utils.Bounds.fromDelimitedString(inputBoundsStr);
	    if(oldBounds.equals(org.mrgeo.utils.Bounds.world))
	    	bounds = rvr.getBounds();
	    else 
	    	bounds = Bounds.convertOldToNewBounds(oldBounds);
	    	
	    MrGeoProperties.getInstance().setProperty("giraph.disableAutoBounds", "true");
	    bounds = CostDistanceBoundsConfiguration.getBounds(bounds, 
	                                                       maxCost, minFriction, 
	                                                       job.getConfiguration());
	  }
	  MrsImageDataProvider.setupMrsPyramidSingleInputFormat(job.getInternalJob(), 
	                                                argInputImage,
	                                                zoomLevel, meta.getTilesize(),
	                                                bounds.convertNewToOldBounds(),
	                                                providerProperties);

	  // Debug note: To run cost distance locally for debugging purposes, comment
	  // out the following two lines and uncomment the lines below that hard-code the
	  // worker configuration to 1 and disable giraph.SplitMasterWorker
	  final int numWorkers = CostDistanceWorkersConfiguration.getNumWorkers(meta, job.getInternalJob().getConfiguration());
	  job.getConfiguration().setWorkerConfiguration(numWorkers, numWorkers, 100.0f);
//    job.getConfiguration().setWorkerConfiguration(1, 1, 100.0f);
//    job.getConfiguration().setBoolean("giraph.SplitMasterWorker", false);

    final AdHocDataProvider statsProvider = DataProviderFactory.createAdHocDataProvider(providerProperties);
    // get the ad hoc provider set up for map/reduce
    statsProvider.setupJob(job.getInternalJob());
	  job.getConfiguration().set(MrsImageVertexOutputFormat.STATS_PROVIDER,
	      statsProvider.getResourceName());
	  job.getConfiguration().setVertexOutputFormatClass(MrsImageVertexOutputFormat.class);
	  Path tmpOutputPath = HadoopFileUtils.createUniqueTmpPath();
	  
	  FileSystem fs = tmpOutputPath.getFileSystem(getConf());
    try
    {
  	  if (fs.exists(tmpOutputPath))
  	  {
  		  fs.delete(tmpOutputPath, true);
  	  }
  	  LOG.info("Outputting Giraph results to " + tmpOutputPath);
  	  FileOutputFormat.setOutputPath(job.getInternalJob(), tmpOutputPath);
  
  	  /* 
  	   * Setting the below key/value class doesn't work as expected and I'm not sure why
  	   * The only way to set parameters at the Hadoop job level seems to be through the 
  	   * configuration.  
  	   */
  	  //	  job.getInternalJob().setOutputKeyClass(TileIdWritable.class);
  	  //	  job.getInternalJob().setOutputValueClass(RasterWritable.class);
  	  job.getConfiguration().setClass("mapred.output.key.class", TileIdWritable.class, Object.class);
  	  job.getConfiguration().setClass("mapred.output.value.class", RasterWritable.class, Object.class);
    
  	  boolean isVerbose = true;
  	  if(!job.run(isVerbose))
  		  return -1;
  
//	  MrsImageDataProvider dp = DataProviderFactory.getMrsImageDataProvider(tmpOutputPath.toUri().toString(), AccessMode.READ);
//	  MrsImagePyramidMetadataReader metadataReader = dp.getMetadataReader();
//	  final MrsImagePyramidMetadata metadata = metadataReader.read();
  	  // TODO: The following has to be re-factored...
      final MrsImagePyramidMetadata metadata = HadoopUtils.getSingleMetadata(
                                              job.getInternalJob().getConfiguration()); 
      
      // update the pyramid level stats - note that max zoom level here would also work if the input 
      // zoom level was not the max zoom level of the friction surface, because BoundsCropper would
      // take care to set the input zoom level to the max zoom level in the metadata
      ImageStats[] levelStats = ImageStats.readStats(statsProvider);
      metadata.setImageStats(metadata.getMaxZoomLevel(), levelStats);
      //set the image level stats too which are the same as the max zoom level
      metadata.setStats(levelStats);
    
      // setup metadata in the next job configuration
      final Configuration config = getConf();
      HadoopUtils.setMetadata(config, metadata);
   
  	  // convert to pyramid job
  	  String hadoopJobArgs[] = new String[] {"-inputpyramid", tmpOutputPath.toUri().toString(),
  			  								                   "-outputpyramid", argOutputPyramid};
  	  int statusHadoopJob = ToolRunner.run(config, new ConvertToPyramidDriver(), hadoopJobArgs);
  	  
  	  LOG.info("Blowing away temporary giraph job results dir:  " + tmpOutputPath);
  	  LOG.info("Temporary convert-to-pyramid job results dir:  " + argOutputPyramid);

      return statusHadoopJob;
    }
    finally
    {
      fs.delete(tmpOutputPath, true);
      statsProvider.delete();
    }
  }

  private void setZooServersIfNecessary(GiraphJob job) {
	  final Properties properties = MrGeoProperties.getInstance();
	  
	  if(!properties.containsKey("hadoop.params") || 
			  !properties.getProperty("hadoop.params").contains("libjars")) {
		  LOG.info("Could not find libjars in \"hadoop.params\", looking for \"zooservers\"");


		  final String zooServers = properties.getProperty("zooservers");
		  if(zooServers == null) {		
			  throw new IllegalArgumentException(
					  "Please set zooservers to point to a list of host:port pairs - e.g., "
							  + "zooservers = \"host1:port1,host2:port2,host3:port3\"");
		  }
		  LOG.info("Using zooservers " + zooServers);

		  job.getConfiguration().set(GiraphConstants.ZOOKEEPER_LIST, zooServers);
	  }
  }
  
  public static class RasterizeVectorReader {
    private Bounds bounds;
    private List<Long> tileIds;
    
    public Bounds getBounds() {
      return bounds;
    }
    public List<Long> getTileIds() {
      return Collections.unmodifiableList(tileIds);
    }

    public void read(String rvPyramidStr, int zoomLevel,
        final Properties providerProperties) throws IOException
    {
      Path imagePath = new Path(rvPyramidStr);
      MrsImage image = null;
      try
      {
        image = MrsImage.open(imagePath.toUri().toString(), zoomLevel, providerProperties);
        final int tileSize = image.getTilesize();
        bounds = TMSUtils.tileToBounds(
            TMSUtils.TileBounds.convertFromLongRectangle(image.getTileBounds()), zoomLevel, tileSize);
        tileIds = new ArrayList<Long>();
        
        int totalTiles=0, emptyTiles=0, zeroTiles=0; 
        for (long ty = image.getMinTileY(); ty <= image.getMaxTileY(); ty++)
        {
          for (long tx = image.getMinTileX(); tx <= image.getMaxTileX(); tx++)
          {
            totalTiles++;
            final Raster rvRaster = image.getTile(tx, ty);
            
            if (rvRaster == null) {
              emptyTiles++;
              continue;
            }
            if(getCountZeroPixels(rvRaster) > 0) {
              long tileId = TMSUtils.tileid(tx, ty, zoomLevel);
              tileIds.add(tileId);
            } else {
              zeroTiles++;
            }
            
          }
        }
        
        // error checking
        if (tileIds.size() == 0)
        {
          throw new IllegalArgumentException(
              "CostDistance could not find any valid tiles in the raster produced by "
                  + "RasterizeVector - \"" + rvPyramidStr + "\".");
        }

        LOG.info(String.format("RasterizeVectorReader: Total tiles = %d, Empty tiles = %d, " + 
                                "Non-Zero Tiles = %d, Zero Tiles = %d", 
                                totalTiles, emptyTiles, tileIds.size(), zeroTiles));
        
        // error checking
        if((totalTiles - emptyTiles - tileIds.size()) != zeroTiles) {
          throw new IllegalStateException(
                        String.format("Zero tiles = %d whereas " + 
                                      "totalTiles - emptyTiles - tileIds.size() = %d",
                                      zeroTiles, (totalTiles - emptyTiles - tileIds.size())));
        }
                  
      }
      finally
      {
        if (image != null)
        {
          image.close();
        }
      }
    }
    
    private int getCountZeroPixels(Raster rvRaster) {
      int countZeroPixels = 0; 
      for(int py = 0; py < rvRaster.getHeight(); py++) {
        for(int px = 0; px < rvRaster.getWidth(); px++) {
          float v = rvRaster.getSampleFloat(px, py, 0);
          if(!Float.isNaN(v) && v == 0f) {
            countZeroPixels++;
          }           
        }      
      }   
      return countZeroPixels;
    }
  }
	
  
  /**
   * Execute the benchmark.
   *
   * @param args Typically the command line arguments.
   * @throws Exception Any exception from the computation.
   */
  public static void main(final String[] args) throws Exception {
    System.exit(ToolRunner.run(HadoopUtils.createConfiguration(),new CostDistanceDriver(), args));
  }
}

