package org.mrgeo.rasterops;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexOutputFormat;
import org.apache.giraph.io.VertexWriter;
import org.apache.giraph.io.formats.SequenceFileVertexInputFormat;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.log4j.Logger;
import org.mrgeo.image.ImageStats;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.adhoc.AdHocDataProvider;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.utils.HadoopUtils;

import java.awt.image.Raster;
import java.io.IOException;
import java.util.Arrays;

public class MrsImageVertexOutputFormat 
	extends VertexOutputFormat<TileIdWritable, 
								RasterWritable, 
								ByteWritable> {
  
	  @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(MrsImageVertexOutputFormat.class);
	  public static final String STATS_PROVIDER = "stats.provider";
	  
	/** Internal output format */
	  protected SequenceFileOutputFormat<WritableComparable<?>, Writable> outputFormat = 
	      new SequenceFileOutputFormat<WritableComparable<?>, Writable>();

	  @Override
	  public VertexWriter<TileIdWritable, 
	  						RasterWritable, 
	  						ByteWritable> 
	  			createVertexWriter(TaskAttemptContext context) 
	  					throws IOException, InterruptedException{
		  return new MrsImageVertexWriter(outputFormat.getRecordWriter(context));
	  }

	  /**
	   * Vertex reader used with {@link SequenceFileVertexInputFormat}.
	   *
	   * @param <I> Vertex id
	   * @param <V> Vertex data
	   * @param <E> Edge data
	   * @param <M> Message data
	   * @param <X> Value type
	   */
	  public static class MrsImageVertexWriter extends VertexWriter<TileIdWritable, 
	                                                                   RasterWritable, 
	                                                                   ByteWritable> {
	    /** Internal record writer from {@link SequenceFileOutputFormat} */
	    private final RecordWriter<WritableComparable<?>, Writable> recordWriter;

	    /* 
	     * These three fields are needed by stats collection. The use of TaskInputOutputContext 
	     * is non-obvious. The StatsUtils layer needs it to figure the task working directory. 
	     * Turns out that Giraph's VertexWriter layer, instead of getting TaskInputOutputContext, 
	     * gets a TaskAttemptContext. However, Giraph's WorkerContext has a TaskInputOutputContext,
	     * and hence we cache it here from the first vertex in writeVertex 
	     */
	    private MrsImagePyramidMetadata metadata;
	    private ImageStats[] stats;
	    private TaskInputOutputContext<?,?,?,?> mapperContext;
	    
	    public MrsImageVertexWriter(RecordWriter<WritableComparable<?>, Writable> recordWriter) {
	      this.recordWriter = recordWriter;
	    }

	    @Override
	    public void initialize(TaskAttemptContext context) throws IOException, InterruptedException {
	      metadata = HadoopUtils.getSingleMetadata(context.getConfiguration());

	      // Initialize ImageStats array
	      stats = ImageStats.initializeStatsArray(metadata.getBands());

	    }

	    @Override
	    public void writeVertex(
	        Vertex<TileIdWritable, RasterWritable, ByteWritable, ?> vertex)
	            throws IOException, InterruptedException {
	      InMemoryRasterWritable imrw = (InMemoryRasterWritable) vertex.getValue();
	      if(imrw.isInitialized()) {
	        if(mapperContext == null) {
	          mapperContext = vertex.getWorkerContext().getContext();
	        }
	        
	        Raster thirdBandRaster = imrw.getThirdBandRaster();
	        
	        // compute stats on the tile and aggregate across previous tiles
	        final ImageStats[] tileStats = ImageStats.computeStats(thirdBandRaster, 
	                                                          metadata.getDefaultValues());
	        stats = ImageStats.aggregateStats(Arrays.asList(stats, tileStats));

	        recordWriter.write(vertex.getId(), RasterWritable.toWritable(thirdBandRaster));
	      }
	    }



	    @Override
	    public void close(TaskAttemptContext context) throws IOException,  InterruptedException{
	      /* mapperContext can be null if the worker got no vertices to work on (e.g., more workers
	       * than splits), in which case, writeVertex is not called and mapperContext is not 
	       * initialized
	       */
	      if(mapperContext != null) {	        
	        String adhoc = mapperContext.getConfiguration().get(STATS_PROVIDER, null);
	        if (adhoc == null)
	        {
	          throw new IOException("Stats provider not set");
	          
	        }
	        AdHocDataProvider statsProvider = DataProviderFactory.getAdHocDataProvider(adhoc,
	            AccessMode.WRITE, context.getConfiguration());
	        ImageStats.writeStats(statsProvider, stats);
	      }
	      recordWriter.close(context);
	    }

	  }

	@Override
	public void checkOutputSpecs(JobContext context) throws IOException,
			InterruptedException {
		outputFormat.checkOutputSpecs(context);
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return outputFormat.getOutputCommitter(context);
	}
}

