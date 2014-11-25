package org.mrgeo.rasterops;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.data.image.MrsImageOutputFormatProvider;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.utils.HadoopUtils;

import java.io.OutputStream;
import java.util.Map;
import java.util.Properties;

public class ConvertToPyramidDriver extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(ConvertToPyramidDriver.class);

	@Override
	public int run(String[] args) throws Exception {
		LOG.debug("ConvertToPyramidDriver args = ");
		for(String arg : args)
			LOG.debug(arg);
		
		SimpleOptionsParser parser = new SimpleOptionsParser(args);

		String argInputPyramid = parser.getOptionValue("inputpyramid");
		String argOutputPyramid = parser.getOptionValue("outputpyramid");
		
		//run job
		Job job = org.mrgeo.mapreduce.MapReduceUtils.createTiledJob("ConvertToPyramidDriver", getConf());
		HadoopUtils.setJar(job, this.getClass());

		job.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(argInputPyramid));

		// Opening the image in OVERWRITE mode will delete it if it already exists
		MrsImageDataProvider dp = DataProviderFactory.getMrsImageDataProvider(argOutputPyramid, AccessMode.OVERWRITE, getConf());

    // Use identity mapper and reducer
		job.setOutputKeyClass(TileIdWritable.class);
		job.setOutputValueClass(RasterWritable.class);    

    Map<String, MrsImagePyramidMetadata> meta = HadoopUtils.getMetadata(job.getConfiguration());
    final MrsImagePyramidMetadata metadata =  meta.values().iterator().next();

    MrsImageOutputFormatProvider ofProvider = MrsImageDataProvider.setupMrsPyramidOutputFormat(
        job, argOutputPyramid, 
      metadata.getBounds(), metadata.getMaxZoomLevel(), metadata.getTilesize(), metadata.getTileType(),
      metadata.getBands(),
      (Properties)null);

		boolean isVerbose = true;
		int statusHadoopJob = job.waitForCompletion(isVerbose) ? 0 : -1;

		ofProvider.teardown(job);

		// write the metadata
		dp.getMetadataWriter().write(metadata);

		return statusHadoopJob;
	}
	public static void main(final String[] args) throws Exception {
		System.exit(ToolRunner.run(new ConvertToPyramidDriver(), args));
	}
}
