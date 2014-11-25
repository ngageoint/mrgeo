package org.mrgeo.cmd.generatesplitfile;

import java.util.Properties;

import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.mrgeo.cmd.Command;
import org.mrgeo.mapreduce.partitioners.ImageSplitGenerator;
import org.mrgeo.mapreduce.partitioners.TileIdPartitioner;
import org.mrgeo.hdfs.tile.SplitFile;
import org.mrgeo.utils.LongRectangle;

/**
 * A utility class to build split files on the command line
 * 
 * GenerateSplitFile -splitfiledir <path> (e.g., file:///tmp or /mrgeo/images - latter assumed to be
 * hdfs) -mintx <num> -minty <num> -maxtx <num> -maxty <num> -zoomlevel <num> -increment <num>
 * 
 */
public class GenerateSplitFile extends Command
{

  @SuppressWarnings("static-access")
  @Override
  public int run(final String[] args, final Configuration conf,
      final Properties providerProperties)
  {
    try
    {
      final Options options = new Options();
      options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription(
        "hdfs or local path to directory into which split file would be copied").create(
        "splitfiledir"));
      options.addOption(OptionBuilder.withArgName("tx").hasArg().withDescription("minimum tx")
        .create("mintx"));
      options.addOption(OptionBuilder.withArgName("ty").hasArg().withDescription("maximum tx")
        .create("minty"));
      options.addOption(OptionBuilder.withArgName("tx").hasArg().withDescription("minimum ty")
        .create("maxtx"));
      options.addOption(OptionBuilder.withArgName("ty").hasArg().withDescription("maximum ty")
        .create("maxty"));
      options.addOption(OptionBuilder.withArgName("zoomlevel").hasArg().withDescription(
        "zoom level").create("zoomlevel"));
      options.addOption(OptionBuilder.withArgName("num").hasArg().withDescription(
        "number of rows to lump in a single partition").create("increment"));
      final OptionsParser parser = new OptionsParser(args, options);

      final String output = parser.getOptionValue("splitfiledir");
      final LongRectangle tileBounds = new LongRectangle(Long.valueOf(parser
        .getOptionValue("mintx")), Long.valueOf(parser.getOptionValue("minty")), Long
        .valueOf(parser.getOptionValue("maxtx")), Long.valueOf(parser.getOptionValue("maxty")));
      final int zoomLevel = Integer.valueOf(parser.getOptionValue("zoomlevel"));
      final int increment = Integer.valueOf(parser.getOptionValue("increment"));

      final Job job = new Job(conf, "Fake job");

      final Path splitFileTmp = TileIdPartitioner.setup(job, new ImageSplitGenerator(tileBounds
        .getMinX(), tileBounds.getMinY(), tileBounds.getMaxX(), tileBounds.getMaxY(), zoomLevel,
        increment));
      if (splitFileTmp != null)
      {
        final SplitFile sf = new SplitFile(job.getConfiguration());
        sf.copySplitFile(splitFileTmp.toString(), output);
      }
      return 0;
    }
    catch (final Exception e)
    {
      e.printStackTrace();
    }

    return -1;
  }
}
