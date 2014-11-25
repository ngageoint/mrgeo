package org.mrgeo.cmd.generatesplitfile;

import org.apache.commons.cli.*;

import java.util.Collection;

public class OptionsParser
{
  private final CommandLine line;

  public OptionsParser(final String[] args) throws ParseException
  {
    this(args, new Options());
  }

  public OptionsParser(final String[] args, final Options options) throws ParseException
  {
    final CommandLineParser parser = new GnuParser();

    // start with default options
    final Options allOptions = getOptions();

    // add any provided by user
    final Collection<Option> userOptions = (options.getOptions());
    for (final Option option : userOptions)
    {
      allOptions.addOption(option);
    }
    line = parser.parse(allOptions, args);
  }

  @SuppressWarnings("static-access")
  private static Options getOptions()
  {
    final Options options = new Options();
    options.addOption(OptionBuilder.withArgName("instance").hasArg().withDescription(
      "accumulo instance name").create("instance"));
    options.addOption(OptionBuilder.withArgName("zooservers").hasArg().withDescription(
      "comma separated list of server:port").create("zooservers"));
    options.addOption(OptionBuilder.withArgName("user").hasArg().withDescription(
      "accumulo user name").create("user"));
    options.addOption(OptionBuilder.withArgName("password").hasArg().withDescription(
      "accumulo password").create("password"));
    options.addOption(OptionBuilder.withArgName("table").hasArg()
      .withDescription("use given table").create("table"));
    options.addOption(OptionBuilder.withArgName("file").hasArg()
      .withDescription("path to key file").create("keyfile"));
    options.addOption(OptionBuilder.withArgName("file").hasArg().withDescription(
      "path to directory used to cache random key file").create("randomkeyfiledir"));
    options.addOption(OptionBuilder.withArgName("num").hasArg().withDescription(
      "number of scan threads").create("threads"));
    options.addOption(OptionBuilder.withArgName("num").hasArg().withDescription("number of tiles")
      .create("tiles"));
    options.addOption(OptionBuilder.withArgName("tileid").hasArg().withDescription(
      "minimum tileId in range").create("mintile"));
    options.addOption(OptionBuilder.withArgName("tileid").hasArg().withDescription(
      "maximum tileId in range").create("maxtile"));
    options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription(
      "path to input pyramid").create("inputpyramid"));
    options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription(
      "path to output pyramid").create("outputpyramid"));
    options.addOption(OptionBuilder.withArgName("inputs").hasArg().withDescription(
      "list of input paths or tables").create("inputs"));
    options.addOption(OptionBuilder.withArgName("source").hasArg().withDescription(
      "accumulo, hdfs, or hdfs_old").create("datasource"));
    options.addOption(OptionBuilder.withArgName("zoomlevel").hasArg().withDescription("zoom level")
      .create("zoomlevel"));
    options.addOption(OptionBuilder.withArgName("workers").hasArg().withDescription(
      "number of giraph workers").create("workers"));
    options.addOption(OptionBuilder.withArgName("supersteps").hasArg().withDescription(
      "number of supersteps").create("supersteps"));
    options.addOption(OptionBuilder.withArgName("points").hasArg().withDescription(
      "comma-separated list of source points (e.g., \"(33.34,100.34),(30.39,90.34)\"").create(
      "sourcepoints"));
    options.addOption(OptionBuilder.withArgName("cost").hasArg().withDescription("maximum cost")
      .create("maxcost"));
    options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription(
      "Raster output by RasterizeVector").create("rvpyramid"));
    return options;
  }

  public String getOptionValue(final String option) throws ParseException
  {
    final String value = line.getOptionValue(option);
    if (value == null)
    {
      throw new ParseException("expected option \"" + option + "\" is missing");
    }
    return value;
  }

  public boolean isOptionProvided(final String option)
  {
    return ((line.getOptionValue(option) == null) ? false : true);
  }
}
