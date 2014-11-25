package org.mrgeo.cmd;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.mrgeo.utils.HadoopUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VectorizeRaster extends Configured implements Tool
{
  private static Logger _log;

//  public static MrsPyramidv1 createPyramid(Configuration conf, String file) throws Exception
//  {
//    OpImageRegistrar.registerMrGeoOps();
//    Path output = HadoopFileUtils.createUniqueTmpPath();
//
//    RenderedImageMapOp image = new RenderedImageMapOp();
//    image.getParameters().add(file);
//    image.setRenderedImageFactory(new GeoTiffDescriptor());
//    RenderedImage ri = GeoTiffDescriptor.create(file, null);
//    image.setDefaultNoDataValue(0, OpImageUtils.getNoData(ri, Double.NaN));
//
//    RenderedImageMapOp convert = new RenderedImageMapOp();
//    convert.getParameters().add(file);
//    convert.setRenderedImageFactory(new ConvertToFloatDescriptor());
//    convert.addInput(image);
//    convert.setUseCache(true);
//
//    MapAlgebraExecutionerv1 exec = new MapAlgebraExecutionerv1();
//    exec.setOutputPath(output);
//    exec.setRoot(convert);
//    ProgressHierarchy progress = new ProgressHierarchy();
//    exec.execute(conf, progress);
//
//    return MrsPyramidv1.loadPyramid(output);
//  }

  public static Options createOptions()
  {
    Options result = new Options();

    Option input = new Option("i", "input", true, "Input raster image");
    input.setRequired(true);
    result.addOption(input);

    Option output = new Option("o", "output", true, "Output vector data");
    output.setRequired(true);
    result.addOption(output);

    Option threshold = new Option("t", "threshold", true, "Threshold column for vector output");
    output.setRequired(false);
    result.addOption(threshold);

    return result;
  }

  public static void main(String[] args) throws Exception
  {
    int res = ToolRunner.run(HadoopUtils.createConfiguration(), new VectorizeRaster(), args);
    System.exit(res);
  }

  @Override
  public int run(String[] args) throws Exception
  {
    _log = LoggerFactory.getLogger(VectorizeRaster.class);

    Options options = createOptions();
    CommandLine line = null;
    try
    {
      CommandLineParser parser = new PosixParser();
      line = parser.parse(options, args);
    }
    catch (ParseException e)
    {
      System.out.println(e.getMessage());
      System.out.println();
      new HelpFormatter().printHelp("mrgeo VectorizeRaster", options);
      return 1;
    }

    String inputImage = line.getOptionValue("i");
    String output = line.getOptionValue("o");
    double threshold = 0.0;
    if (options.hasOption("t"))
    {
      threshold = Double.valueOf(line.getOptionValue("t"));
    }
    Path outputPath = new Path(output);

    if (inputImage.isEmpty())
    {
      System.out.println("Input image must be specified.");
      System.out.println();
      new HelpFormatter().printHelp("mrgeo VectorizeRaster", options);
      return 1;
    }
    _log.info(inputImage);

    
    new HelpFormatter().printHelp("mrgeo VectorizeRaster", options);
    return 1;

//    FileSystem fs = HadoopFileUtils.getFileSystem(getConf());
//    if (fs.exists(outputPath))
//    {
//      fs.delete(outputPath, true);
//    }
//
//    MrsPyramidv1 image = null;
//    boolean imageFromTif = false;
//    if (inputImage.endsWith("tif") || inputImage.endsWith("tiff"))
//    {
//      image = createPyramid(getConf(), inputImage); 
//      imageFromTif = true;
//    }
//    else
//    {
//      image = MrsPyramidv1.loadPyramid(new Path(inputImage));
//    }
//
//    if (image != null)
//    {
//      VectorizeRasterTool tool = new VectorizeRasterTool();
//      tool.run(image, new Path(output), threshold, getConf(), null, null);
//
//      if (imageFromTif)
//      {
//
//        fs.delete(image.getPath(), true);
//      }
//    }
//    return 0;
  }
}
