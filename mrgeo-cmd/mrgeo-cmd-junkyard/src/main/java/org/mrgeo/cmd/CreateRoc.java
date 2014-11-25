/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.cmd;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.mrgeo.classifier.DrawRocDriver;
import org.mrgeo.classifier.RocDriver;
import org.mrgeo.classifier.SortOnColumnDriver;
import org.mrgeo.classifier.TprDriver;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.utils.HadoopUtils;

import java.awt.geom.Rectangle2D;
import java.util.Arrays;

/**
 */
public class CreateRoc extends Configured implements Tool
{
  public static Options createOptions()
  {
    Options result = new Options();

    Option input = new Option("i", "input", true, "Input TSV directory");
    input.setRequired(true);
    input.setArgs(Integer.MAX_VALUE);
    result.addOption(input);

    Option output = new Option("o", "output", true, "Output base name");
    output.setRequired(true);
    result.addOption(output);

    Option passThroughColumn = new Option("p", "pass", true, "Pass this column through");
    result.addOption(passThroughColumn);

    Option sortColumn = new Option("s", "sort", true, "The column to sort on");
    sortColumn.setRequired(true);
    result.addOption(sortColumn);

    Option color = new Option("l", "color", true, "Graph color");
    output.setRequired(true);
    result.addOption(color);

    Option graph = new Option("g", "graph", false, "Enable graph");
    result.addOption(graph);

    return result;
  }

  public static void main(String[] args) throws Exception
  {
    int res = ToolRunner.run(HadoopUtils.createConfiguration(), new CreateRoc(), args);
    System.exit(res);
  }

  @Override
  public int run(String[] args) throws Exception
  {
    CommandLineParser parser = new PosixParser();
    Options options = CreateRoc.createOptions();
    CommandLine line = null;
    try
    {
      line = parser.parse(options, args);
    }
    catch (ParseException e)
    {
      System.out.println(e.getMessage());
      System.out.println();
      new HelpFormatter().printHelp("mrgeo CreateRoc", options);
      return 1;
    }

    FileSystem fs = HadoopFileUtils.getFileSystem(getConf());
    String colorStr = line.getOptionValue("l");
    System.out.printf("Color: %s\n", colorStr);
    String outputBase = new Path(line.getOptionValue("o")).makeQualified(fs).toString();
    Path input = new Path(line.getOptionValue("i")).makeQualified(fs);
    String sortColumn = line.getOptionValue("s");
    String[] passThroughColumns = line.getOptionValues("p");
    // make it consistent
    Arrays.sort(passThroughColumns);

    try
    {
      Path sorted = new Path(outputBase + "-sorted.tsv");
      Path sortedBlocks = new Path(outputBase + "-sorted-blocks.seq");
      Path roc = new Path(outputBase + "-roc.tsv");

      SortOnColumnDriver socd = new SortOnColumnDriver();
      socd.setColumn(sortColumn);
      socd.runJob(input.toString(), sorted.toString(), null);

      TprDriver td = new TprDriver();
      td.runJob("fraud", "class", sorted.toString(), sortedBlocks.toString(), getConf(), null);

      RocDriver rocd = new RocDriver();
      rocd.setPassThroughColumns(passThroughColumns);
      rocd.runJob("fraud", "class", sorted.toString(), sortedBlocks.toString(), roc.toString(), getConf(), null);

      if (line.hasOption("g"))
      {
        DrawRocDriver drocd = new DrawRocDriver();
        drocd.setLabels("False Positive Rate", "True Positive Rate");
        drocd.setColor(colorStr);
        drocd.setRange(new Rectangle2D.Float(0.0f, 0.0f, 1.05f, 1.05f));
        drocd.runJob("fpr", "tpr", roc.toString(), outputBase + "-roc", getConf(), null);

        drocd = new DrawRocDriver();
        drocd.setLabels("True Positive Rate", "Precision");
        drocd.setColor(colorStr);
        drocd.setRange(new Rectangle2D.Float(0.0f, 0.0f, 0.25f * 1.05f, 0.25f * 1.05f));
        drocd.runJob("tpr", "ppv", roc.toString(), outputBase + "-ppv-tpr", getConf(), null);

        drocd = new DrawRocDriver();
        drocd.setLabels("Transactions Flagged", "Dollars Caught");
        drocd.setColor(colorStr);
        drocd.setRange(new Rectangle2D.Float(0.0f, 0.0f, 20000.0f * 1.1f, .7e6f * 1.1f));
        drocd.runJob("positives", "TRAN_AMT_tp_sum", roc.toString(), outputBase
            + "-positives-v-fraud-caught", getConf(), null);
      }
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw e;
    }

    return 0;
  }
}
