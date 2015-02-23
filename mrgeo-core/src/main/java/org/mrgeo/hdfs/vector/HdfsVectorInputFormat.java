package org.mrgeo.hdfs.vector;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.mrgeo.geometry.Geometry;

public class HdfsVectorInputFormat extends InputFormat<LongWritable, Geometry> implements Serializable
{
//  private static final String ATTRIBUTE_NAMES_FIELD = HdfsVectorInputFormat.class.getName() + ".attributeNames";
//  private static final String X_COL_FIELD = HdfsVectorInputFormat.class.getName() + ".xCol";
//  private static final String Y_COL_FIELD = HdfsVectorInputFormat.class.getName() + ".yCol";
//  private static final String GEOMETRY_COL_FIELD = HdfsVectorInputFormat.class.getName() + ".geometryCol";
//  private static final String DELIMITER_CHAR_FIELD = HdfsVectorInputFormat.class.getName() + ".delimiter";
//  private static final String SKIP_FIRST_LINE_FIELD = HdfsVectorInputFormat.class.getName() + ".skipFirstLine";

  private static final long serialVersionUID = 1L;

  public static void setupJob(Configuration conf)
  {
//    conf.set(ATTRIBUTE_NAMES_FIELD, StringUtils.join(attributeNames, ","));
//    conf.setInt(X_COL_FIELD, xCol);
//    conf.setInt(Y_COL_FIELD, yCol);
//    conf.setInt(GEOMETRY_COL_FIELD, geometryCol);
//    String strDelimiter = new String(new char[] { delimiter });
//    conf.set(DELIMITER_CHAR_FIELD, strDelimiter);
//    conf.setBoolean(SKIP_FIRST_LINE_FIELD, skipFirstLine);
  }

  @Override
  public RecordReader<LongWritable, Geometry> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException
  {
//    Configuration conf = context.getConfiguration();
//    int xCol = conf.getInt(X_COL_FIELD, -1);
//    int yCol = conf.getInt(Y_COL_FIELD, -1);
//    int geometryCol = conf.getInt(GEOMETRY_COL_FIELD, -1);
//    String delimiter = conf.get(DELIMITER_CHAR_FIELD, ",");
//    String strAttrNames = conf.get(ATTRIBUTE_NAMES_FIELD, "");
//    String[] attrNames = strAttrNames.split(",");
//    boolean skipFirstLine = conf.getBoolean(SKIP_FIRST_LINE_FIELD, false);
    HdfsVectorRecordReader recordReader = new HdfsVectorRecordReader();
    recordReader.initialize(split, context);
    return recordReader;
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException
  {
    return new TextInputFormat().getSplits(context);
  }
}
